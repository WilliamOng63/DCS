/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distributedsystem;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;



public class SequentialConsistency implements consistencyHandler {
    private final NameServer dns;
    private int globalSequenceCount = 0;
    private final Map<String, Map<Integer, Message>> nodeBuffers = new ConcurrentHashMap<>();
    private final Map<String, Integer> nextSequence = new ConcurrentHashMap<>();
    private final Map<String, String> centralSeatRegistry = new ConcurrentHashMap<>();
    private final List<Message> globalCommitLog = new CopyOnWriteArrayList<>();

    public SequentialConsistency(NameServer dns) { this.dns = dns; }
    public void initNodeBuffer(String nodeId) {
        nodeBuffers.putIfAbsent(nodeId, new ConcurrentHashMap<>());
        nextSequence.putIfAbsent(nodeId, 1);
    }

    @Override
    public synchronized void handleWrite(DistributedNode node, Message msg) {
        String targetSeat = msg.getKey();
        String passengerId = msg.getValue();

      if (!centralSeatRegistry.containsKey(targetSeat)) {
        centralSeatRegistry.put(targetSeat, passengerId);
        globalSequenceCount++;
        int assignedSeqId = globalSequenceCount;
        String linearStateSnapshot = targetSeat + ":OCCUPIED_BY_" + passengerId;

        // IMMEDIATE LOCAL COMMIT to prevent local stale reads on the coordinator
        String currentLocalState = node.getDataValue();
        String updatedState = (currentLocalState == null || currentLocalState.contains("POOL")) ? "" : currentLocalState + ", ";
        node.setLocalDataValue(updatedState + linearStateSnapshot);

        Message replicateMsg = new Message(Message.Type.SEQUENTIAL, Message.Command.REPLICATE, targetSeat, linearStateSnapshot, System.currentTimeMillis(), node.getNodeId(), assignedSeqId);
        globalCommitLog.add(replicateMsg); 

  
            for (String domain : dns.getAllNodes()) {
                Mailbox targetMailbox = dns.resolve(domain);
                if (targetMailbox != null) {
                    node.getMyMailbox().send(targetMailbox, replicateMsg);
                }
            }  
        } else {
            // 🚀 默默记录冲突，不在高并发高强度的抢票期间往控制台疯狂打印被拒绝的日志
            MessageHandler.telemetry.conflictsDetected.incrementAndGet();
        }
    }

    @Override
    public void handleReplicate(DistributedNode node, Message msg) {
        initNodeBuffer(node.getNodeId());
        Map<Integer, Message> buffer = nodeBuffers.get(node.getNodeId());
        buffer.put(msg.getSequenceId(), msg);

        synchronized (node) {
            int currentSequence = nextSequence.get(node.getNodeId());
            while (buffer.containsKey(currentSequence)) {
                Message orderMsg = buffer.remove(currentSequence);
                String currentLocalState = node.getDataValue();
                
                //  Only append if the seat isn't already in our local memory!
                // This prevents the coordinator from double-appending its own loopback messages.
                if (currentLocalState == null || !currentLocalState.contains(orderMsg.getKey() + ":OCCUPIED")) {
                    String updatedState = (currentLocalState == null || currentLocalState.contains("POOL")) ? "" : currentLocalState + ", ";
                    updatedState += orderMsg.getValue();
                    node.setLocalDataValue(updatedState);
                }
                
                currentSequence++;
            }
            nextSequence.put(node.getNodeId(), currentSequence);
        }
    }

    @Override
    public void handleRestart(DistributedNode node) {
        nodeBuffers.put(node.getNodeId(), new ConcurrentHashMap<>());
        nextSequence.put(node.getNodeId(), 1);
        for (Message loadHistoryMsg : globalCommitLog) {
            node.getMyMailbox().send(node.getMyMailbox(), loadHistoryMsg);
        }
    }

    @Override public void handleRead(DistributedNode node, Message msg) {}
}


