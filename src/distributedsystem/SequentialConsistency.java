/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/Class.java to edit this template
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
    
    // Pessimistic locking infrastructure - tracks which keys are currently locked
    private final Map<String, Long> pendingLocks = new ConcurrentHashMap<>();  // key -> lock acquisition timestamp
    private final long LOCK_TIMEOUT_MS = 5000;  // Lock expires after 5 seconds
    
    //  Simulated network distance for consensus overhead (in milliseconds)
    private static final int CONSENSUS_DELAY_MIN_MS = 8;
    private static final int CONSENSUS_DELAY_MAX_MS = 15;

    public SequentialConsistency(NameServer dns) { this.dns = dns; }
    public void initNodeBuffer(String nodeId) {
        nodeBuffers.putIfAbsent(nodeId, new ConcurrentHashMap<>());
        nextSequence.putIfAbsent(nodeId, 1);
    }

    /**
     *  Check if a key is currently locked by another writer (pessimistic locking)
     * Returns true if the key is free to write; false if it's locked by someone else
     */
    private synchronized boolean acquireLock(String key) {
        Long currentLockTime = pendingLocks.get(key);
        long now = System.currentTimeMillis();
        
        // If key is not locked, or the lock has expired, we can acquire it
        if (currentLockTime == null || (now - currentLockTime) > LOCK_TIMEOUT_MS) {
            pendingLocks.put(key, now);
            return true;  // Lock acquired successfully
        }
        
        return false;  // Lock is held by another writer
    }
    
    /**
     *  Release the lock on a key after write completes
     */
    private synchronized void releaseLock(String key) {
        pendingLocks.remove(key);
    }

    @Override
    public synchronized void handleWrite(DistributedNode node, Message msg) {
        String targetSeat = msg.getKey();
        String passengerId = msg.getValue();
        
        //  Simulate consensus/2PC delay (8-15ms) for strong consistency
        // This represents quorum gathering, Paxos/Raft rounds, or distributed locking overhead
        try {
            int consensusDelayMs = CONSENSUS_DELAY_MIN_MS + (int)(Math.random() * (CONSENSUS_DELAY_MAX_MS - CONSENSUS_DELAY_MIN_MS));
            Thread.sleep(consensusDelayMs);
            
            //  Track this latency in the message for metrics reporting
            // (In a real system, this would be returned in a WRITE_ACK)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        //  Pessimistic locking - try to acquire exclusive lock on the key
        // This represents the "prepare phase" of 2PC or Paxos/Raft quorum gathering
        if (!acquireLock(targetSeat)) 
{            //  Write rejected due to lock contention - pessimistic approach prevents conflicts
            MessageHandler.telemetry.clientSideRejections.incrementAndGet();
            return;  // Early rejection - never reaches the cluster
        }

        try {
            // Now check if the seat is available (double-check after acquiring lock)
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
                // Even with pessimistic locking, conflicts can happen if two writers
                // acquire lock and then both check - very rare but logged for completeness
                // In sequential consistency, this should be near-zero
                MessageHandler.telemetry.conflictsDetected.incrementAndGet();
            }
        } finally {
            // Always release the lock
            releaseLock(targetSeat);
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
    
    @Override public void handleReplicateNack(DistributedNode node, Message msg) {}
}


