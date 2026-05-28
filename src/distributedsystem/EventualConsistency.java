/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distributedsystem;
import java.util.*;


/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

public class EventualConsistency implements consistencyHandler {
    private final NameServer dns;

    public EventualConsistency(NameServer dns) {
        this.dns = dns;
    }

    /**
     * 🚀 CLEAN & SILENT REWRITTEN FOR APPEND-ONLY MULTI-SEAT POSITION STORM
     * Preserves previously sold tickets in RAM while silencing console overheads.
     */
    @Override
    public void handleWrite(DistributedNode node, Message msg) {
        String currentSnapshot = node.getDataValue();
        String targetSeat = msg.getKey();    // e.g., "SEAT_42"
        String passengerId = msg.getValue(); // e.g., "Passenger-A"

        // Evaluate vacancy inside this decentralized region data layer snapshot view
        if (currentSnapshot == null || !currentSnapshot.contains(targetSeat + ":OCCUPIED")) {
            
            // Incremental append-only memory snapshot stream mapping [INDEX]
            String updatedSnapshot = (currentSnapshot == null || currentSnapshot.contains("INITIAL_NULL") || currentSnapshot.contains("POOL")) 
                                     ? "" : currentSnapshot + ", ";
            updatedSnapshot += targetSeat + ":OCCUPIED_BY_" + passengerId;
            
            node.setLocalDataValue(updatedSnapshot); // Optimistic low-latency local commit [INDEX]

            // 🚀 QUIET PROPAGATION: Removed the high-frequency println here!
            // Broadcast replication data packet over the VLAN wire links asynchronously
            for (String domain : dns.getAllNodes()) {
                // FIXED: Aligned strictly to your modernized resolveMailbox signature
                Mailbox targetMailbox = dns.resolve(domain);
                if (targetMailbox != null && targetMailbox != node.getMyMailbox()) {
                    Message replicateMsg = new Message(
                        Message.Type.EVENTUAL,
                        Message.Command.REPLICATE, 
                        targetSeat,
                        updatedSnapshot, // Push the raw un-sequenced memory frame map over wire [INDEX]
                        msg.getTimeStamp(),
                        node.getNodeId(),
                        -1 
                    );
                    // Leverage your thread-safe compile-clean double parameter signature [INDEX]
                    node.getMyMailbox().send(targetMailbox, replicateMsg);
                }
            }
        }
    }

    /**
     * 🚀 QUIET DUAL-PURPOSE READ RESOLUTION
     * Silenced all client query logs while preserving pure distributed active anti-entropy loops.
     */
    @Override
    public void handleRead(DistributedNode node, Message msg) {
        // 🚀 Removed high-frequency passenger check queries to keep terminal clean!
        
        // Anti-Entropy Trigger: If this read probe is sent from a lagging rebooted peer... [INDEX]
        if (msg.getSenderId() != null 
                && !msg.getSenderId().equals("CLIENT_APP") 
                && !msg.getSenderId().equals("AIRLINE_HQ")
                && !msg.getSenderId().equals("PASSENGER_APP")) {
            
            // Only logs macro administration recovery plane actions [INDEX]
            System.out.println("[GOSSIP ACTION] Node " + node.getNodeId() + " healing rebooted peer: " + msg.getSenderId());
            
            Mailbox rebootedMailbox = dns.resolve(msg.getSenderId());
            if (rebootedMailbox != null) {
                // Return this node's real un-manipulated variable heap state string back across wire
                Message recoveryPayload = new Message(
                    Message.Type.EVENTUAL,
                    Message.Command.REPLICATE, 
                    msg.getKey(),
                    node.getDataValue(), // 🚀 True decentralized data exchange from actual RAM [INDEX]
                    System.currentTimeMillis(),
                    node.getNodeId(),
                    -1
                );
                node.getMyMailbox().send(rebootedMailbox, recoveryPayload);
            }
        }
    }

    @Override
    public void handleReplicate(DistributedNode node, Message msg) {
        // Blindly overwrite state registers to maintain native Last-Write-Wins (LWW) traits [INDEX]
        node.setLocalDataValue(msg.getValue());
    }

    /**
     * 🚀 ACTIVE ANTI-ENTROPY INTERFACE REBOOT DETECTOR
     * Executed exactly once upon Scenario 3 failure recovery to realign parameters silently.
     */
    @Override
    public void handleRestart(DistributedNode node) {
        System.out.println("[ANTI-ENTROPY INITIATED] Rebooted Node " + node.getNodeId() + " seeking synchronization neighbors...");
        
        for (String domain : dns.getAllNodes()) {
            if (!domain.equals(node.getNodeId())) {
                Mailbox peerMailbox = dns.resolve(domain);
                
                if (peerMailbox != null) {
                    System.out.println("[GOSSIP OUTREACH] Node " + node.getNodeId() + " dispatching synchronization packet to peer: " + domain);
                    
                    Message syncProbe = new Message(
                        Message.Type.EVENTUAL,
                        Message.Command.READ, 
                        "DYNAMIC_RECOVERY_PROBE", // Wildcard recovery token mapping
                        "", 
                        System.currentTimeMillis(),
                        node.getNodeId(), // Pass itself so neighbor knows where to target reply response [INDEX]
                        -1
                    );
                    
                    node.getMyMailbox().send(peerMailbox, syncProbe);
                    break; // Outreach successfully bonded with one neighbor, exit loop safely
                }
            }
        }
    }  
}

