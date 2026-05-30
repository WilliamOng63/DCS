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
    
    // 🚀 NEW: Simulated latency for eventual consistency writes (fire-and-forget)
    // Much lower than Sequential Consistency because no consensus needed
    private static final int FAST_LATENCY_MIN_MS = 1;
    private static final int FAST_LATENCY_MAX_MS = 3;

    public EventualConsistency(NameServer dns) {
        this.dns = dns;
    }

    /**
     * 🚀 TIMESTAMPED OPTIMISTIC COMMIT FOR FIRST-WRITE-WINS (FWW)
     * Embeds the write timestamp directly into the string format for conflict resolution.
     * Format: [SEAT]:OCCUPIED_BY_[PASSENGER]@[TIMESTAMP]
     * Example: SEAT_42:OCCUPIED_BY_Passenger-A@1715001234567
     */
    @Override
    public void handleWrite(DistributedNode node, Message msg) {
        String currentSnapshot = node.getDataValue();
        String targetSeat = msg.getKey();    // e.g., "SEAT_42"
        String passengerId = msg.getValue(); // e.g., "Passenger-A"
        long writeTimestamp = msg.getTimeStamp(); // Capture the write request timestamp

        // 🚀 NEW: Simulate low latency for fire-and-forget async write
        // This represents fast local commit without waiting for consensus
        try {
            int fastLatencyMs = FAST_LATENCY_MIN_MS + (int)(Math.random() * (FAST_LATENCY_MAX_MS - FAST_LATENCY_MIN_MS));
            Thread.sleep(fastLatencyMs);
            
            // Track this latency in metrics
            MessageHandler.telemetry.cumulativeOperationLatencyMs.addAndGet(fastLatencyMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 🚀 FWW: Check if this seat already has an entry (which would indicate a conflict)
        if (currentSnapshot == null || !currentSnapshot.contains(targetSeat + ":OCCUPIED")) {
            
            // No existing entry for this seat - Append with timestamp embedded in string
            String updatedSnapshot = (currentSnapshot == null || currentSnapshot.contains("INITIAL_NULL") || currentSnapshot.contains("POOL")) 
                                     ? "" : currentSnapshot + ", ";
            // 🚀 KEY FORMAT: Append timestamp to enable FWW conflict resolution downstream
            updatedSnapshot += targetSeat + ":OCCUPIED_BY_" + passengerId + "@" + writeTimestamp;
            
            node.setLocalDataValue(updatedSnapshot); // Optimistic low-latency local commit [INDEX]

            // 🚀 QUIET PROPAGATION: Removed the high-frequency println here!
            // Broadcast replication data packet over the VLAN wire links asynchronously
            // This happens in the background - write returns to client immediately!
            for (String domain : dns.getAllNodes()) {
                // FIXED: Aligned strictly to your modernized resolveMailbox signature
                Mailbox targetMailbox = dns.resolve(domain);
                if (targetMailbox != null && targetMailbox != node.getMyMailbox()) {
                    Message replicateMsg = new Message(
                        Message.Type.EVENTUAL,
                        Message.Command.REPLICATE, 
                        targetSeat,
                        updatedSnapshot, // Push the raw un-sequenced memory frame map over wire [INDEX]
                        writeTimestamp,  // 🚀 Pass the original write timestamp for FWW comparison
                        node.getNodeId(),
                        -1 
                    );
                    // Leverage your thread-safe compile-clean double parameter signature [INDEX]
                    node.getMyMailbox().send(targetMailbox, replicateMsg);
                }
            }
        } else {
            // 🚀 NEW: In eventual consistency, conflicts are allowed at the local level
            // They get resolved later during background replication (First-Write-Wins)
            // This is why eventual consistency has high throughput but eventual conflicts
            MessageHandler.telemetry.conflictsDetected.incrementAndGet();
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

    /**
     * 🚀 THE TIMESTAMP DUEL: FIRST-WRITE-WINS (FWW) RECEIVER VALIDATION
     * When receiving a REPLICATE message with a timestamped record:
     * - Parse the incoming message to extract target seat and incoming timestamp
     * - Search local dataValue for that specific seat
     * - IF seat NOT in local memory: Append the incoming record (no conflict)
     * - IF seat IS in local memory: Compare timestamps via FWW logic:
     *   - If incoming_timestamp < local_timestamp: Incoming was requested FIRST → accept
     *   - If incoming_timestamp > local_timestamp: Local was requested FIRST → reject + send NACK
     */
    @Override
    public void handleReplicate(DistributedNode node, Message msg) {
        String currentSnapshot = node.getDataValue();
        String targetSeat = msg.getKey();           // e.g., "SEAT_42"
        String incomingState = msg.getValue();      // The full updated snapshot from the sender
        long incomingTimestamp = msg.getTimeStamp(); // 🚀 The timestamp of the write request
        String senderId = msg.getSenderId();
        
        // 🚀 PARSE: Extract the timestamp from the incoming replicated record for this specific seat
        long parsedIncomingTimestamp = extractTimestampForSeat(incomingState, targetSeat);
        
        // 🚀 SEARCH: Check if we already have an entry for this seat
        boolean seatExistsLocally = currentSnapshot != null && currentSnapshot.contains(targetSeat + ":OCCUPIED");
        
        if (!seatExistsLocally) {
            // ✅ NO CONFLICT: Seat is not in our local memory. Append the incoming record.
            String updatedSnapshot = (currentSnapshot == null || currentSnapshot.isEmpty() || currentSnapshot.contains("INITIAL_NULL")) 
                                     ? incomingState 
                                     : currentSnapshot + ", " + extractSeatRecordFromSnapshot(incomingState, targetSeat);
            node.setLocalDataValue(updatedSnapshot);
            
            // Optional: Send ACK to confirm successful replication
            Message ackMsg = new Message(
                Message.Type.EVENTUAL,
                Message.Command.REPLICATE_ACK,
                targetSeat,
                incomingState,
                System.currentTimeMillis(),
                node.getNodeId(),
                msg.getSequenceId()
            );
            
            Mailbox senderMailbox = dns.resolve(senderId);
            if (senderMailbox != null) {
                node.getMyMailbox().send(senderMailbox, ackMsg);
            }
        } else {
            // ⚔️ CONFLICT DETECTED: Seat exists in local memory. Compare timestamps.
            long localTimestamp = extractTimestampForSeat(currentSnapshot, targetSeat);
            
            if (parsedIncomingTimestamp < localTimestamp) {
                // ✅ INCOMING WINS: Incoming message was requested FIRST (smaller timestamp = earlier request)
                // Replace the local entry with the incoming (older) entry
                String updatedSnapshot = replaceSeatRecordInSnapshot(currentSnapshot, targetSeat, 
                                                                     extractSeatRecordFromSnapshot(incomingState, targetSeat));
                node.setLocalDataValue(updatedSnapshot);
                
                // Send ACK to confirm acceptance
                Message ackMsg = new Message(
                    Message.Type.EVENTUAL,
                    Message.Command.REPLICATE_ACK,
                    targetSeat,
                    incomingState,
                    System.currentTimeMillis(),
                    node.getNodeId(),
                    msg.getSequenceId()
                );
                
                Mailbox senderMailbox = dns.resolve(senderId);
                if (senderMailbox != null) {
                    node.getMyMailbox().send(senderMailbox, ackMsg);
                }
            } else {
                // ✅ LOCAL WINS: Local message was requested FIRST (local timestamp is older/smaller)
                // Reject the incoming message and send NACK with the authoritative local record
                String localSeatRecord = extractSeatRecordFromSnapshot(currentSnapshot, targetSeat);
                
                Message nackMsg = new Message(
                    Message.Type.EVENTUAL,
                    Message.Command.REPLICATE_NACK,
                    targetSeat,
                    localSeatRecord,  // 🚀 Send ONLY the winning local seat record as the payload
                    System.currentTimeMillis(),
                    node.getNodeId(),
                    msg.getSequenceId()
                );
            
                // Route NACK back to the sender with the winning local record
                Mailbox senderMailbox = dns.resolve(senderId);
                if (senderMailbox != null) {
                    node.getMyMailbox().send(senderMailbox, nackMsg);
                }
                
                // Count this as a conflict detected during FWW validation
                MessageHandler.telemetry.conflictsDetected.incrementAndGet();
                
                System.out.println("[FWW CONFLICT] Node " + node.getNodeId() + " rejected write from " + senderId 
                                 + " for " + targetSeat + " (local timestamp " + localTimestamp + " < incoming " + parsedIncomingTimestamp + ")");
            }
        }
    }

    /**
     * 🚀 BACKGROUND ROLLBACK: FIRST-WRITE-WINS (FWW) NEGATIVE ACKNOWLEDGMENT
     * When receiving a REPLICATE_NACK, the original sender learns it lost the timestamp race.
     * Parse the payload to get the authoritative (older) booking record.
     * Use String manipulation to replace the incorrect local booking with the correct one.
     * This maintains FWW: the writer with the earlier timestamp wins across all replicas.
     */
    @Override
    public void handleReplicateNack(DistributedNode node, Message msg) {
        String authoritativeSeatRecord = msg.getValue(); // The winning record from the peer
        String targetSeat = msg.getKey();
        String currentSnapshot = node.getDataValue();
        
        // 🚀 ROLLBACK: Find and replace the incorrect local entry with the authoritative entry
        if (currentSnapshot != null && currentSnapshot.contains(targetSeat + ":OCCUPIED")) {
            // Replace the local seat record with the authoritative one from the NACK
            String correctedSnapshot = replaceSeatRecordInSnapshot(currentSnapshot, targetSeat, authoritativeSeatRecord);
            node.setLocalDataValue(correctedSnapshot);
            
            // Count this as a conflict that required rollback
            MessageHandler.telemetry.conflictsDetected.incrementAndGet();
            
            System.out.println("[FWW ROLLBACK] Node " + node.getNodeId() + " rolled back " + targetSeat 
                             + " to authoritative state from " + msg.getSenderId());
        }
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
    
    // ==================== 🚀 FWW HELPER METHODS ====================
    
    /**
     * 🚀 EXTRACT TIMESTAMP FOR A SPECIFIC SEAT FROM SNAPSHOT
     * Parses the comma-separated snapshot to find the specific seat record and extract its timestamp.
     * Format: SEAT_42:OCCUPIED_BY_Passenger-A@1715001234567
     * Returns the timestamp, or 0 if not found.
     */
    private long extractTimestampForSeat(String snapshot, String targetSeat) {
        if (snapshot == null || snapshot.isEmpty()) {
            return 0;
        }
        
        // Split by comma to get individual seat records
        String[] records = snapshot.split(",");
        
        for (String record : records) {
            record = record.trim();
            // Check if this record is for our target seat
            if (record.startsWith(targetSeat + ":")) {
                // Extract the timestamp (last part after @)
                if (record.contains("@")) {
                    try {
                        String timestampStr = record.substring(record.lastIndexOf("@") + 1);
                        return Long.parseLong(timestampStr);
                    } catch (NumberFormatException e) {
                        System.err.println("[ERROR] Failed to parse timestamp from record: " + record);
                        return 0;
                    }
                }
            }
        }
        
        return 0; // Seat not found in snapshot
    }
    
    /**
     * 🚀 EXTRACT SEAT RECORD FROM SNAPSHOT
     * Returns the complete seat record for a specific seat (including timestamp).
     * Format: SEAT_42:OCCUPIED_BY_Passenger-A@1715001234567
     * Returns null if not found.
     */
    private String extractSeatRecordFromSnapshot(String snapshot, String targetSeat) {
        if (snapshot == null || snapshot.isEmpty()) {
            return null;
        }
        
        // Split by comma to get individual seat records
        String[] records = snapshot.split(",");
        
        for (String record : records) {
            record = record.trim();
            // Check if this record is for our target seat
            if (record.startsWith(targetSeat + ":")) {
                return record;
            }
        }
        
        return null; // Seat not found in snapshot
    }
    
    /**
     * 🚀 REPLACE SEAT RECORD IN SNAPSHOT
     * Replaces the entry for a specific seat with a new record.
     * Properly handles the comma-separated format.
     * If oldRecord is not found, appends newRecord to the snapshot.
     */
    private String replaceSeatRecordInSnapshot(String snapshot, String targetSeat, String newRecord) {
        if (snapshot == null || snapshot.isEmpty()) {
            return newRecord;
        }
        
        // Split by comma to get individual seat records
        String[] records = snapshot.split(",");
        StringBuilder result = new StringBuilder();
        boolean found = false;
        
        for (int i = 0; i < records.length; i++) {
            String record = records[i].trim();
            
            if (record.startsWith(targetSeat + ":")) {
                // Replace this record with the new one
                if (i > 0) {
                    result.append(", ");
                }
                result.append(newRecord);
                found = true;
            } else if (!record.isEmpty()) {
                // Keep this record as-is
                if (result.length() > 0) {
                    result.append(", ");
                }
                result.append(record);
            }
        }
        
        // If the seat was not found, append the new record
        if (!found) {
            if (result.length() > 0) {
                result.append(", ");
            }
            result.append(newRecord);
        }
        
        return result.toString();
    }
}

