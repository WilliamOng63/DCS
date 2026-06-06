/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distributedsystem;
import java.util.*;
/**
 *
 * @author PC
 */

public class MessageHandler {
    private final consistencyHandler handler;
    public static final Metrics telemetry = new Metrics();
    
    // Auth pointer to verify if the cluster returns linearizable fresh data
    public static String expectedGlobalLatestValue = "POOL";

    public MessageHandler(consistencyHandler handler) {
        this.handler = handler;
    }

    public consistencyHandler getHandler() { return handler; }

    public synchronized void handleIncomingMessage(DistributedNode node, Message msg) {
        // 🚀 FIXED: Calculate actual per-operation latency from message creation time to processing time
        long currentTime = System.currentTimeMillis();
        long operationLatency = currentTime - msg.getTimeStamp();
        telemetry.cumulativeOperationLatencyMs.addAndGet(operationLatency);
        telemetry.messagesProcessed.incrementAndGet();  // 🚀 Track actual messages for accurate latency
        
        // Suppressed high-frequency spam logs to keep output clean as requested
        switch (msg.getCommand()) {
            case WRITE:
                // 1. Log the incoming transaction attempt into global metrics ledger
                telemetry.totalRequests.incrementAndGet(); 
                
                // Track what the oracle expects the latest state to mutate into
                expectedGlobalLatestValue = msg.getKey() + ":OCCUPIED_BY_" + msg.getValue();
                
                // 🚀 Delegate seat occupancy checks to the targeted consistency engine algorithm
                handler.handleWrite(node, msg);
                break;
                
            case REPLICATE:
                handler.handleReplicate(node, msg);
                break;
                
            case REPLICATE_NACK:
                // 🚀 NEW: FWW - Peer had earlier write, rollback our optimistic commit
                handler.handleReplicateNack(node, msg);
                break;
                
            case READ:
                telemetry.totalReads.incrementAndGet(); 
                
                // 2. 🛡️ CRITICAL CORRECTNESS AUDIT BARRIER
                // Compare the node's local variable snapshot memory with the global logic timeline tracker
                String localSnapshot = node.getDataValue();
                
                // If running in Strong Sequential mode, localSnapshot MUST match the global truth perfectly!
                if (handler instanceof SequentialConsistency) {
                    // Under CP model, if the lagging replica hasn't caught up, log as a structural conflict
                    if (localSnapshot == null || localSnapshot.contains("POOL") || !localSnapshot.contains(msg.getKey())) {
                        telemetry.staleReadsDetected.incrementAndGet();
                        telemetry.conflictsDetected.incrementAndGet();
                    }
                } else {
                    // Under Eventual Availability profile, if it returns an un-synchronized vacant marker, count as a Stale Read
                    if (localSnapshot == null || !localSnapshot.contains(msg.getKey())) {
                        telemetry.staleReadsDetected.incrementAndGet();
                    }
                }
                
                handler.handleRead(node, msg);
                break;
                
            case MIGRATE:
                break;
                
            default:
                System.out.println("[PROTOCOL EXCEPTION] Unknown packet header!");
        }
    }

    // Telemetry Class Profile Preservation
    public static class Metrics {
        public final java.util.concurrent.atomic.AtomicInteger totalRequests = new java.util.concurrent.atomic.AtomicInteger(0);
        public final java.util.concurrent.atomic.AtomicInteger successfulBookings = new java.util.concurrent.atomic.AtomicInteger(0);
        public final java.util.concurrent.atomic.AtomicInteger conflictsDetected = new java.util.concurrent.atomic.AtomicInteger(0);
        public final java.util.concurrent.atomic.AtomicInteger clientSideRejections = new java.util.concurrent.atomic.AtomicInteger(0);  // New: Track pessimistic rejections
        public final java.util.concurrent.atomic.AtomicInteger totalReads = new java.util.concurrent.atomic.AtomicInteger(0);
        public final java.util.concurrent.atomic.AtomicInteger staleReadsDetected = new java.util.concurrent.atomic.AtomicInteger(0);
        public final java.util.concurrent.atomic.AtomicInteger messagesProcessed = new java.util.concurrent.atomic.AtomicInteger(0);  // 🚀 NEW: Count actual messages for latency
        public final java.util.concurrent.atomic.AtomicLong totalLatencyMs = new java.util.concurrent.atomic.AtomicLong(0);
        public final java.util.concurrent.atomic.AtomicLong cumulativeOperationLatencyMs = new java.util.concurrent.atomic.AtomicLong(0);  // New: Track total latency across operations

        public void printReport(long durationMs, String scenarioName) {
            System.out.println("\n==================================================");
            System.out.println("=== PERFORMANCE METRICS: " + scenarioName + " ===");
            System.out.println("==================================================");
            System.out.printf("Total Write Requests : %d (Flight Bookings Tried)\n", totalRequests.get());
            System.out.printf("Successful Bookings  : %d (Confirmed Boarding Passes)\n", successfulBookings.get());
            System.out.printf("Client-Side Rejections: %d (Pessimistic Locking Blocks)\n", clientSideRejections.get());
            System.out.printf("Conflicts/Violations : %d (Total Cluster Collisions)\n", conflictsDetected.get());
            System.out.println("--------------------------------------------------");
            System.out.printf("Total Read Requests  : %d (Inventory Queries)\n", totalReads.get());
            System.out.printf("Stale Reads Detected : %d (Overbooking Vector Warnings)\n", staleReadsDetected.get());
            System.out.println("--------------------------------------------------");
            
            // 🚀 FIXED: Calculate average latency ONLY from messages actually processed (not local reads)
            int totalMsgsProcessed = messagesProcessed.get();
            double actualAvgLatency = totalMsgsProcessed > 0 
                ? (double) cumulativeOperationLatencyMs.get() / totalMsgsProcessed 
                : 0.0;
            
            long totalOps = totalRequests.get() + totalReads.get();
            double throughput = totalOps > 0 ? (double) totalOps / (Math.max(1, durationMs) / 1000.0) : 0.0;
            
            System.out.printf("Average Message Latency (measured): %.2f ms\n", actualAvgLatency);
            System.out.printf("Messages Processed   : %d\n", totalMsgsProcessed);
            System.out.printf("Throughput           : %.2f ops/sec\n", throughput);
            System.out.println("==================================================\n");
        }
    }
}
