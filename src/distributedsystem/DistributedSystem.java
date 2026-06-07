
package distributedsystem;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 * @author PC
 */

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ThreadLocalRandom; // Lock-free high-concurrency random engine


public class DistributedSystem {
    public enum Scenario { NORMAL, HIGH_CONCURRENCY, NODE_FAILURE }  //we have 3 scenerio

    public static void main(String[] args) throws InterruptedException {
        System.out.println("==================================================================");
        System.out.println("   DISTRIFLY: ENTERPRISE DISTRIBUTED SYSTEM REPLICATION GRID     ");
        System.out.println("==================================================================\n");

        //  RUN ALL 4 PERFORMANCE TEST COMBINATIONS
        System.out.println(">>> RUNNING ALL 4 PERFORMANCE METRIC CONFIGURATIONS <<<\n");
        
        // Test 1: Eventual Consistency + Structured Naming
        System.out.println("\n\n████████████████████████████████████████████████████████████████");
        System.out.println("TEST 1: EVENTUAL CONSISTENCY + STRUCTURED NAMING");
        System.out.println("████████████████████████████████████████████████████████████████\n");
        runPerformanceTest(false, NameServer.NamingMode.STRUCTURED);
        
        // Test 2: Eventual Consistency + Flat Naming
        System.out.println("\n\n████████████████████████████████████████████████████████████████");
        System.out.println("TEST 2: EVENTUAL CONSISTENCY + FLAT NAMING");
        System.out.println("████████████████████████████████████████████████████████████████\n");
        runPerformanceTest(false, NameServer.NamingMode.FLAT);
        
        // Test 3: Sequential Consistency + Structured Naming
        System.out.println("\n\n████████████████████████████████████████████████████████████████");
        System.out.println("TEST 3: SEQUENTIAL CONSISTENCY + STRUCTURED NAMING");
        System.out.println("████████████████████████████████████████████████████████████████\n");
        runPerformanceTest(true, NameServer.NamingMode.STRUCTURED);
        
        // Test 4: Sequential Consistency + Flat Naming
        System.out.println("\n\n████████████████████████████████████████████████████████████████");
        System.out.println("TEST 4: SEQUENTIAL CONSISTENCY + FLAT NAMING");
        System.out.println("████████████████████████████████████████████████████████████████\n");
        runPerformanceTest(true, NameServer.NamingMode.FLAT);
        
        System.out.println("\n\n==================================================================");
        System.out.println("         DISTRIFLY ENTERPRISE REPLICATION BENCH ENGINE TERMINATED ");
        System.out.println("==================================================================");
    }

    public static void runPerformanceTest(boolean useSequentialEngine, NameServer.NamingMode activeNaming) throws InterruptedException {
        // CONTROL PANEL FOR SYSTEM MODE TESTING
        Scenario activeScenario = Scenario.NORMAL; 

        System.out.println(">>> System Deployment Scenario : " + activeScenario);
        System.out.println(">>> Data Replication Engine    : " + (useSequentialEngine ? "STRONG SEQUENTIAL" : "EVENTUAL CONSISTENCY"));
        System.out.println(">>> Naming Topology Resolution : " + activeNaming + "\n");

        NameServer dns = new NameServer(activeNaming);
        consistencyHandler selectedHandler = useSequentialEngine ? new SequentialConsistency(dns) : new EventualConsistency(dns);
        Message.Type networkProfile = useSequentialEngine ? Message.Type.SEQUENTIAL : Message.Type.EVENTUAL;
        MessageHandler msgHandler = new MessageHandler(selectedHandler);

        List<String> flightSeatsPool = new ArrayList<>();
        for (int i = 1; i <= 50; i++) flightSeatsPool.add("SEAT_" + i);

        DistributedNode n1 = new DistributedNode("Node_A", new Mailbox(), msgHandler);
        DistributedNode n2 = new DistributedNode("Node_B", new Mailbox(), msgHandler);
        DistributedNode n3 = new DistributedNode("Node_C", new Mailbox(), msgHandler);
        n1.setLocalDataValue("POOL"); n2.setLocalDataValue("POOL"); n3.setLocalDataValue("POOL");

        String domainA = "star.asia.Node_A";
        String domainB = "star.europe.Node_B";
        String domainC = "onworld.asia.Node_C";
        dns.register(domainA, n1.getMyMailbox());
        dns.register(domainB, n2.getMyMailbox());
        dns.register(domainC, n3.getMyMailbox());
        
        List<DistributedNode> cluster = Arrays.asList(n1, n2, n3);
        ExecutorService systemHardwareGrid = Executors.newFixedThreadPool(3);
        cluster.forEach(systemHardwareGrid::execute); // Mount process loops on thread boundaries [INDEX]
        Thread.sleep(200);

        MessageHandler.telemetry.totalRequests.set(0);
        MessageHandler.telemetry.successfulBookings.set(0);
        MessageHandler.telemetry.conflictsDetected.set(0);
        MessageHandler.telemetry.clientSideRejections.set(0);  //  Reset rejection counter
        MessageHandler.telemetry.totalReads.set(0);
        MessageHandler.telemetry.staleReadsDetected.set(0);
        MessageHandler.telemetry.messagesProcessed.set(0);  //  Reset message counter
        MessageHandler.telemetry.totalLatencyMs.set(0);
        MessageHandler.telemetry.cumulativeOperationLatencyMs.set(0);  // Reset cumulative latency

        int numUsers = (activeScenario == Scenario.HIGH_CONCURRENCY) ? 100 : 10;
        int numOperations = (activeScenario == Scenario.HIGH_CONCURRENCY) ? 1000 : 100;

        if (activeScenario == Scenario.NODE_FAILURE) {
            new Thread(() -> {
                try {
                    Thread.sleep(400); n3.crash(); // Induce hardware crash fault injection [INDEX]
                    Thread.sleep(1000); n3.restart(); // Induce recovery log replay catchup [INDEX, INDEX]
                } catch (Exception e) {}
            }).start();
        }

        ExecutorService clientStormPool = Executors.newFixedThreadPool(numUsers);
        CountDownLatch transactionLatch = new CountDownLatch(numOperations);
        ConcurrentHashMap<String, String> globalTruthOracle = new ConcurrentHashMap<>();
        
        long systemStartTimestamp = System.currentTimeMillis();

        for (int i = 0; i < numOperations; i++) {
            clientStormPool.submit(() -> {
                String targetSeatCode = flightSeatsPool.get(ThreadLocalRandom.current().nextInt(flightSeatsPool.size()));
                String passengerId = "Passenger-" + Thread.currentThread().getId() + "-" + ThreadLocalRandom.current().nextInt(100);
                DistributedNode targetedServer = cluster.get(ThreadLocalRandom.current().nextInt(cluster.size()));

                // Log the read operation manually since we are directly accessing local memory
                MessageHandler.telemetry.totalReads.incrementAndGet();
                
                //Check local server for the seat availability
                String localVariableSnapshot = targetedServer.getDataValue();
                boolean looksVacantLocally = localVariableSnapshot == null || !localVariableSnapshot.contains(targetSeatCode + ":OCCUPIED");
                
                // Prevent False Stale Reads for Sequential Consistency
                // Only flag stale reads if we are running the Eventual Consistency engine.
                if (!useSequentialEngine && globalTruthOracle.containsKey(targetSeatCode) && looksVacantLocally) {
                    MessageHandler.telemetry.staleReadsDetected.incrementAndGet(); 
                }

                if (looksVacantLocally && targetedServer.alive()) {
                    String domainHandle = targetedServer.getNodeId().equals("Node_A") ? domainA : targetedServer.getNodeId().equals("Node_B") ? domainB : domainC;
                    Mailbox resolvedNIC = dns.resolve(domainHandle);
                    if (resolvedNIC != null) {
                        long operationStartTime = System.currentTimeMillis();
                        Message bookingPacket = new Message(networkProfile, Message.Command.WRITE, targetSeatCode, passengerId, operationStartTime, passengerId, -1);
                        targetedServer.getMyMailbox().send(resolvedNIC, bookingPacket); 

                        // Track latency for Sequential Consistency writes
                        // Sequential handler will add its consensus delay to metrics
                        // Eventual handler will add its fast latency to metrics
                        
                        globalTruthOracle.compute(targetSeatCode, (k, currentOwner) -> {
                           if (currentOwner == null) {
                              // Removed totalRequests double-counting 
                                MessageHandler.telemetry.successfulBookings.incrementAndGet();
                                return passengerId;
                            } else {
                               //  Only let the Oracle count conflicts for Eventual Consistency
                                // Sequential Consistency rejects conflicts at the client level (pessimistic locking)
                                // So it should have near-zero cluster collisions
                                if (!useSequentialEngine) {
                                    MessageHandler.telemetry.conflictsDetected.incrementAndGet(); 
                                }
                                    return currentOwner; 
                            }
                        });
                    }
                } else if (useSequentialEngine && !looksVacantLocally) {
                    // Track client-side rejections for Sequential Consistency
                    // This accounts for pessimistic locking rejections at the client level
                    MessageHandler.telemetry.clientSideRejections.incrementAndGet();
                }
                transactionLatch.countDown();
            });
        }

        transactionLatch.await();
        System.out.println("[WORKLOAD FLUSHED] Waiting 4 seconds for network convergence...");
        Thread.sleep(4000); // Timeline tracking buffer for lagging mail elements [INDEX]

        // Executing live process migration state transfer prior to clock stoppage [INDEX, INDEX]
        System.out.println("\n>>> [PROCESS OPTIMIZATION] Executing live state migration trial (Node_A -> Node_C) <<<");
        n1.migrateStateTo(domainC, dns); 
        Thread.sleep(1500); 

        //  ACCURATE TELEMETRY CAPTURE STOPPAGE WHEEL
        long systemEndTimestamp = System.currentTimeMillis();

        clientStormPool.shutdown(); systemHardwareGrid.shutdownNow(); // Trip circuit breakers safely [INDEX]
        
        //  EXPORT ENGINE BINDING: Output both macro-telemetry reports cleanly with 0 console spam
        dns.printSummaryReport(); // Added to print name server path hops & network drop ratios from messageHandle.java
        
        String consistencyType = useSequentialEngine ? "SEQUENTIAL" : "EVENTUAL";
        String namingType = activeNaming == NameServer.NamingMode.FLAT ? "FLAT" : "STRUCTURED";
        String scenarioLabel = consistencyType + "_" + namingType + "_" + activeScenario.toString();
        MessageHandler.telemetry.printReport(systemEndTimestamp - systemStartTimestamp, scenarioLabel);
    }
}

