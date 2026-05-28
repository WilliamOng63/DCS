import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Campus Room Booking Simulation
 * Executes all Scenarios sequentially for easy benchmarking.
 */
public class NewClass {

    // =========================================================================
    // CONFIGURATION ENUMS
    // =========================================================================
    public enum NamingModel {
        FLAT, STRUCTURED
    }

    public enum ConsistencyModel {
        SEQUENTIAL, EVENTUAL
    }

    public enum Scenario {
        NORMAL_LOAD, HIGH_CONCURRENCY, NODE_FAILURE
    }

    // =========================================================================
    // CORE DOMAIN MODELS
    // =========================================================================
    static class Room {
        String id;
        boolean isBooked;
        String bookedBy;
        long lastUpdated;

        Room(String id) {
            this.id = id;
            this.isBooked = false;
            this.bookedBy = null;
            this.lastUpdated = 0;
        }

        Room copy() {
            Room r = new Room(this.id);
            r.isBooked = this.isBooked;
            r.bookedBy = this.bookedBy;
            r.lastUpdated = this.lastUpdated;
            return r;
        }
    }

    static class Metrics {
        AtomicInteger totalRequests = new AtomicInteger(0);
        AtomicInteger successfulBookings = new AtomicInteger(0);
        AtomicInteger conflictsDetected = new AtomicInteger(0);
        AtomicLong totalLatencyMs = new AtomicLong(0);
        AtomicLong totalLookupTimeMs = new AtomicLong(0);

        void printReport(long durationMs, String scenarioName) {
            System.out.println("=== PERFORMANCE METRICS: " + scenarioName + " ===");
            System.out.printf("Total Requests:       %d\n", totalRequests.get());
            System.out.printf("Successful Bookings:  %d\n", successfulBookings.get());
            System.out.printf("Conflicts/Violations: %d\n", conflictsDetected.get());
            System.out.printf("Average Latency:      %.2f ms\n",
                    (double) totalLatencyMs.get() / Math.max(1, totalRequests.get()));
            System.out.printf("Avg Lookup Time:      %.2f ms\n",
                    (double) totalLookupTimeMs.get() / Math.max(1, totalRequests.get()));
            System.out.printf("Throughput:           %.2f ops/sec\n",
                    (totalRequests.get() / (Math.max(1, durationMs) / 1000.0)));
            System.out.println("==================================================\n");
        }
    }

    // =========================================================================
    // DISTRIBUTED SYSTEM COMPONENTS
    // =========================================================================
    static class Node {
        String nodeId;
        boolean isOnline = true;
        ConcurrentHashMap<String, Room> localLedger = new ConcurrentHashMap<>();
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        Node(String nodeId, List<String> initialRooms) {
            this.nodeId = nodeId;
            for (String rId : initialRooms) {
                localLedger.put(rId, new Room(rId));
            }
        }

        boolean applyBooking(String roomId, String userId, long timestamp, boolean isBackgroundSync) {
            if (!isOnline)
                return false;

            lock.writeLock().lock();
            try {
                Room room = localLedger.get(roomId);
                if (room == null)
                    return false;

                if (isBackgroundSync) {
                    // REPLICATION LOGIC: Nodes syncing use Last-Writer-Wins
                    if (room.isBooked && room.lastUpdated >= timestamp) {
                        return false;
                    }
                } else {
                    // CLIENT LOGIC: First-come, first-serve. Reject if already booked!
                    if (room.isBooked) {
                        return false;
                    }
                }

                room.isBooked = true;
                room.bookedBy = userId;
                room.lastUpdated = timestamp;
                return true;
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    static class ReplicationManager {
        List<Node> nodes;
        ConsistencyModel model;
        ExecutorService asyncReplicator = Executors.newCachedThreadPool();
        Metrics metrics;

        ReplicationManager(List<Node> nodes, ConsistencyModel model, Metrics metrics) {
            this.nodes = nodes;
            this.model = model;
            this.metrics = metrics;
        }

        boolean processBooking(Node primaryNode, String roomId, String userId) {
            long timestamp = System.currentTimeMillis();

            if (model == ConsistencyModel.SEQUENTIAL) {
                synchronized (this) {
                    Room check = primaryNode.localLedger.get(roomId);
                    if (check != null && check.isBooked) {
                        metrics.conflictsDetected.incrementAndGet();
                        return false;
                    }
                    for (Node n : nodes) {
                        // SEQUENTIAL: Always false, as these are synchronous client-driven writes
                        if (n.isOnline)
                            n.applyBooking(roomId, userId, timestamp, false);
                    }
                    return true;
                }
            } else {
                // EVENTUAL CONSISTENCY

                // 1. Pass FALSE: This is a brand new client request
                boolean localSuccess = primaryNode.applyBooking(roomId, userId, timestamp, false);
                if (!localSuccess) {
                    metrics.conflictsDetected.incrementAndGet();
                    return false;
                }

                // 2. Propagate asynchronously
                asyncReplicator.submit(() -> {
                    for (Node n : nodes) {
                        if (n != primaryNode && n.isOnline) {
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException e) {
                            }

                            // Pass TRUE: This is a background server sync
                            boolean replicated = n.applyBooking(roomId, userId, timestamp, true);
                            if (!replicated) {
                                metrics.conflictsDetected.incrementAndGet();
                            }
                        }
                    }
                });
                return true;
            }
        }

        // Clean up threads after each scenario
        void shutdown() throws InterruptedException {
            asyncReplicator.shutdown();
            asyncReplicator.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    static class NamingServer {
        NamingModel model;
        Map<String, Node> flatDirectory = new HashMap<>();
        List<Node> allNodes;

        NamingServer(NamingModel model, List<Node> nodes, List<String> allRooms) {
            this.model = model;
            this.allNodes = nodes;
            for (int i = 0; i < allRooms.size(); i++) {
                flatDirectory.put(allRooms.get(i), nodes.get(i % nodes.size()));
            }
        }

        Node resolve(String roomId, Metrics metrics) {
            long start = System.currentTimeMillis();
            Node target = null;

            try {
                if (model == NamingModel.FLAT) {
                    Thread.sleep(1);
                    target = flatDirectory.get(roomId);
                } else if (model == NamingModel.STRUCTURED) {
                    Thread.sleep(5);
                    String[] parts = roomId.split("\\.");
                    if (parts.length == 3) {
                        int hash = Math.abs(parts[1].hashCode());
                        target = allNodes.get(hash % allNodes.size());
                    } else {
                        target = flatDirectory.get(roomId);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            metrics.totalLookupTimeMs.addAndGet(System.currentTimeMillis() - start);
            return target;
        }
    }

    // =========================================================================
    // SIMULATION RUNNER
    // =========================================================================

    public static void main(String[] args) throws InterruptedException {
        // Global Configurations for the run
        // NamingModel activeNaming = NamingModel.FLAT;
        NamingModel activeNaming = NamingModel.STRUCTURED;
        //ConsistencyModel activeConsistency = ConsistencyModel.SEQUENTIAL;
        ConsistencyModel activeConsistency = ConsistencyModel.EVENTUAL;

        System.out.println("==================================================");
        System.out.println("Starting Full Simulation Suite");
        System.out.println("Global Naming Model: " + activeNaming);
        System.out.println("Global Consistency Model: " + activeConsistency);
        System.out.println("==================================================\n");

        // Iterate sequentially through every scenario
        for (Scenario scenario : Scenario.values()) {
            runSimulation(scenario, activeNaming, activeConsistency);
            Thread.sleep(1000); // Brief pause to separate console output
        }

        System.out.println("All scenarios completed successfully.");
    }

    /**
     * Executes a single self-contained simulation scenario.
     */
    private static void runSimulation(Scenario activeScenario, NamingModel activeNaming,
            ConsistencyModel activeConsistency) throws InterruptedException {
        System.out.println(">>> Executing Scenario: " + activeScenario + " ...");

        // 1. FIX: Adjust parameters to ensure the simulation runs long enough for a
        // crash
        int numUsers = (activeScenario == Scenario.HIGH_CONCURRENCY) ? 100 : 10;
        int numOperations;
        if (activeScenario == Scenario.HIGH_CONCURRENCY)
            numOperations = 1000;
        else if (activeScenario == Scenario.NODE_FAILURE)
            numOperations = 500; // Increased to 500
        else
            numOperations = 100;

        // Initialize Data
        List<String> rooms = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            rooms.add("engineering.blockA.room" + i);
            rooms.add("science.blockB.room" + i);
        }

        // Initialize Nodes
        Node nodeA = new Node("Node-A", rooms);
        Node nodeB = new Node("Node-B", rooms);
        Node nodeC = new Node("Node-C", rooms);
        List<Node> cluster = Arrays.asList(nodeA, nodeB, nodeC);

        Metrics metrics = new Metrics();
        NamingServer namingServer = new NamingServer(activeNaming, cluster, rooms);
        ReplicationManager repManager = new ReplicationManager(cluster, activeConsistency, metrics);

        // 2. FIX: Adjusted timing. Crash at 400ms, recover at 1400ms.
        if (activeScenario == Scenario.NODE_FAILURE) {
            new Thread(() -> {
                try {
                    Thread.sleep(400); // Wait 400ms into the simulation
                    System.out.println("    [ALERT] Node-C has crashed!");
                    nodeC.isOnline = false;
                    Thread.sleep(1000); // Stay offline for 1 full second
                    System.out.println("    [ALERT] Node-C is recovering and catching up...");
                    nodeC.isOnline = true;
                } catch (InterruptedException e) {
                }
            }).start();
        }

        // Execute Concurrent Clients
        ExecutorService clientPool = Executors.newFixedThreadPool(numUsers);
        CountDownLatch latch = new CountDownLatch(numOperations);
        Random random = new Random();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numOperations; i++) {
            clientPool.submit(() -> {
                long reqStart = System.currentTimeMillis();
                String targetRoom = rooms.get(random.nextInt(rooms.size()));
                String userId = "User-" + Thread.currentThread().getId();

                Node targetNode = namingServer.resolve(targetRoom, metrics);

                if (targetNode != null && targetNode.isOnline) {
                    boolean success = repManager.processBooking(targetNode, targetRoom, userId);
                    if (success)
                        metrics.successfulBookings.incrementAndGet();
                }

                metrics.totalRequests.incrementAndGet();
                metrics.totalLatencyMs.addAndGet(System.currentTimeMillis() - reqStart);
                latch.countDown();
            });

            // 3. FIX: Simulate a realistic "arrival rate" for requests.
            // By pausing for 5ms per request, 500 requests will take ~2.5 seconds to
            // submit.
            // This guarantees Node C's 1.4-second crash/recovery window happens exactly
            // mid-simulation.
            if (activeScenario == Scenario.NODE_FAILURE) {
                Thread.sleep(5);
            }
        }

        latch.await();
        long endTime = System.currentTimeMillis();

        // Cleanly shutdown thread pools so the next scenario starts fresh
        clientPool.shutdown();
        clientPool.awaitTermination(2, TimeUnit.SECONDS);
        repManager.shutdown();

        // Print Results
        metrics.printReport(endTime - startTime, activeScenario.toString());
    }
}