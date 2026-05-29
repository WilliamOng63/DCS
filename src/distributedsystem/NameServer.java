/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distributedsystem;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;



public class NameServer {
    public enum NamingMode { FLAT, STRUCTURED }
    private final NamingMode activeMode;

    // DHT Cluster Configuration
    private static final int TOTAL_NODES = 3;

    private final Map<String, Mailbox> flatTable = new ConcurrentHashMap<>();

    private static class DNSNode {
        Mailbox mailbox = null;
        final Map<String, DNSNode> subDomains = new ConcurrentHashMap<>();
    }
    private final DNSNode dnsRoot = new DNSNode();

    // Structural Macro Metrics Registers (thread-safe atomic counters)
    private final AtomicInteger totalLookups = new AtomicInteger(0);
    private final AtomicInteger successfulLookups = new AtomicInteger(0);
    private final AtomicInteger packetLossCount = new AtomicInteger(0);
    private double totalHopsTraversed = 0.0; // Protected by synchronized access below

    public NameServer(NamingMode activeMode) {
        this.activeMode = activeMode;
        System.out.println("[DNS BOOT] Naming Service initialized in mode: " + activeMode);
    }

    /**
     * Boot-up registration phase. Logs are preserved here since it executes only once.
     */
    public void register(String name, Mailbox mailbox) {
        if (activeMode == NamingMode.FLAT) {
            flatTable.put(name, mailbox);
            System.out.println("[REGISTRATION] Flat Name recorded: \"" + name + "\" -> Bound to Mailbox.");
        } else {
            String[] parts = name.split("\\.");
            DNSNode current = dnsRoot;
            for (String part : parts) {
                current.subDomains.putIfAbsent(part, new DNSNode());
                current = current.subDomains.get(part);
            }
            current.mailbox = mailbox;
            System.out.println("[REGISTRATION] Structured Tree Path created: Root -> " 
                    + String.join(" -> ", parts) + " -> Attached to Mailbox.");
        }
    }

    /**
     * 🚀 REWRITTEN FOR HIGH-THROUGHPUT QUIET EXECUTION
     * All console prints inside the massive concurrent loops have been successfully removed.
     */
    public Mailbox resolve(String name) {
        this.totalLookups.incrementAndGet(); 
        double hopsForThisLookup = 1.0; // Start at 1 for root lookup

        // Simulate 5% transient network dropping packet loss
        if (Math.random() < 0.05) {
            this.packetLossCount.incrementAndGet();
            return null; // Silent dropout to replicate unstable WAN infrastructure
        }

        Mailbox resolvedMailbox = null;

        if (activeMode == NamingMode.FLAT) {
            hopsForThisLookup = calculateDHTHops(TOTAL_NODES);
            resolvedMailbox = flatTable.get(name);
        } else {
            String[] parts = name.split("\\.");
            DNSNode current = dnsRoot;
            
            for (String part : parts) {
                if (current != null) {
                    current = current.subDomains.get(part);
                    hopsForThisLookup++; // Increment after each level traversal
                }
            }
            if (current != null) {
                resolvedMailbox = current.mailbox;
            }
        }

        if (resolvedMailbox != null) {
            this.successfulLookups.incrementAndGet();
            synchronized (this) {
                this.totalHopsTraversed += hopsForThisLookup;
            }
        }

        return resolvedMailbox; // Pure in-memory routing, 0ms I/O delay!
    }

    /**
     * Calculate DHT (Distributed Hash Table) hop count using Chord-like O(log N) complexity.
     * Simulates real-world routing variance with +/- 0.5 hop randomization.
     * Raw logarithmic hops preserve DHT efficiency advantage over structured naming.
     */
    private double calculateDHTHops(int clusterSize) {
        if (clusterSize <= 1) return 1.0;
        
        // O(log N) complexity: logarithmic hops for Chord DHT
        double baseHops = Math.log(clusterSize) / Math.log(2);
        
        // Add randomization of +/- 0.5 hops to simulate real-world WAN routing variance
        double randomVariance = (Math.random() - 0.5); // Generates -0.5 to +0.5
        
        return Math.max(1.0, baseHops + randomVariance);
    }

    /**
     * 🚀 EXPORT ENGINE FOR C4 CHARTS: Invoked exclusively at the very end of the system run.
     * Replaces the thousands of spam logs with one clean macro-telemetry matrix string.
     */
    public void printSummaryReport() {
        int totalLookupsValue = totalLookups.get();
        int successfulLookupsValue = successfulLookups.get();
        double totalHopsValue;
        synchronized (this) {
            totalHopsValue = totalHopsTraversed;
        }
        
        double successRate = (totalLookupsValue == 0) ? 0.0 : ((double) successfulLookupsValue / totalLookupsValue) * 100;
        double avgHops = (successfulLookupsValue == 0) ? 0.0 : totalHopsValue / successfulLookupsValue;
        
        System.out.println("==================================================");
        System.out.println("=== NAMING ARCHITECTURE RESOLUTION SUMMARY     ===");
        System.out.println("==================================================");
        System.out.printf("• Active Architecture      : %s\n", activeMode);
        System.out.printf("• Total Resolution Audits : %d\n", totalLookupsValue);
        System.out.printf("• Injected Packet Drops    : %d\n", packetLossCount.get());
        System.out.printf("• Address Lookup Success   : %.2f%%\n", successRate);
        System.out.printf("• Average Traversal Hops   : %.2f hops\n", avgHops);
        System.out.println("==================================================\n");
    }

    public List<String> getAllNodes() {
        if (activeMode == NamingMode.FLAT) {
            return new ArrayList<>(flatTable.keySet());
        } else {
            return Arrays.asList("star.asia.Node_A", "star.europe.Node_B", "onworld.asia.Node_C");
        }
    }
}


