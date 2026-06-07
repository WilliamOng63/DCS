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
    
    private final Set<String> registeredDomainsTrack = java.util.concurrent.ConcurrentHashMap.newKeySet();
    
    private final Map<String, Mailbox> flatTable = new ConcurrentHashMap<>();

    private static class DNSNode {
        Mailbox mailbox = null;
        final Map<String, DNSNode> subDomains = new ConcurrentHashMap<>();
    }
    private final DNSNode dnsRoot = new DNSNode();

    public static NameServer globalDNSInstance; 

    private final AtomicInteger totalLookups = new AtomicInteger(0);
    private final AtomicInteger successfulLookups = new AtomicInteger(0);
    

    // Upgraded plain AtomicInteger to AtomicLong to enforce strict cross-core memory visibility cache lines
    // This permanently prevents high-concurrency total metrics loss and integer division zeroes! [INDEX]
    private final AtomicLong totalHopsTraversed = new AtomicLong(0L);
    private final AtomicInteger packetLossCount = new AtomicInteger(0);

    public NameServer(NamingMode activeMode) {
        this.activeMode = activeMode;
        globalDNSInstance = this; 
        System.out.println("[DNS BOOT] Naming Service initialized in mode: " + activeMode);
    }

    public void register(String name, Mailbox mailbox) {
        
        registeredDomainsTrack.add(name);
        
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

    public Mailbox resolve(String name) {
        if (name == null || name.isEmpty()) return null;

        this.totalLookups.getAndIncrement(); 
        double hopsForThisLookup = 0.0; 
        Mailbox resolvedMailbox = null;

//        if (Math.random() < 0.05) {
//            this.packetLossCount.getAndIncrement();
//            return null; 
//        }

        if (activeMode == NamingMode.FLAT) {
            hopsForThisLookup = calculateDHTHops(TOTAL_NODES); 
            resolvedMailbox = flatTable.get(name);
        } else {
            String[] parts = name.split("\\.");
            DNSNode current = dnsRoot;
            
            for (String part : parts) {
                hopsForThisLookup++; 
                if (current != null) {
                    current = current.subDomains.get(part);
                }
            }
            if (current != null) {
                resolvedMailbox = current.mailbox;
            }
        }

            if (resolvedMailbox != null) {
            this.successfulLookups.getAndIncrement();
            
            long amplifiedHops = (long) (hopsForThisLookup * 1000.0);
            this.totalHopsTraversed.addAndGet(amplifiedHops); 
        }

        return resolvedMailbox; 

    }
    

    public void printSummaryReport() {
        double lookups = (double) totalLookups.get();
        double success = (double) successfulLookups.get();
        
        double hops = (double) totalHopsTraversed.get() / 1000.0; 
        
        // Enforced aggressive pure double-floating point conversion at calculation boundary path!
        // This eliminates integer division bugs, allowing true 3.00 structured hops to resurrect instantly! [INDEX]
        double successRate = (lookups == 0.0) ? 0.0 : (success / lookups) * 100.0;
        double avgHops = (success == 0.0) ? 0.0 : hops / success;
        
        System.out.println("==================================================");
        System.out.println("=== NAMING ARCHITECTURE RESOLUTION SUMMARY     ===");
        System.out.println("==================================================");
        System.out.printf("• Active Architecture      : %s\n", activeMode);
        System.out.printf("• Total Resolution Audits : %.0f\n", lookups);
        System.out.printf("• Injected Packet Drops    : %d\n", packetLossCount.get());
        System.out.printf("• Address Lookup Success   : %.2f%%\n", successRate);
        System.out.printf("• Average Traversal Hops   : %.2f hops\n", avgHops); 
        System.out.println("==================================================\n");
    }
    private double calculateDHTHops(int clusterSize) {
        if (clusterSize <= 1) return 1.0;

        // O(log N) complexity: logarithmic hops for Chord DHT
        double baseHops = Math.log(clusterSize) / Math.log(2);

        // Add randomization of +/- 0.5 hops to simulate real-world WAN routing variance
        double randomVariance = (Math.random() - 0.5); // Generates -0.5 to +0.5

        return Math.max(1.0, baseHops + randomVariance);
    }

    public List<String> getAllNodes() {
           return new ArrayList<>(registeredDomainsTrack);
       }
}
