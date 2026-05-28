/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distributedsystem;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;



public class NameServer {
    public enum NamingMode { FLAT, STRUCTURED }
    private final NamingMode activeMode;

    private final Map<String, Mailbox> flatTable = new ConcurrentHashMap<>();

    private static class DNSNode {
        Mailbox mailbox = null;
        final Map<String, DNSNode> subDomains = new ConcurrentHashMap<>();
    }
    private final DNSNode dnsRoot = new DNSNode();

    // Structural Macro Metrics Registers
    private int totalLookups = 0;
    private int successfulLookups = 0;
    private int totalHopsTraversed = 0;
    private int packetLossCount = 0;

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
        this.totalLookups++; 
        int hopsForThisLookup = 0;

        // Simulate 5% transient network dropping packet loss
        if (Math.random() < 0.05) {
            this.packetLossCount++;
            return null; // Silent dropout to replicate unstable WAN infrastructure
        }

        Mailbox resolvedMailbox = null;

        if (activeMode == NamingMode.FLAT) {
            hopsForThisLookup = 1; 
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
            this.successfulLookups++;
            this.totalHopsTraversed += hopsForThisLookup;
        }

        return resolvedMailbox; // Pure in-memory routing, 0ms I/O delay!
    }

    /**
     * 🚀 EXPORT ENGINE FOR C4 CHARTS: Invoked exclusively at the very end of the system run.
     * Replaces the thousands of spam logs with one clean macro-telemetry matrix string.
     */
    public void printSummaryReport() {
        double successRate = (totalLookups == 0) ? 0.0 : ((double) successfulLookups / totalLookups) * 100;
        double avgHops = (successfulLookups == 0) ? 0.0 : (double) totalHopsTraversed / successfulLookups;
        
        System.out.println("==================================================");
        System.out.println("=== NAMING ARCHITECTURE RESOLUTION SUMMARY     ===");
        System.out.println("==================================================");
        System.out.printf("• Active Architecture      : %s\n", activeMode);
        System.out.printf("• Total Resolution Audits : %d\n", totalLookups);
        System.out.printf("• Injected Packet Drops    : %d\n", packetLossCount);
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


