/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distributedsystem;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 * @author PC
 */
public class Mailbox {
    public final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final Random random = new Random();
    
    private static final ExecutorService networkWireExecutor = Executors.newCachedThreadPool();
    
    public void send(Mailbox targetMailbox,Message msg) {
         CompletableFuture.runAsync(() -> {
            try {
                int latency = random.nextInt(700) + 100;
                Thread.sleep(latency);
                
                targetMailbox.queue.offer(msg);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        },networkWireExecutor);
    }
    
    public Message receive() throws InterruptedException {
        return queue.take();  // blocks until message available
    }
    
    public Message poll() {
        return queue.poll();  // non‑blocking, returns null if empty
    }
    
}
