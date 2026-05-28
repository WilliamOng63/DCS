/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package distributedsystem;

/**
 *
 * @author PC
 */
public interface consistencyHandler {
    void handleWrite(DistributedNode node, Message msg);
    void handleRead(DistributedNode node, Message msg);
    void handleReplicate(DistributedNode node, Message msg);
    void handleRestart(DistributedNode node);
}
