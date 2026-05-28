/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package distributedsystem;

/**
 *
 * @author PC
 */
public interface Node {
    String getNodeId();
    boolean alive();
    void crash();
    void restart();
    
}
