/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distributedsystem;

/**
 *
 * @author PC
 */
public class Message {
    enum Type { EVENTUAL, SEQUENTIAL }
    public enum Command { 
        WRITE,  
        READ,    
        REPLICATE,
        MIGRATE
    }
    private final String senderId;
    private final Type type;
    private final String key;
    private final String value;
    
    private final Command command;
    private final long timeStamp;
    private final int sequenceId;

    public Message(Type type, Command command, String key, String value, long timeStamp, String senderId, int sequenceId) {
        this.type = type;
        this.command = command;
        this.key = key;
        this.value = value;
        this.timeStamp = timeStamp;
        this.senderId = senderId;
        this.sequenceId = sequenceId;
    }

    public Command getCommand() {
        return command;
    }
    
    public int getSequenceId() {
        return sequenceId;
    }

    public String getSenderId() {
        return senderId;
    }


    public Type getType() {
        return type;
    }


    public String getKey() {
        return key;
    }


    public String getValue() {
        return value;
    }


    public long getTimeStamp() {
        return timeStamp;
    }
    
}

