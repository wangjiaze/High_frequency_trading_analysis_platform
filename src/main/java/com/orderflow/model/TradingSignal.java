// src/main/java/com/orderflow/model/TradingSignal.java
package com.orderflow.model;

public class TradingSignal {
    private long timestamp;
    private String type;
    private String direction;
    private double strength;
    private String message;
    
    // Getters and setters
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public double getStrength() {
        return strength;
    }

    public void setStrength(double strength) {
        this.strength = strength;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "TradingSignal{" +
                "timestamp=" + timestamp +
                ", type='" + type + '\'' +
                ", direction='" + direction + '\'' +
                ", strength=" + strength +
                ", message='" + message + '\'' +
                '}';
    }
}