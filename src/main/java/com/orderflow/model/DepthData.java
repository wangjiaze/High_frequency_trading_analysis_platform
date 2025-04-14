// src/main/java/com/orderflow/model/DepthData.java
package com.orderflow.model;

import java.util.List;

public class DepthData {
    private long lastUpdateId;
    private List<List<String>> bids;
    private List<List<String>> asks;
    private long localTimestamp;

    // Getters and setters
    public long getLastUpdateId() {
        return lastUpdateId;
    }

    public void setLastUpdateId(long lastUpdateId) {
        this.lastUpdateId = lastUpdateId;
    }

    public List<List<String>> getBids() {
        return bids;
    }

    public void setBids(List<List<String>> bids) {
        this.bids = bids;
    }

    public List<List<String>> getAsks() {
        return asks;
    }

    public void setAsks(List<List<String>> asks) {
        this.asks = asks;
    }

    public long getLocalTimestamp() {
        return localTimestamp;
    }

    public void setLocalTimestamp(long localTimestamp) {
        this.localTimestamp = localTimestamp;
    }
}