// src/main/java/com/orderflow/model/TradeData.java
package com.orderflow.model;

public class TradeData {
    private long t;          // Trade ID
    private long T;          // Transaction time
    private String p;        // Price
    private String q;        // Quantity
    private boolean m;       // Is the buyer the market maker?
    private boolean M;       // Is this best price match?
    private long localTimestamp;

    // Getters and setters
    public long getT() {
        return t;
    }

    public void setT(long t) {
        this.t = t;
    }

    public long getTransactionTime() {
        return T;
    }

    public void setTransactionTime(long t) {
        this.T = t;
    }

    public String getPrice() {
        return p;
    }

    public void setPrice(String p) {
        this.p = p;
    }

    public String getQuantity() {
        return q;
    }

    public void setQuantity(String q) {
        this.q = q;
    }

    public boolean isMarketMaker() {
        return m;
    }

    public void setMarketMaker(boolean m) {
        this.m = m;
    }

    public boolean isBestPriceMatch() {
        return M;
    }

    public void setBestPriceMatch(boolean m) {
        this.M = m;
    }

    public long getLocalTimestamp() {
        return localTimestamp;
    }

    public void setLocalTimestamp(long localTimestamp) {
        this.localTimestamp = localTimestamp;
    }
}