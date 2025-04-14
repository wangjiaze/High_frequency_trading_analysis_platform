// src/main/java/com/orderflow/model/OrderFlowMetrics.java
package com.orderflow.model;

public class OrderFlowMetrics {
    private long timestamp;
    private double latestPrice;
    private double bidAskRatio;
    private double topPressureRatio;
    private double delta;
    private double vwap;
    private double buyVolume;
    private double sellVolume;

    // Getters
    public long getTimestamp() {
        return timestamp;
    }

    public double getLatestPrice() {
        return latestPrice;
    }

    public double getBidAskRatio() {
        return bidAskRatio;
    }

    public double getTopPressureRatio() {
        return topPressureRatio;
    }

    public double getDelta() {
        return delta;
    }

    public double getVwap() {
        return vwap;
    }

    public double getBuyVolume() {
        return buyVolume;
    }

    public double getSellVolume() {
        return sellVolume;
    }

    // Setters
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setLatestPrice(double latestPrice) {
        this.latestPrice = latestPrice;
    }

    public void setBidAskRatio(double bidAskRatio) {
        this.bidAskRatio = bidAskRatio;
    }

    public void setTopPressureRatio(double topPressureRatio) {
        this.topPressureRatio = topPressureRatio;
    }

    public void setDelta(double delta) {
        this.delta = delta;
    }

    public void setVwap(double vwap) {
        this.vwap = vwap;
    }

    public void setBuyVolume(double buyVolume) {
        this.buyVolume = buyVolume;
    }

    public void setSellVolume(double sellVolume) {
        this.sellVolume = sellVolume;
    }

    @Override
    public String toString() {
        return "OrderFlowMetrics{" +
                "timestamp=" + timestamp +
                ", latestPrice=" + latestPrice +
                ", bidAskRatio=" + bidAskRatio +
                ", topPressureRatio=" + topPressureRatio +
                ", delta=" + delta +
                ", vwap=" + vwap +
                ", buyVolume=" + buyVolume +
                ", sellVolume=" + sellVolume +
                '}';
    }
}