package org.example;

import java.io.Serializable;

public class Event implements Serializable {

    private String userId;
    private String itemId;
    private String interactionType;
    private long timestamp;
    private int value;
    private int count;

    // ✅ Aggregation fields
    private int minValue;
    private int maxValue;
    private int sum;
    private double avg;   // ✅ already present

    public Event() {}

    public Event(String userId, String itemId, String interactionType, long timestamp, int value) {
        this.userId = userId;
        this.itemId = itemId;
        this.interactionType = interactionType;
        this.timestamp = timestamp;
        this.value = value;

        // ✅ Initialize aggregation fields
        this.count = 1;
        this.minValue = value;
        this.maxValue = value;
        this.sum = value;

        // ✅ ADD THIS (for consistency with Flink reduce)
        this.avg = value;
    }

    // =========================
    // GETTERS
    // =========================
    public String getUserId() { return userId; }
    public String getItemId() { return itemId; }
    public String getInteractionType() { return interactionType; }
    public long getTimestamp() { return timestamp; }
    public int getValue() { return value; }
    public int getCount() { return count; }
    public int getMinValue() { return minValue; }
    public int getMaxValue() { return maxValue; }
    public int getSum() { return sum; }

    // ✅ Keep your dynamic avg (GOOD)
    public double getAvg() {
        return count == 0 ? 0 : (double) sum / count;
    }

    // =========================
    // SETTERS
    // =========================
    public void setUserId(String userId) { this.userId = userId; }
    public void setItemId(String itemId) { this.itemId = itemId; }
    public void setInteractionType(String interactionType) { this.interactionType = interactionType; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public void setValue(int value) { this.value = value; }
    public void setCount(int count) { this.count = count; }
    public void setMinValue(int minValue) { this.minValue = minValue; }
    public void setMaxValue(int maxValue) { this.maxValue = maxValue; }
    public void setSum(int sum) { this.sum = sum; }

    // ✅ ADD THIS (fix for Flink reduce usage)
    public void setAvg(double avg) { this.avg = avg; }

    // =========================
    // toString
    // =========================
    @Override
    public String toString() {
        return "Event{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", interactionType='" + interactionType + '\'' +
                ", timestamp=" + timestamp +
                ", value=" + value +
                ", count=" + count +
                ", minValue=" + minValue +
                ", maxValue=" + maxValue +
                ", sum=" + sum +
                ", avg=" + getAvg() +   // ✅ always correct
                '}';
    }
}