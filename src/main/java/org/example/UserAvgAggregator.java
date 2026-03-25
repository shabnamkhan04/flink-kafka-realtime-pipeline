package org.example;

import org.apache.flink.api.common.functions.AggregateFunction;

public class UserAvgAggregator implements AggregateFunction<Event, UserAvgAggregator.AvgAccumulator, Double> {

    // Better name
    public static class AvgAccumulator {
        int count = 0;
        long sum = 0;
    }

    @Override
    public AvgAccumulator createAccumulator() {
        return new AvgAccumulator();
    }

    @Override
    public AvgAccumulator add(Event value, AvgAccumulator acc) {
        acc.count++;
        acc.sum += value.getValue(); // ✅ FIX THIS (use actual field)
        return acc;
    }

    @Override
    public Double getResult(AvgAccumulator acc) {
        return acc.count == 0 ? 0.0 : (double) acc.sum / acc.count;
    }

    @Override
    public AvgAccumulator merge(AvgAccumulator a, AvgAccumulator b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }
}