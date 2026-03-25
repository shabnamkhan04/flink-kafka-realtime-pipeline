package org.example;

import org.apache.flink.api.common.functions.AggregateFunction;

public class ItemStatsAggregator implements
        AggregateFunction<Event, ItemStatsAggregator.Stats, ItemStatsAggregator.Stats> {

    public static class Stats {
        public int count = 0;
        public int min = Integer.MAX_VALUE;
        public int max = Integer.MIN_VALUE;
    }

    @Override
    public Stats createAccumulator() {
        return new Stats();
    }

    @Override
    public Stats add(Event value, Stats acc) {
        acc.count++;
        acc.min = Math.min(acc.min, value.getValue());
        acc.max = Math.max(acc.max, value.getValue());
        return acc;
    }

    @Override
    public Stats getResult(Stats acc) {
        return acc;
    }

    @Override
    public Stats merge(Stats a, Stats b) {
        a.count += b.count;
        a.min = Math.min(a.min, b.min);
        a.max = Math.max(a.max, b.max);
        return a;
    }
}