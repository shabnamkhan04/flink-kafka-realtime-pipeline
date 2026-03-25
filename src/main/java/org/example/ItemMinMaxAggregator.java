package org.example;

import org.apache.flink.api.common.functions.ReduceFunction;

public class ItemMinMaxAggregator implements ReduceFunction<Event> {

    @Override
    public Event reduce(Event e1, Event e2) {

        // ✅ Count
        e1.setCount(e1.getCount() + e2.getCount());

        // ✅ Min / Max
        e1.setMinValue(Math.min(e1.getMinValue(), e2.getMinValue()));
        e1.setMaxValue(Math.max(e1.getMaxValue(), e2.getMaxValue()));

        // ✅ Sum
        e1.setSum(e1.getSum() + e2.getSum());

        // ✅ Latest timestamp
        e1.setTimestamp(Math.max(e1.getTimestamp(), e2.getTimestamp()));

        // ✅ Keep meaningful fields (avoid null overwrite)
        if (e1.getUserId() == null) {
            e1.setUserId(e2.getUserId());
        }
        if (e1.getInteractionType() == null) {
            e1.setInteractionType(e2.getInteractionType());
        }

        return e1;
    }
}