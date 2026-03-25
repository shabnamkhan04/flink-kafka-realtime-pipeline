package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

public class FlinkJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // =========================================
        // ✅ Kafka Configuration
        // =========================================
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-group");

        FlinkKafkaConsumer<String> kafkaSource =
                new FlinkKafkaConsumer<>(
                        "user_interactions",
                        new SimpleStringSchema(),
                        props
                );

        // =========================================
        // ✅ Source (Kafka)
        // =========================================
        DataStream<String> kafkaStream = env.addSource(kafkaSource);

        // =========================================
// ✅ JSON → Event (FIXED)
// =========================================
        DataStream<Event> stream = kafkaStream.map(json -> {
            ObjectMapper mapper = new ObjectMapper();
            Event e = mapper.readValue(json, Event.class);

            // ✅ VERY IMPORTANT FIX (reinitialize aggregation fields)
            e.setCount(1);
            e.setMinValue(e.getValue());
            e.setMaxValue(e.getValue());
            e.setSum(e.getValue());

            return e;
        });

        // =========================================
        // ✅ Prepare (SAFE COPY — no mutation issues)
        // =========================================
        DataStream<Event> preparedStream =
                stream.map(e -> new Event(
                        e.getUserId(),
                        e.getItemId(),
                        e.getInteractionType(),
                        e.getTimestamp(),
                        e.getValue()
                ));

        // =========================================
        // ✅ GLOBAL REDUCE (FIXED AVG)
        // =========================================
        DataStream<Event> reducedStream =
                preparedStream
                        .keyBy(e -> "global")
                        .reduce((e1, e2) -> {

                            Event result = new Event(
                                    e1.getUserId(),
                                    e1.getItemId(),
                                    e1.getInteractionType(),
                                    e1.getTimestamp(),
                                    e1.getValue()
                            );

                            int count = e1.getCount() + e2.getCount();
                            int sum = e1.getSum() + e2.getSum();
                            int min = Math.min(e1.getMinValue(), e2.getMinValue());
                            int max = Math.max(e1.getMaxValue(), e2.getMaxValue());

                            result.setCount(count);
                            result.setSum(sum);
                            result.setMinValue(min);
                            result.setMaxValue(max);
                            //result.setAvg((double) sum / count);

                            // ✅ FIX

                            return result;
                        });

        reducedStream.print("Reduced Stats");

        // =========================================
        // =========================================
// ✅ PER USER AGGREGATION (NEW)
// =========================================
        DataStream<Event> perUser =
                preparedStream
                        .keyBy(Event::getUserId)
                        .reduce((e1, e2) -> {

                            Event result = new Event(
                                    e1.getUserId(),
                                    e1.getItemId(),
                                    e1.getInteractionType(),
                                    e1.getTimestamp(),
                                    e1.getValue()
                            );

                            int count = e1.getCount() + e2.getCount();
                            int sum = e1.getSum() + e2.getSum();
                            int min = Math.min(e1.getMinValue(), e2.getMinValue());
                            int max = Math.max(e1.getMaxValue(), e2.getMaxValue());

                            result.setCount(count);
                            result.setSum(sum);
                            result.setMinValue(min);
                            result.setMaxValue(max);
                           // result.setAvg((double) sum / count);

                            // ✅ IMPORTANT

                            return result;
                        });

// ✅ print it
        perUser.print("Per User Stats");
        // ✅ TOTAL COUNT (FIXED)
        // =========================================
        DataStream<Event> countStream =
                preparedStream
                        .keyBy(e -> "global")
                        .reduce((e1, e2) -> {

                            Event result = new Event(
                                    e1.getUserId(),
                                    e1.getItemId(),
                                    e1.getInteractionType(),
                                    e1.getTimestamp(),
                                    e1.getValue()
                            );

                            int count = e1.getCount() + e2.getCount();
                            int sum = e1.getSum() + e2.getSum();

                            result.setCount(count);
                            result.setSum(sum);
                            // ✅ TOTAL COUNT
                            //result.setAvg((double) sum / count);

                            // ✅ FIX

                            return result;
                        });

        countStream.print("Total Count");

        // =========================================
        // ✅ WINDOW 1 → 5 SECOND STATS (FIXED AVG)
        // =========================================
        DataStream<Event> window5s =
                preparedStream
                        .keyBy(Event::getItemId)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .reduce((e1, e2) -> {

                            Event result = new Event(
                                    e1.getUserId(),
                                    e1.getItemId(),
                                    e1.getInteractionType(),
                                    e1.getTimestamp(),
                                    e1.getValue()
                            );

                            int count = e1.getCount() + e2.getCount();
                            int sum = e1.getSum() + e2.getSum();
                            int min = Math.min(e1.getMinValue(), e2.getMinValue());
                            int max = Math.max(e1.getMaxValue(), e2.getMaxValue());

                            result.setCount(count);
                            result.setSum(sum);
                            result.setMinValue(min);
                            result.setMaxValue(max);
                            // ✅ TOTAL COUNT
                           // result.setAvg((double) sum / count);

                            // ✅ FIX

                            return result;
                        });

        window5s.print("5s Window");
        DataStream<Event> alertStream =
                window5s.filter(event -> event.getCount() > 100);

        alertStream.map(event -> {
            System.out.println("🔥 ALERT: High activity for item " + event.getItemId());
            return event;
        }).print("ALERT");

        // =========================================
        // ✅ WINDOW 2 → 1 MINUTE STATS (FIXED AVG)
        // =========================================
        DataStream<Event> window1m =
                preparedStream
                        .keyBy(Event::getItemId)
                        .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                        .reduce((e1, e2) -> {

                            Event result = new Event(
                                    e1.getUserId(),
                                    e1.getItemId(),
                                    e1.getInteractionType(),
                                    e1.getTimestamp(),
                                    e1.getValue()
                            );

                            int count = e1.getCount() + e2.getCount();
                            int sum = e1.getSum() + e2.getSum();
                            int min = Math.min(e1.getMinValue(), e2.getMinValue());
                            int max = Math.max(e1.getMaxValue(), e2.getMaxValue());

                            result.setCount(count);
                            result.setSum(sum);
                            result.setMinValue(min);
                            result.setMaxValue(max);
                           // result.setAvg((double) sum / count); // ✅ ADD

                            // ✅ FIX

                            return result;
                        });

        window1m.print("1m Window");
        // =========================================
// ✅ THROUGHPUT (Events per Second)
// ========================================

        // =========================================
        // ✅ Elasticsearch Sink (NO CHANGE)
        // =========================================
        ElasticsearchSink.Builder<Event> esBuilder =
                new ElasticsearchSink.Builder<>(
                        Collections.singletonList(new HttpHost("localhost", 9200, "http")),
                        (event, ctx, indexer) -> {

                            Map<String, Object> json = new HashMap<>();
                            json.put("itemId", event.getItemId());
                            json.put("count", event.getCount());
                            json.put("min", event.getMinValue());
                            json.put("max", event.getMaxValue());
                            json.put("avg", event.getAvg());
                           // json.put("throughput", event.getAvg());
                            json.put("timestamp", System.currentTimeMillis());

                            IndexRequest request = Requests.indexRequest()
                                    .index("flink-stats")
                                    .source(json);

                            indexer.add(request);
                        }
                );

        window5s.addSink(esBuilder.build());


        // ==============// =========================================

        // ✅ EXISTING Sink (NO CHANGE)
        // =========================================
        reducedStream.addSink(ItemStatsSink.createEventSink());

        // =========================================
        // ✅ Execute
        // =========================================
        env.execute("Flink Streaming Job with Kafka + Window + Elasticsearch");
    }
}