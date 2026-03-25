package org.example;

import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

public class ItemStatsSink {

    // =========================================
    // ✅ 1. Sink for Event (USED WITH REDUCE)
    // =========================================
    public static ElasticsearchSink<Event> createEventSink() {

        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("localhost", 9200, "http"));

        return new ElasticsearchSink.Builder<Event>(
                hosts,
                (Event event, RuntimeContext ctx, RequestIndexer indexer) -> {

                    Map<String, Object> json = new HashMap<>();
                    json.put("item_id", event.getItemId());
                    json.put("count", event.getCount());
                    json.put("timestamp", event.getTimestamp());

                    IndexRequest request = Requests.indexRequest()
                            .index("item_stats")
                            .source(json);

                    indexer.add(request);
                }
        ).build();
    }

    // =========================================
    // ✅ 2. Sink for Stats (USED WITH aggregate)
    // =========================================
    public static ElasticsearchSink<ItemStatsAggregator.Stats> createSink() {

        List<HttpHost> hosts = Collections.singletonList(
                new HttpHost("localhost", 9200, "http")
        );

        return new ElasticsearchSink.Builder<ItemStatsAggregator.Stats>(
                hosts,
                (ItemStatsAggregator.Stats element, RuntimeContext ctx, RequestIndexer indexer) -> {

                    Map<String, Object> json = new HashMap<>();
                    json.put("count", element.count);
                    json.put("min", element.min);
                    json.put("max", element.max);

                    IndexRequest request = Requests.indexRequest()
                            .index("item_stats")
                            .source(json);

                    indexer.add(request);
                }
        ).build();
    }
}