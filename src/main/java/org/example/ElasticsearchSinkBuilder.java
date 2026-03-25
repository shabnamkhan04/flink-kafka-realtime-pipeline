package org.example;

import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

public class ElasticsearchSinkBuilder {

    public static ElasticsearchSink<Event> createEventSink() {

        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("localhost", 9200, "http"));

        return new ElasticsearchSink.Builder<Event>(
                hosts,
                (Event event, RuntimeContext ctx, RequestIndexer indexer) -> {

                    Map<String, Object> json = new HashMap<>();

                    // ✅ FIX: use getters instead of direct access
                    json.put("user_id", event.getUserId());
                    json.put("item_id", event.getItemId());
                    json.put("interaction_type", event.getInteractionType());
                    json.put("timestamp", event.getTimestamp());

                    IndexRequest request = Requests.indexRequest()
                            .index("user_interactions")
                            .source(json);

                    indexer.add(request);
                }
        ).build();
    }
}