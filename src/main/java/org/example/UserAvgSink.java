package org.example;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

public class UserAvgSink {

    public static SinkFunction<String> createSink() {

        List<HttpHost> hosts = Collections.singletonList(
                new HttpHost("localhost", 9200, "http")
        );

        return new ElasticsearchSink.Builder<String>(
                hosts,
                (element, ctx, indexer) -> {

                    Map<String, Object> json = new HashMap<>();
                    json.put("data", element);

                    IndexRequest request = Requests.indexRequest()
                            .index("user_avg_metrics")
                            .source(json);

                    indexer.add(request);
                }
        ).build();
    }
}