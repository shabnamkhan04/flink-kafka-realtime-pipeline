package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonToEventMapper implements MapFunction<String, Event> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Event map(String value) throws Exception {
        return mapper.readValue(value, Event.class);
    }
}