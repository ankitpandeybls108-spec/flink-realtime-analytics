package com.flink.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.model.Order;

public class JsonToOrder {

    private static final ObjectMapper mapper =
            new ObjectMapper();

    public static Order parse(String json) throws Exception {

        return mapper.readValue(json, Order.class);
    }
}