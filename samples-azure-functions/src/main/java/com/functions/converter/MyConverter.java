package com.functions.converter;

import com.google.gson.Gson;
import com.microsoft.durabletask.DataConverter;

public class MyConverter implements DataConverter {

    private static final Gson gson = new Gson();
    @Override
    public String serialize(Object value) {
        return gson.toJson(value);
    }

    @Override
    public <T> T deserialize(String data, Class<T> target) {
        return gson.fromJson(data, target);
    }
}
