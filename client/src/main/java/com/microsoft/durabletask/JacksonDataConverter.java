// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * An implementation of {@link DataConverter} that uses Jackson APIs for data serialization.
 */
public final class JacksonDataConverter implements DataConverter {
    // Static singletons are recommended by the Jackson documentation
    private static final ObjectMapper jsonObjectMapper = JsonMapper.builder()
            .findAndAddModules()
            .build();

    @Override
    public String serialize(Object value) {
        if (value == null) {
            return null;
        }

        try {
            return jsonObjectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new DataConverterException(
                    String.format("Failed to serialize argument of type '%s'. Detail error message: %s", value.getClass().getName(), e.getMessage()),
                    e);
        }
    }

    @Override
    public <T> T deserialize(String jsonText, Class<T> targetType) {
        if (jsonText == null || jsonText.length() == 0 || targetType == Void.class) {
            return null;
        }

        try {
            return jsonObjectMapper.readValue(jsonText, targetType);
        } catch (JsonProcessingException e) {
            throw new DataConverterException(String.format("Failed to deserialize the JSON text to %s. Detail error message: %s", targetType.getName(), e.getMessage()), e);
        }
    }
}
