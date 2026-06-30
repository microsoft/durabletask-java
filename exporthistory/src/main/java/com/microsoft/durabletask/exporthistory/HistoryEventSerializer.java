// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.microsoft.durabletask.history.HistoryEvent;

import java.util.List;

/**
 * Serializes the {@code com.microsoft.durabletask.history} domain model to the export wire format.
 * <p>
 * Mirrors the .NET {@code ExportInstanceHistoryActivity} serialization: camelCase property names, null fields
 * omitted, ISO-8601 timestamps, and one history event per line for {@link ExportFormatKind#JSONL} (gzip applied by
 * the blob writer) or a JSON array for {@link ExportFormatKind#JSON}.
 * <p>
 * Note: like the .NET implementation, events are serialized by their concrete runtime type without a type
 * discriminator field. Byte-level parity with .NET's output is an open item tracked in the module design.
 */
final class HistoryEventSerializer {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .findAndAddModules()
            .propertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE)
            .serializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    private HistoryEventSerializer() {
    }

    /**
     * Serializes the history events into the format requested by {@code format}.
     *
     * @param historyEvents the ordered history events
     * @param format        the export format
     * @return the serialized content (JSONL text or JSON array text)
     * @throws com.fasterxml.jackson.core.JsonProcessingException if serialization fails
     */
    static String serialize(List<HistoryEvent> historyEvents, ExportFormat format)
            throws com.fasterxml.jackson.core.JsonProcessingException {
        if (format.getKind() == ExportFormatKind.JSON) {
            return MAPPER.writeValueAsString(historyEvents);
        }
        // JSONL: one event per line, serialized by concrete runtime type.
        StringBuilder sb = new StringBuilder();
        for (HistoryEvent event : historyEvents) {
            sb.append(MAPPER.writeValueAsString(event));
            sb.append('\n');
        }
        return sb.toString();
    }

    /**
     * Gets the blob file extension for a format.
     *
     * @param format the export format
     * @return {@code "jsonl.gz"} for JSONL (compressed) or {@code "json"} for JSON
     */
    static String fileExtension(ExportFormat format) {
        return format.getKind() == ExportFormatKind.JSON ? "json" : "jsonl.gz";
    }

    /**
     * Gets whether the format's content is gzip-compressed.
     *
     * @param format the export format
     * @return {@code true} for JSONL, {@code false} for JSON
     */
    static boolean isCompressed(ExportFormat format) {
        return format.getKind() != ExportFormatKind.JSON;
    }

    /**
     * Gets the blob content type for a format.
     *
     * @param format the export format
     * @return the content type
     */
    static String contentType(ExportFormat format) {
        return format.getKind() == ExportFormatKind.JSON ? "application/json" : "application/jsonl+gzip";
    }
}
