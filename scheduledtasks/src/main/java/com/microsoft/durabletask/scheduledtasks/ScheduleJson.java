// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;

/**
 * Jackson serializers and deserializers that pin persisted schedule state to the .NET wire shape: numeric
 * {@link ScheduleStatus} ordinals, {@code TimeSpan}-formatted intervals, and {@code DateTimeOffset}-formatted
 * timestamps. Referenced from field annotations on {@link ScheduleState} and {@link ScheduleConfiguration}.
 * <p>
 * These are required because Jackson's default temporal and enum handling would emit shapes incompatible with the
 * .NET SDK and the Durable Task Scheduler dashboard.
 */
final class ScheduleJson {

    private ScheduleJson() {
    }

    /** Serializes {@link ScheduleStatus} as its .NET numeric ordinal. */
    public static final class StatusSerializer extends JsonSerializer<ScheduleStatus> {
        @Override
        public void serialize(ScheduleStatus value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeNumber(value.toDotnetOrdinal());
        }
    }

    /** Deserializes {@link ScheduleStatus} from a .NET numeric ordinal (or a legacy string name). */
    public static final class StatusDeserializer extends JsonDeserializer<ScheduleStatus> {
        @Override
        public ScheduleStatus deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            if (p.hasToken(JsonToken.VALUE_NUMBER_INT)) {
                return ScheduleStatus.fromDotnetOrdinal(p.getIntValue());
            }
            return ScheduleStatus.fromPersisted(p.getValueAsString());
        }
    }

    /** Serializes a {@link Duration} as a .NET {@code TimeSpan} constant string. */
    public static final class IntervalSerializer extends JsonSerializer<Duration> {
        @Override
        public void serialize(Duration value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeString(DotNetTimeSpan.format(value));
        }
    }

    /** Deserializes a {@link Duration} from a .NET {@code TimeSpan} constant string. */
    public static final class IntervalDeserializer extends JsonDeserializer<Duration> {
        @Override
        public Duration deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String text = p.getValueAsString();
            if (text == null || text.isEmpty()) {
                return null;
            }
            return DotNetTimeSpan.parse(text);
        }
    }

    /** Serializes an {@link OffsetDateTime} as a .NET {@code DateTimeOffset} round-trip string. */
    public static final class OffsetDateTimeSerializer extends JsonSerializer<OffsetDateTime> {
        @Override
        public void serialize(OffsetDateTime value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeString(DotNetDateTimeOffset.format(value));
        }
    }

    /** Deserializes an {@link OffsetDateTime} from a .NET {@code DateTimeOffset} round-trip string. */
    public static final class OffsetDateTimeDeserializer extends JsonDeserializer<OffsetDateTime> {
        @Override
        public OffsetDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String text = p.getValueAsString();
            if (text == null || text.isEmpty()) {
                return null;
            }
            return DotNetDateTimeOffset.parse(text);
        }
    }
}
