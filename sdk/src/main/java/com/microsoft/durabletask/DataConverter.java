// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public interface DataConverter {
    String serialize(Object value);
    <T> T deserialize(String data, Class<T> target) throws DataConverterException;

    default DataConverterException wrapConverterException(String message, Throwable cause) {
        return new DataConverterException(message, cause);
    }

    // TODO: Should this be a checked exception? Probably, but if so, we should change the programming model so
    //       that most developers don't need to catch it for things like handling  task inputs.
    class DataConverterException extends RuntimeException {
        public DataConverterException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static Instant getInstantFromTimestamp(Timestamp ts) {
        if (ts == null) {
            return null;
        }

        // We don't include nanoseconds because of round-trip issues
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()).truncatedTo(ChronoUnit.MILLIS);
    }

    static Timestamp getTimestampFromInstant(Instant instant) {
        if (instant == null) {
            return null;
        }

        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
