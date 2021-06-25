// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

public interface DataConverter {
    String serialize(Object value);
    <T> T deserialize(String data, Class<T> target);

    default DataConverterException wrapConverterException(String message, Throwable cause) {
        return new DataConverterException(message, cause);
    }

    class DataConverterException extends RuntimeException {
        public DataConverterException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
