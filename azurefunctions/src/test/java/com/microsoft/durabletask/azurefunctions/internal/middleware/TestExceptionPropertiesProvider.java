// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions.internal.middleware;

import com.microsoft.durabletask.ExceptionPropertiesProvider;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Public {@link ExceptionPropertiesProvider} used only by {@link ActivityMiddlewareTest} to verify
 * SPI discovery across class loaders. It must be {@code public} with a public no-arg constructor so
 * {@link java.util.ServiceLoader} can instantiate it.
 */
public class TestExceptionPropertiesProvider implements ExceptionPropertiesProvider {

    @Override
    public Map<String, Object> getExceptionProperties(Exception exception) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("discoveredVia", "serviceLoader");
        return properties;
    }
}
