// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Provider interface for extracting custom properties from exceptions.
 * <p>
 * Implementations of this interface can be registered with a {@link DurableTaskGrpcWorkerBuilder} to include
 * custom exception properties in {@link FailureDetails} when activities or orchestrations fail.
 * These properties are then available via {@link FailureDetails#getProperties()}.
 * <p>
 * Example usage:
 * <pre>{@code
 * DurableTaskGrpcWorker worker = new DurableTaskGrpcWorkerBuilder()
 *     .exceptionPropertiesProvider(exception -> {
 *         if (exception instanceof MyCustomException) {
 *             MyCustomException custom = (MyCustomException) exception;
 *             Map<String, Object> props = new HashMap<>();
 *             props.put("errorCode", custom.getErrorCode());
 *             props.put("retryable", custom.isRetryable());
 *             return props;
 *         }
 *         return null;
 *     })
 *     .addOrchestration(...)
 *     .build();
 * }</pre>
 */
@FunctionalInterface
public interface ExceptionPropertiesProvider {

    /**
     * Extracts custom properties from the given exception.
     * <p>
     * Return {@code null} or an empty map if no custom properties should be included for this exception.
     *
     * @param exception the exception to extract properties from
     * @return a map of property names to values, or {@code null}
     */
    @Nullable
    Map<String, Object> getExceptionProperties(Exception exception);
}
