// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Demonstrates how to use {@link ExceptionPropertiesProvider} to attach custom
 * metadata to failure details when activities or orchestrations fail.
 *
 * <p>This allows callers to inspect structured error information (e.g., error codes,
 * severity levels) without parsing exception messages or stack traces.
 */
final class CustomExceptionPropertiesPattern {

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        DurableTaskGrpcWorker worker = createWorker();
        worker.start();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        String instanceId = client.scheduleNewOrchestrationInstance("ProcessOrder");
        System.out.printf("Started orchestration: %s%n", instanceId);

        OrchestrationMetadata result = client.waitForInstanceCompletion(
                instanceId, Duration.ofSeconds(30), true);

        System.out.printf("Status: %s%n", result.getRuntimeStatus());
        if (result.getRuntimeStatus() == OrchestrationRuntimeStatus.FAILED) {
            FailureDetails failure = result.getFailureDetails();
            System.out.printf("Error: %s%n", failure.getErrorMessage());

            // Navigate inner failures to find the root cause with properties
            FailureDetails current = failure;
            while (current.getInnerFailure() != null) {
                current = current.getInnerFailure();
            }
            if (current.getProperties() != null) {
                System.out.printf("Root cause properties: %s%n", current.getProperties());
            }
        }

        worker.stop();
    }

    static class OrderValidationException extends RuntimeException {
        final String errorCode;
        final int severity;

        OrderValidationException(String message, String errorCode, int severity) {
            super(message);
            this.errorCode = errorCode;
            this.severity = severity;
        }
    }

    private static DurableTaskGrpcWorker createWorker() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();

        // Register a provider that extracts custom fields from known exception types
        builder.exceptionPropertiesProvider(exception -> {
            if (exception instanceof OrderValidationException) {
                OrderValidationException ove = (OrderValidationException) exception;
                Map<String, Object> props = new HashMap<>();
                props.put("errorCode", ove.errorCode);
                props.put("severity", ove.severity);
                return props;
            }
            return null;
        });

        builder.addOrchestration(new TaskOrchestrationFactory() {
            @Override
            public String getName() { return "ProcessOrder"; }

            @Override
            public TaskOrchestration create() {
                return ctx -> {
                    ctx.callActivity("ValidateOrder", "order-123", Void.class).await();
                    ctx.complete("done");
                };
            }
        });

        builder.addActivity(new TaskActivityFactory() {
            @Override
            public String getName() { return "ValidateOrder"; }

            @Override
            public TaskActivity create() {
                return ctx -> {
                    throw new OrderValidationException(
                            "Order has invalid items",
                            "INVALID_ITEMS",
                            3);
                };
            }
        });

        return builder.build();
    }
}
