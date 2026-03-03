// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerClientExtensions;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerWorkerExtensions;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Demonstrates OpenTelemetry distributed tracing with the Durable Task SDK.
 * Traces are exported to Jaeger via OTLP/gRPC for visualization.
 *
 * <p>The Java SDK's client automatically propagates W3C trace context
 * (traceparent/tracestate) when scheduling orchestrations. Activity spans
 * are created automatically by the worker around each activity execution.
 *
 * <p>Prerequisites:
 * <ul>
 *   <li>DTS emulator: {@code docker run -d -p 8080:8080 -p 8082:8082 mcr.microsoft.com/dts/dts-emulator:latest}</li>
 *   <li>Jaeger: {@code docker run -d -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one:latest}</li>
 * </ul>
 */
final class TracingPattern {
    private static final Logger logger = Logger.getLogger(TracingPattern.class.getName());

    // Shared parent context so activity spans become children of the orchestration span
    private static volatile Context orchestrationContext;

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        // Configure OpenTelemetry with OTLP exporter to Jaeger
        String otlpEndpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
        if (otlpEndpoint == null) {
            otlpEndpoint = "http://localhost:4317";
        }

        OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(otlpEndpoint)
                .build();

        Resource resource = Resource.builder()
                .put("service.name", "durabletask-java-tracing-sample")
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                .build();

        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();

        Tracer tracer = openTelemetry.getTracer("Microsoft.DurableTask");
        logger.info("OpenTelemetry configured with OTLP exporter at " + otlpEndpoint);

        // Build connection string for DTS emulator
        String connectionString = System.getenv("DURABLE_TASK_CONNECTION_STRING");
        if (connectionString == null) {
            String endpoint = System.getenv("ENDPOINT");
            String taskHub = System.getenv("TASKHUB");
            if (endpoint == null) endpoint = "http://localhost:8080";
            if (taskHub == null) taskHub = "default";

            String authType = endpoint.startsWith("http://localhost") ? "None" : "DefaultAzure";
            connectionString = String.format("Endpoint=%s;TaskHub=%s;Authentication=%s",
                    endpoint, taskHub, authType);
        }
        logger.info("Using connection string: " + connectionString);

        // Create worker with orchestration and activities
        DurableTaskGrpcWorker worker = DurableTaskSchedulerWorkerExtensions
                .createWorkerBuilder(connectionString)
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "FanOutFanIn"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            // Fan-out: schedule multiple parallel activities
                            List<Task<String>> parallelTasks = new java.util.ArrayList<>();
                            String[] cities = {"Seattle", "Tokyo", "London", "Paris", "Sydney"};
                            for (String city : cities) {
                                parallelTasks.add(
                                    ctx.callActivity("GetWeather", city, String.class));
                            }

                            // Fan-in: wait for all activities to complete
                            List<String> results = ctx.allOf(parallelTasks).await();

                            // Aggregate results
                            String summary = ctx.callActivity(
                                    "CreateSummary", String.join(", ", results), String.class).await();

                            ctx.complete(summary);
                        };
                    }
                })
                .addActivity(new TaskActivityFactory() {
                    @Override public String getName() { return "GetWeather"; }
                    @Override public TaskActivity create() {
                        return ctx -> {
                            String city = ctx.getInput(String.class);
                            logger.info("[GetWeather] Getting weather for: " + city);
                            try { Thread.sleep(20); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                            return city + "=72F";
                        };
                    }
                })
                .addActivity(new TaskActivityFactory() {
                    @Override public String getName() { return "CreateSummary"; }
                    @Override public TaskActivity create() {
                        return ctx -> {
                            String input = ctx.getInput(String.class);
                            logger.info("[CreateSummary] Creating summary for: " + input);
                            return "Weather Report: " + input;
                        };
                    }
                })
                .build();

        // Start worker and wait for it to connect
        worker.start();
        Thread.sleep(5000);
        logger.info("Worker started with OpenTelemetry tracing.");

        // Create client
        DurableTaskClient client = DurableTaskSchedulerClientExtensions
                .createClientBuilder(connectionString).build();

        // Create a parent span — the SDK automatically propagates W3C trace context
        Span orchestrationSpan = tracer.spanBuilder("create_orchestration:FanOutFanIn")
                .setAttribute("durabletask.task.name", "FanOutFanIn")
                .setAttribute("durabletask.type", "orchestration")
                .startSpan();

        orchestrationContext = Context.current().with(orchestrationSpan);

        String instanceId;
        try (Scope scope = orchestrationSpan.makeCurrent()) {
            logger.info("Scheduling FanOutFanIn orchestration...");
            instanceId = client.scheduleNewOrchestrationInstance(
                    "FanOutFanIn",
                    new NewOrchestrationInstanceOptions().setInput("weather-request"));
            orchestrationSpan.setAttribute("durabletask.task.instance_id", instanceId);
            logger.info("Started orchestration: " + instanceId);
        }

        // Wait for completion
        logger.info("Waiting for completion...");
        OrchestrationMetadata result = client.waitForInstanceCompletion(
                instanceId, Duration.ofSeconds(60), true);
        orchestrationSpan.end();

        logger.info("Status: " + result.getRuntimeStatus());
        logger.info("Result: " + result.readOutputAs(String.class));
        logger.info("");
        logger.info("View traces in Jaeger UI: http://localhost:16686");
        logger.info("  Search for service: durabletask-java-tracing-sample");
        logger.info("View orchestration in DTS Dashboard: http://localhost:8082");

        // Flush traces and shut down
        tracerProvider.forceFlush();
        Thread.sleep(2000);
        tracerProvider.shutdown();
        worker.stop();
        System.exit(0);
    }
}
