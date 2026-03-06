package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.Task;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Sample demonstrating distributed tracing with Durable Functions.
 * <p>
 * Uses a Fan-Out/Fan-In pattern with a timer, matching the DTS sample (TracingPattern.java).
 * Trace context is automatically propagated from the HTTP trigger through the
 * orchestration to each activity. When Application Insights is configured,
 * you will see correlated traces across the entire workflow.
 */
public class TracingChain {

    @FunctionName("StartFanOutFanIn")
    public HttpResponseMessage startFanOutFanIn(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Starting FanOutFanIn orchestration");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("FanOutFanIn");
        context.getLogger().info("Created orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * Fan-Out/Fan-In orchestration: waits 1s, then runs 5 GetWeather activities
     * in parallel, and finally calls CreateSummary to aggregate the results.
     */
    @FunctionName("FanOutFanIn")
    public String fanOutFanIn(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {

        // Wait 1 second (creates a timer span)
        ctx.createTimer(Duration.ofSeconds(1)).await();

        // Fan-out: schedule 5 GetWeather activities in parallel
        List<String> cities = Arrays.asList("Seattle", "Tokyo", "London", "Paris", "Sydney");
        List<Task<String>> tasks = cities.stream()
                .map(city -> ctx.callActivity("GetWeather", city, String.class))
                .collect(Collectors.toList());
        List<String> results = ctx.allOf(tasks).await();

        // Fan-in: aggregate results
        String combined = String.join(", ", results);
        return ctx.callActivity("CreateSummary", combined, String.class).await();
    }

    @FunctionName("GetWeather")
    public String getWeather(
            @DurableActivityTrigger(name = "city") String city,
            final ExecutionContext context) {
        context.getLogger().info("[GetWeather] Getting weather for: " + city);
        return city + "=72F";
    }

    @FunctionName("CreateSummary")
    public String createSummary(
            @DurableActivityTrigger(name = "input") String input,
            final ExecutionContext context) {
        context.getLogger().info("[CreateSummary] Creating summary for: " + input);
        return "Weather Report: " + input;
    }
}
