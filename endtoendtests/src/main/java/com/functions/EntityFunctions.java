package com.functions;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableEntityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.util.Optional;

/**
 * Azure Functions for entity e2e tests.
 * <p>
 * Tests:
 * <ul>
 *   <li>{@code callEntity} — orchestrator calls entity and returns result</li>
 *   <li>{@code signalEntity} — orchestrator signals entity (fire-and-forget), then calls to verify</li>
 *   <li>{@code signalAndCallEntity} — orchestrator signals entity to add, then calls to get updated value</li>
 * </ul>
 */
public class EntityFunctions {

    // ─── Entity trigger ───

    @FunctionName("Counter")
    public String counterEntity(
            @DurableEntityTrigger(name = "req") String req) {
        return EntityRunner.loadAndRun(req, CounterEntity::new);
    }

    // ─── Orchestrations ───

    /**
     * Orchestration that calls the counter entity "add" operation and returns the result.
     * Input: JSON with entityKey and addValue fields.
     */
    @FunctionName("CallEntityOrchestration")
    public int callEntityOrchestration(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        EntityPayload input = ctx.getInput(EntityPayload.class);
        EntityInstanceId entityId = new EntityInstanceId("counter", input.entityKey);
        return ctx.callEntity(entityId, "add", input.addValue, Integer.class).await();
    }

    /**
     * Orchestration that signals the counter entity to "add" then calls "get" to verify.
     * This tests both signalEntity (fire-and-forget) and callEntity (request-response).
     */
    @FunctionName("SignalThenCallEntityOrchestration")
    public int signalThenCallEntityOrchestration(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        EntityPayload input = ctx.getInput(EntityPayload.class);
        EntityInstanceId entityId = new EntityInstanceId("counter", input.entityKey);

        // Signal — fire-and-forget
        ctx.signalEntity(entityId, "add", input.addValue);

        // Call — request-response (should see the updated state)
        return ctx.callEntity(entityId, "get", null, Integer.class).await();
    }

    /**
     * Orchestration that calls "get" on a fresh counter entity (tests zero-initialized state).
     */
    @FunctionName("CallEntityGetOrchestration")
    public int callEntityGetOrchestration(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        String entityKey = ctx.getInput(String.class);
        EntityInstanceId entityId = new EntityInstanceId("counter", entityKey);
        return ctx.callEntity(entityId, "get", null, Integer.class).await();
    }

    // ─── HTTP triggers ───

    /**
     * POST /api/StartCallEntityOrchestration?key={key}&value={value}
     */
    @FunctionName("StartCallEntityOrchestration")
    public HttpResponseMessage startCallEntityOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        String key = request.getQueryParameters().getOrDefault("key", "e2e-call-" + System.currentTimeMillis());
        int value = Integer.parseInt(request.getQueryParameters().getOrDefault("value", "5"));

        DurableTaskClient client = durableContext.getClient();
        EntityPayload payload = new EntityPayload(key, value);
        String instanceId = client.scheduleNewOrchestrationInstance("CallEntityOrchestration", payload);
        context.getLogger().info("Started CallEntityOrchestration: " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * POST /api/StartSignalThenCallEntityOrchestration?key={key}&value={value}
     */
    @FunctionName("StartSignalThenCallEntityOrchestration")
    public HttpResponseMessage startSignalThenCallEntityOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        String key = request.getQueryParameters().getOrDefault("key", "e2e-signal-" + System.currentTimeMillis());
        int value = Integer.parseInt(request.getQueryParameters().getOrDefault("value", "10"));

        DurableTaskClient client = durableContext.getClient();
        EntityPayload payload = new EntityPayload(key, value);
        String instanceId = client.scheduleNewOrchestrationInstance("SignalThenCallEntityOrchestration", payload);
        context.getLogger().info("Started SignalThenCallEntityOrchestration: " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * POST /api/StartCallEntityGetOrchestration?key={key}
     */
    @FunctionName("StartCallEntityGetOrchestration")
    public HttpResponseMessage startCallEntityGetOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        String key = request.getQueryParameters().getOrDefault("key", "e2e-get-" + System.currentTimeMillis());

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("CallEntityGetOrchestration", key);
        context.getLogger().info("Started CallEntityGetOrchestration: " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    // ─── Helpers ───

    /**
     * Payload for entity orchestrations.
     */
    public static class EntityPayload {
        public String entityKey;
        public int addValue;

        public EntityPayload() {
        }

        public EntityPayload(String entityKey, int addValue) {
            this.entityKey = entityKey;
            this.addValue = addValue;
        }
    }
}
