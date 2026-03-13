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

    /**
     * Comprehensive orchestration that exercises signal, call, and reset on a counter entity.
     * Steps: signal add(5) -> call get (expect 5) -> signal add(10) -> call get (expect 15)
     *        -> signal reset -> call get (expect 0).
     * Returns a summary string with pass/fail.
     */
    @FunctionName("ComprehensiveEntityOrchestration")
    public String comprehensiveEntityOrchestration(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        String entityKey = ctx.getInput(String.class);
        EntityInstanceId counterId = new EntityInstanceId("counter", entityKey);
        StringBuilder result = new StringBuilder();

        // Test 1: signalEntity (fire-and-forget) — add 5
        ctx.signalEntity(counterId, "add", 5);
        result.append("Step 1: Signaled add(5)\n");

        // Test 2: callEntity (request-response) — get current value, should be 5
        int valueAfterAdd5 = ctx.callEntity(counterId, "get", null, Integer.class).await();
        result.append("Step 2: callEntity get() returned ").append(valueAfterAdd5).append("\n");

        // Test 3: signalEntity — add 10
        ctx.signalEntity(counterId, "add", 10);
        result.append("Step 3: Signaled add(10)\n");

        // Test 4: callEntity — get current value, should be 15
        int valueAfterAdd10 = ctx.callEntity(counterId, "get", null, Integer.class).await();
        result.append("Step 4: callEntity get() returned ").append(valueAfterAdd10).append("\n");

        // Test 5: signalEntity — reset
        ctx.signalEntity(counterId, "reset");
        result.append("Step 5: Signaled reset()\n");

        // Test 6: callEntity — get current value, should be 0
        int valueAfterReset = ctx.callEntity(counterId, "get", null, Integer.class).await();
        result.append("Step 6: callEntity get() returned ").append(valueAfterReset).append("\n");

        // Summary
        boolean passed = (valueAfterAdd5 == 5) && (valueAfterAdd10 == 15) && (valueAfterReset == 0);
        result.append("\nAll tests passed: ").append(passed);
        return result.toString();
    }

    /**
     * Orchestration that calls "add" twice with different values to produce a result
     * that differs from either individual input (proving the entity accumulates state).
     * Input: JSON with entityKey and addValue fields. Calls add(addValue) then add(addValue + 2).
     * Returns the final result.
     */
    @FunctionName("CallEntityTwiceOrchestration")
    public int callEntityTwiceOrchestration(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        EntityPayload input = ctx.getInput(EntityPayload.class);
        EntityInstanceId entityId = new EntityInstanceId("counter", input.entityKey);
        // First add
        ctx.callEntity(entityId, "add", input.addValue, Integer.class).await();
        // Second add with a different value
        int secondValue = input.addValue + 2;
        return ctx.callEntity(entityId, "add", secondValue, Integer.class).await();
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

    /**
     * POST /api/StartComprehensiveEntityOrchestration?key={key}
     */
    @FunctionName("StartComprehensiveEntityOrchestration")
    public HttpResponseMessage startComprehensiveEntityOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        String key = request.getQueryParameters().getOrDefault("key", "e2e-comprehensive-" + System.currentTimeMillis());

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("ComprehensiveEntityOrchestration", key);
        context.getLogger().info("Started ComprehensiveEntityOrchestration: " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * POST /api/StartCallEntityTwiceOrchestration?key={key}&value={value}
     */
    @FunctionName("StartCallEntityTwiceOrchestration")
    public HttpResponseMessage startCallEntityTwiceOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        String key = request.getQueryParameters().getOrDefault("key", "e2e-twice-" + System.currentTimeMillis());
        int value = Integer.parseInt(request.getQueryParameters().getOrDefault("value", "3"));

        DurableTaskClient client = durableContext.getClient();
        EntityPayload payload = new EntityPayload(key, value);
        String instanceId = client.scheduleNewOrchestrationInstance("CallEntityTwiceOrchestration", payload);
        context.getLogger().info("Started CallEntityTwiceOrchestration: " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * GET /api/GetEntityState?name={name}&key={key}
     * Returns the entity's current state as a JSON integer.
     */
    @FunctionName("GetEntityState")
    public HttpResponseMessage getEntityState(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET},
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        String name = request.getQueryParameters().get("name");
        String key = request.getQueryParameters().get("key");
        if (name == null || key == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Missing 'name' or 'key' query parameter")
                    .build();
        }

        EntityInstanceId entityId = new EntityInstanceId(name, key);
        EntityMetadata metadata = durableContext.getEntityMetadata(entityId, true);
        if (metadata == null) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .body("Entity not found: " + name + "/" + key)
                    .build();
        }

        String serializedState = metadata.getSerializedState();
        context.getLogger().info("Entity " + name + "/" + key + " state: " + serializedState);
        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(serializedState)
                .build();
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
