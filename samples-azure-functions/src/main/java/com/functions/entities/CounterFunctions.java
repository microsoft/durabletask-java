// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableEntityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.util.Optional;

/**
 * Azure Functions for the Counter entity sample.
 * <p>
 * Demonstrates three entity dispatch modes:
 * <ul>
 *   <li>{@code mode=entity} (default) — dispatches to {@link CounterEntity} ({@code TaskEntity<Integer>})</li>
 *   <li>{@code mode=state} — dispatches to {@link StateCounterEntity} (POJO state dispatch)</li>
 *   <li>{@code mode=manual} — dispatches to {@link ManualCounterEntity} ({@code ITaskEntity})</li>
 * </ul>
 * <p>
 * This mirrors the .NET {@code Counter.cs} and {@code CounterApis} from
 * {@code durabletask-dotnet/samples/AzureFunctionsApp/Entities/}.
 * <p>
 * See {@code counters.http} for example HTTP requests.
 */
public class CounterFunctions {

    // ─── Entity trigger functions ───

    /**
     * Entity function for the class-based counter ({@link CounterEntity}).
     */
    @FunctionName("Counter")
    public String counterEntity(
            @DurableEntityTrigger(name = "req") String req) {
        return EntityRunner.loadAndRun(req, CounterEntity::new);
    }

    /**
     * Entity function for the state-dispatch counter ({@link StateCounterEntity}).
     */
    @FunctionName("Counter_State")
    public String counterStateEntity(
            @DurableEntityTrigger(name = "req") String req) {
        return EntityRunner.loadAndRun(req, StateCounterEntity::new);
    }

    /**
     * Entity function for the manual (low-level) counter ({@link ManualCounterEntity}).
     */
    @FunctionName("Counter_Manual")
    public String counterManualEntity(
            @DurableEntityTrigger(name = "req") String req) {
        return EntityRunner.loadAndRun(req, ManualCounterEntity::new);
    }

    // ─── Orchestration ───

    /**
     * Orchestration that calls the counter entity to add a value and return the result.
     */
    @FunctionName("CounterOrchestration")
    public int counterOrchestration(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        CounterPayload input = ctx.getInput(CounterPayload.class);
        return ctx.callEntity(input.entityId, "add", input.addValue, Integer.class).await();
    }

    // ─── HTTP API functions ───

    /**
     * POST /api/counters/{id}/add/{value}?mode={mode}
     * <p>
     * Starts an orchestration that calls the counter entity to add a value.
     */
    @FunctionName("Counter_Add")
    public HttpResponseMessage counterAdd(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    route = "counters/{id}/add/{value}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id,
            @BindingName("value") int value,
            final ExecutionContext context) {
        EntityInstanceId entityId = getEntityId(request, id);
        DurableTaskClient client = durableContext.getClient();
        CounterPayload payload = new CounterPayload(entityId, value);
        String instanceId = client.scheduleNewOrchestrationInstance("CounterOrchestration", payload);
        context.getLogger().info("Started CounterOrchestration: " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * GET /api/counters/{id}?mode={mode}
     * <p>
     * Gets the current state of the counter entity.
     */
    @FunctionName("Counter_Get")
    public HttpResponseMessage counterGet(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET},
                    route = "counters/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id) {
        EntityInstanceId entityId = getEntityId(request, id);
        DurableTaskClient client = durableContext.getClient();

        TypedEntityMetadata<Integer> entity = client.getEntities().getEntityMetadata(entityId, Integer.class);
        if (entity == null) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND).build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(entity)
                .build();
    }

    /**
     * DELETE /api/counters/{id}?mode={mode}
     * <p>
     * Deletes the counter entity using the built-in implicit "delete" operation.
     */
    @FunctionName("Counter_Delete")
    public HttpResponseMessage counterDelete(
            @HttpTrigger(name = "req", methods = {HttpMethod.DELETE},
                    route = "counters/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id) {
        EntityInstanceId entityId = getEntityId(request, id);
        DurableTaskClient client = durableContext.getClient();
        client.getEntities().signalEntity(entityId, "delete");
        return request.createResponseBuilder(HttpStatus.ACCEPTED).build();
    }

    /**
     * POST /api/counters/{id}/reset?mode={mode}
     * <p>
     * Signals the counter entity to reset to zero.
     */
    @FunctionName("Counter_Reset")
    public HttpResponseMessage counterReset(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    route = "counters/{id}/reset",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id) {
        EntityInstanceId entityId = getEntityId(request, id);
        DurableTaskClient client = durableContext.getClient();
        client.getEntities().signalEntity(entityId, "reset");
        return request.createResponseBuilder(HttpStatus.ACCEPTED).build();
    }

    // ─── Helpers ───

    /**
     * Resolves the entity name based on the {@code mode} query parameter.
     */
    private static EntityInstanceId getEntityId(HttpRequestMessage<?> request, String key) {
        String mode = request.getQueryParameters().get("mode");
        String name;
        if (mode == null) {
            name = "counter";
        } else {
            switch (mode.toLowerCase()) {
                case "1":
                case "state":
                    name = "counter_state";
                    break;
                case "2":
                case "manual":
                    name = "counter_manual";
                    break;
                case "0":
                case "entity":
                default:
                    name = "counter";
                    break;
            }
        }
        return new EntityInstanceId(name, key);
    }

    /**
     * Payload for the CounterOrchestration.
     */
    public static class CounterPayload {
        public EntityInstanceId entityId;
        public int addValue;

        public CounterPayload() {
        }

        public CounterPayload(EntityInstanceId entityId, int addValue) {
            this.entityId = entityId;
            this.addValue = addValue;
        }
    }
}
