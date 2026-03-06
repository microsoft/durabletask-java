// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableEntityTrigger;

import java.util.Optional;

/**
 * Azure Functions for the Lifetime entity sample.
 * <p>
 * Demonstrates entity lifecycle: initialization via {@code initializeState},
 * custom deletion (nulling state), and the implicit "delete" operation.
 * <p>
 * This mirrors the .NET {@code Lifetime.cs} and {@code LifetimeApis} from
 * {@code durabletask-dotnet/samples/AzureFunctionsApp/Entities/}.
 * <p>
 * See {@code lifetimes.http} for example HTTP requests.
 */
public class LifetimeFunctions {

    // ─── Entity trigger function ───

    @FunctionName("Lifetime")
    public String lifetimeEntity(
            @DurableEntityTrigger(name = "req") String req) {
        return EntityRunner.loadAndRun(req, LifetimeEntity::new);
    }

    // ─── HTTP API functions ───

    /**
     * GET /api/lifetimes/{id}
     * <p>
     * Gets the current state of the lifetime entity.
     */
    @FunctionName("Lifetime_Get")
    public HttpResponseMessage lifetimeGet(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET},
                    route = "lifetimes/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id,
            final ExecutionContext context) {
        EntityInstanceId entityId = new EntityInstanceId("Lifetime", id);
        DurableTaskClient client = durableContext.getClient();

        TypedEntityMetadata<LifetimeState> entity = client.getEntities()
                .getEntityMetadata(entityId, LifetimeState.class);
        if (entity == null) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND).build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(entity)
                .build();
    }

    /**
     * PUT /api/lifetimes/{id}
     * <p>
     * Initializes the lifetime entity by sending an "init" signal.
     */
    @FunctionName("Lifetime_Init")
    public HttpResponseMessage lifetimeInit(
            @HttpTrigger(name = "req", methods = {HttpMethod.PUT},
                    route = "lifetimes/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id,
            final ExecutionContext context) {
        EntityInstanceId entityId = new EntityInstanceId("Lifetime", id);
        DurableTaskClient client = durableContext.getClient();
        client.getEntities().signalEntity(entityId, "init");
        return request.createResponseBuilder(HttpStatus.ACCEPTED).build();
    }

    /**
     * DELETE /api/lifetimes/{id}?custom={true|false}
     * <p>
     * Deletes the lifetime entity. If {@code custom=true}, uses the {@code customDelete}
     * operation; otherwise uses the standard {@code delete} operation.
     */
    @FunctionName("Lifetime_Delete")
    public HttpResponseMessage lifetimeDelete(
            @HttpTrigger(name = "req", methods = {HttpMethod.DELETE},
                    route = "lifetimes/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id,
            final ExecutionContext context) {
        EntityInstanceId entityId = new EntityInstanceId("Lifetime", id);
        DurableTaskClient client = durableContext.getClient();

        String customParam = request.getQueryParameters().get("custom");
        boolean useCustomDelete = "true".equalsIgnoreCase(customParam);
        String operation = useCustomDelete ? "customDelete" : "delete";

        client.getEntities().signalEntity(entityId, operation);
        return request.createResponseBuilder(HttpStatus.ACCEPTED).build();
    }
}
