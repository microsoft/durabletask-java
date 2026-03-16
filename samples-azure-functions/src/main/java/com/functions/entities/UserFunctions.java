// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableEntityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.util.Optional;

/**
 * Azure Functions for the User entity sample.
 * <p>
 * Demonstrates:
 * <ul>
 *   <li>Complex entity state (UserState) with set/update operations</li>
 *   <li>Entity-triggered orchestrations via {@code TaskEntityContext.startNewOrchestration}</li>
 *   <li>Implicit "delete" operation from the base class</li>
 * </ul>
 * <p>
 * This mirrors the .NET {@code User.cs}, {@code UserApis}, and {@code Greeting.cs} from
 * {@code durabletask-dotnet/samples/AzureFunctionsApp/Entities/}.
 * <p>
 * See {@code users.http} for example HTTP requests.
 *
 * <p>APIs:
 * <ul>
 *   <li>Create User: PUT /api/users/{id}?name={name}&amp;age={age}</li>
 *   <li>Update User: PATCH /api/users/{id}?name={name}&amp;age={age}</li>
 *   <li>Get User: GET /api/users/{id}</li>
 *   <li>Delete User: DELETE /api/users/{id}</li>
 *   <li>Greet User: POST /api/users/{id}/greet?message={message}</li>
 * </ul>
 */
public class UserFunctions {

    // ─── Entity trigger function ───

    @FunctionName("User")
    public String userEntity(
            @DurableEntityTrigger(name = "req") String req) {
        return EntityRunner.loadAndRun(req, UserEntity::new);
    }

    // ─── Greeting orchestration & activity ───

    /**
     * Orchestration that greets a user. Triggered from within the User entity's
     * {@code greet} operation via {@code context.startNewOrchestration}.
     */
    @FunctionName("GreetingOrchestration")
    public UserEntity.GreetingInput greetingOrchestration(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        UserEntity.GreetingInput input = ctx.getInput(UserEntity.GreetingInput.class);
        return ctx.callActivity("GreetingActivity", input, UserEntity.GreetingInput.class).await();
    }

    /**
     * Activity that performs the greeting logic.
     */
    @FunctionName("GreetingActivity")
    public UserEntity.GreetingInput greetingActivity(
            @DurableActivityTrigger(name = "input") UserEntity.GreetingInput input,
            final ExecutionContext context) {
        String message = input.customMessage != null
                ? input.customMessage
                : String.format("Hello, %s! You are %d years old.", input.name, input.age);
        context.getLogger().info("Greeting: " + message);
        return input;
    }

    // ─── HTTP API functions ───

    /**
     * PUT /api/users/{id}?name={name}&amp;age={age}
     * <p>
     * Creates or overwrites a user entity. Both name and age are required.
     */
    @FunctionName("PutUser")
    public HttpResponseMessage putUser(
            @HttpTrigger(name = "req", methods = {HttpMethod.PUT},
                    route = "users/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id) {
        String name = request.getQueryParameters().get("name");
        String ageStr = request.getQueryParameters().get("age");

        if (name == null || ageStr == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Both name and age must be provided.")
                    .build();
        }

        int age;
        try {
            age = Integer.parseInt(ageStr);
        } catch (NumberFormatException e) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Age must be a valid integer.")
                    .build();
        }

        if (age < 0) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Age must be a positive integer.")
                    .build();
        }

        EntityInstanceId entityId = new EntityInstanceId("User", id);
        DurableTaskClient client = durableContext.getClient();
        client.getEntities().signalEntity(entityId, "set", new UserState(name, age));
        return request.createResponseBuilder(HttpStatus.ACCEPTED).build();
    }

    /**
     * PATCH /api/users/{id}?name={name}&amp;age={age}
     * <p>
     * Partially updates a user entity. Either name or age can be updated.
     */
    @FunctionName("PatchUser")
    public HttpResponseMessage patchUser(
            @HttpTrigger(name = "req", methods = {HttpMethod.PATCH},
                    route = "users/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id) {
        String name = request.getQueryParameters().get("name");
        String ageStr = request.getQueryParameters().get("age");

        Integer age = null;
        if (ageStr != null) {
            try {
                age = Integer.parseInt(ageStr);
            } catch (NumberFormatException e) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .body("Age must be a valid integer.")
                        .build();
            }
            if (age < 0) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .body("Age must be a positive integer.")
                        .build();
            }
        }

        EntityInstanceId entityId = new EntityInstanceId("User", id);
        DurableTaskClient client = durableContext.getClient();
        client.getEntities().signalEntity(entityId, "update", new UserUpdate(name, age));
        return request.createResponseBuilder(HttpStatus.ACCEPTED).build();
    }

    /**
     * GET /api/users/{id}
     * <p>
     * Gets the current state of the user entity.
     */
    @FunctionName("GetUser")
    public HttpResponseMessage getUser(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET},
                    route = "users/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id) {
        EntityInstanceId entityId = new EntityInstanceId("User", id);
        DurableTaskClient client = durableContext.getClient();

        TypedEntityMetadata<UserState> entity = client.getEntities()
                .getEntityMetadata(entityId, UserState.class);
        if (entity == null) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND).build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(entity.getState())
                .build();
    }

    /**
     * DELETE /api/users/{id}
     * <p>
     * Deletes the user entity using the implicit "delete" operation from {@code TaskEntity}.
     */
    @FunctionName("DeleteUser")
    public HttpResponseMessage deleteUser(
            @HttpTrigger(name = "req", methods = {HttpMethod.DELETE},
                    route = "users/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id) {
        EntityInstanceId entityId = new EntityInstanceId("User", id);
        DurableTaskClient client = durableContext.getClient();
        // Even though UserEntity does not have a 'delete' method, the base class TaskEntity handles it
        client.getEntities().signalEntity(entityId, "delete");
        return request.createResponseBuilder(HttpStatus.ACCEPTED).build();
    }

    /**
     * POST /api/users/{id}/greet?message={message}
     * <p>
     * Signals the user entity to initiate a greeting orchestration.
     */
    @FunctionName("GreetUser")
    public HttpResponseMessage greetUser(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    route = "users/{id}/greet",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id) {
        EntityInstanceId entityId = new EntityInstanceId("User", id);
        DurableTaskClient client = durableContext.getClient();

        String message = request.getQueryParameters().get("message");
        client.getEntities().signalEntity(entityId, "greet", message);
        return request.createResponseBuilder(HttpStatus.ACCEPTED).build();
    }
}
