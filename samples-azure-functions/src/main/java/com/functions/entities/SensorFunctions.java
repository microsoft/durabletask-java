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
 * Azure Functions for the Sensor/Aggregator entity communication sample.
 * <p>
 * Demonstrates two advanced entity communication patterns:
 * <ul>
 *   <li><b>Entity-to-entity signaling</b>: The {@link SensorEntity} uses
 *       {@link TaskEntityContext#signalEntity} to forward readings to the
 *       {@link AggregatorEntity}.</li>
 *   <li><b>Entity starting an orchestration</b>: The {@link AggregatorEntity} uses
 *       {@link TaskEntityContext#startNewOrchestration} to kick off a
 *       {@code TemperatureAlert} orchestration when a reading exceeds a threshold.</li>
 * </ul>
 * <p>
 * This mirrors the standalone {@code EntityCommunicationSample} adapted for Azure Functions.
 * <p>
 * See {@code sensors.http} for example HTTP requests.
 *
 * <p>APIs:
 * <ul>
 *   <li>Record reading: POST /api/sensors/{id}/record/{temperature}</li>
 *   <li>Get sensor state: GET /api/sensors/{id}</li>
 *   <li>Get aggregator state: GET /api/sensors/{id}/aggregator</li>
 * </ul>
 */
public class SensorFunctions {

    // ─── Entity trigger functions ───

    @FunctionName("Sensor")
    public String sensorEntity(
            @DurableEntityTrigger(name = "req") String req) {
        return EntityRunner.loadAndRun(req, SensorEntity::new);
    }

    @FunctionName("Aggregator")
    public String aggregatorEntity(
            @DurableEntityTrigger(name = "req") String req) {
        return EntityRunner.loadAndRun(req, AggregatorEntity::new);
    }

    // ─── Alert orchestration & activity ───

    /**
     * Orchestration triggered by the {@link AggregatorEntity} when a temperature reading
     * exceeds the threshold. Calls an activity to handle the alert.
     */
    @FunctionName("TemperatureAlert")
    public String temperatureAlert(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        double temperature = ctx.getInput(Double.class);
        return ctx.callActivity("SendAlert", temperature, String.class).await();
    }

    /**
     * Activity that sends a temperature alert.
     */
    @FunctionName("SendAlert")
    public String sendAlert(
            @DurableActivityTrigger(name = "temperature") double temperature,
            final ExecutionContext context) {
        String message = String.format(
                "ALERT: Temperature %.1f°C exceeds threshold!", temperature);
        context.getLogger().warning(message);
        return message;
    }

    // ─── HTTP API functions ───

    /**
     * POST /api/sensors/{id}/record/{temperature}
     * <p>
     * Signals the sensor entity to record a temperature reading.
     * The sensor entity will forward the reading to the aggregator entity (entity-to-entity signaling).
     */
    @FunctionName("Sensor_Record")
    public HttpResponseMessage recordReading(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    route = "sensors/{id}/record/{temperature}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id,
            @BindingName("temperature") double temperature) {
        EntityInstanceId entityId = new EntityInstanceId("Sensor", id);
        DurableTaskClient client = durableContext.getClient();
        client.getEntities().signalEntity(entityId, "record", temperature);
        return request.createResponseBuilder(HttpStatus.ACCEPTED).build();
    }

    /**
     * GET /api/sensors/{id}
     * <p>
     * Gets the current state of the sensor entity.
     */
    @FunctionName("Sensor_Get")
    public HttpResponseMessage getSensor(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET},
                    route = "sensors/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id) {
        EntityInstanceId entityId = new EntityInstanceId("Sensor", id);
        DurableTaskClient client = durableContext.getClient();

        TypedEntityMetadata<SensorState> entity = client.getEntities()
                .getEntityMetadata(entityId, SensorState.class);
        if (entity == null) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND).build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(entity.getState())
                .build();
    }

    /**
     * GET /api/sensors/{id}/aggregator
     * <p>
     * Gets the current state of the aggregator entity associated with the sensor.
     */
    @FunctionName("Aggregator_Get")
    public HttpResponseMessage getAggregator(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET},
                    route = "sensors/{id}/aggregator",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id) {
        EntityInstanceId entityId = new EntityInstanceId("Aggregator", id);
        DurableTaskClient client = durableContext.getClient();

        TypedEntityMetadata<AggregatorState> entity = client.getEntities()
                .getEntityMetadata(entityId, AggregatorState.class);
        if (entity == null) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND).build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(entity.getState())
                .build();
    }
}
