// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.durabletask.AbstractTaskEntity;
import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.TaskEntityContext;
import com.microsoft.durabletask.TaskEntityOperation;

/**
 * Sensor entity that records temperature readings and forwards them to an aggregator entity.
 * <p>
 * Demonstrates <b>entity-to-entity signaling</b> via {@link TaskEntityContext#signalEntity}.
 * Each recorded reading is forwarded to an {@link AggregatorEntity} with the same key.
 * <p>
 * This mirrors the standalone {@code EntityCommunicationSample.SensorEntity}.
 */
public class SensorEntity extends AbstractTaskEntity<SensorState> {

    /**
     * Records a temperature reading and forwards it to the aggregator entity.
     * <p>
     * The {@link TaskEntityContext} parameter is automatically injected by the dispatch engine.
     */
    public void record(double temperature, TaskEntityContext ctx) {
        this.state.lastReading = temperature;
        this.state.totalReadings++;

        // Entity-to-entity signaling: forward the reading to the aggregator
        EntityInstanceId aggregatorId = new EntityInstanceId("Aggregator", ctx.getId().getKey());
        ctx.signalEntity(aggregatorId, "addReading", temperature);
    }

    @Override
    protected SensorState initializeState(TaskEntityOperation operation) {
        return new SensorState();
    }

    @Override
    protected Class<SensorState> getStateType() {
        return SensorState.class;
    }
}
