// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.durabletask.AbstractTaskEntity;
import com.microsoft.durabletask.TaskEntityContext;
import com.microsoft.durabletask.TaskEntityOperation;

/**
 * Aggregator entity that computes running averages and starts an alert orchestration
 * when a reading exceeds a threshold.
 * <p>
 * Demonstrates <b>entity starting an orchestration</b> via
 * {@link TaskEntityContext#startNewOrchestration}.
 * <p>
 * This mirrors the standalone {@code EntityCommunicationSample.AggregatorEntity}.
 */
public class AggregatorEntity extends AbstractTaskEntity<AggregatorState> {
    private static final double ALERT_THRESHOLD = 30.0;

    /**
     * Adds a reading to the running total. If the reading exceeds the threshold,
     * starts a {@code TemperatureAlert} orchestration.
     * <p>
     * The {@link TaskEntityContext} parameter is automatically injected by the dispatch engine.
     */
    public void addReading(double reading, TaskEntityContext ctx) {
        this.state.sum += reading;
        this.state.count++;

        if (reading > ALERT_THRESHOLD) {
            // Entity starting an orchestration when a threshold is breached
            ctx.startNewOrchestration("TemperatureAlert", reading);
        }
    }

    /**
     * Returns the current aggregator state.
     */
    public AggregatorState get() {
        return this.state;
    }

    @Override
    protected AggregatorState initializeState(TaskEntityOperation operation) {
        return new AggregatorState();
    }

    @Override
    protected Class<AggregatorState> getStateType() {
        return AggregatorState.class;
    }
}
