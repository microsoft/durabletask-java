// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Sample demonstrating entity-to-entity signaling and entity-started orchestrations.
 * <p>
 * This sample shows two advanced entity communication patterns:
 * <ul>
 *   <li><b>Entity-to-entity signaling</b>: An entity uses {@link TaskEntityContext#signalEntity}
 *       to send a fire-and-forget message to another entity.</li>
 *   <li><b>Entity starting an orchestration</b>: An entity uses
 *       {@link TaskEntityContext#startNewOrchestration} to kick off an orchestration when
 *       a threshold condition is met.</li>
 * </ul>
 * <p>
 * Scenario: A sensor entity receives temperature readings. Each reading is forwarded to an
 * aggregator entity that tracks the average. When the aggregator detects a reading above a
 * threshold, it starts an alert orchestration.
 * <p>
 * Usage: Run this sample with a Durable Task sidecar listening on localhost:4001.
 * <pre>
 *   docker run -d -p 4001:4001 durabletask-sidecar
 *   ./gradlew :samples:run -PmainClass=io.durabletask.samples.EntityCommunicationSample
 * </pre>
 */
final class EntityCommunicationSample {

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        DurableTaskGrpcWorker worker = new DurableTaskGrpcWorkerBuilder()
                .addEntity("Sensor", SensorEntity::new)
                .addEntity("Aggregator", AggregatorEntity::new)
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "TemperatureAlert"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            double temperature = ctx.getInput(Double.class);
                            String message = String.format(
                                    "ALERT: Temperature %.1f°C exceeds threshold!", temperature);
                            ctx.complete(message);
                        };
                    }
                })
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "SendReadings"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            EntityInstanceId sensorId = new EntityInstanceId("Sensor", "sensor-1");

                            // Send several temperature readings to the sensor
                            double[] readings = { 22.5, 24.0, 26.5, 35.0, 23.0 };
                            for (double reading : readings) {
                                ctx.signalEntity(sensorId, "record", reading);
                            }

                            ctx.complete("Sent " + readings.length + " readings");
                        };
                    }
                })
                .build();

        worker.start();
        System.out.println("Worker started. Sensor and Aggregator entities registered.");

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();

        // Send readings through an orchestration
        String instanceId = client.scheduleNewOrchestrationInstance("SendReadings");
        OrchestrationMetadata result = client.waitForInstanceCompletion(
                instanceId, Duration.ofSeconds(30), true);
        System.out.printf("Orchestration result: %s%n", result.readOutputAs(String.class));

        // Wait for entity processing and potential alert orchestration
        Thread.sleep(5000);

        // Check final entity states
        EntityMetadata sensor = client.getEntityMetadata(
                new EntityInstanceId("Sensor", "sensor-1"), true);
        if (sensor != null) {
            System.out.printf("Sensor state: %s%n", sensor.readStateAs(SensorState.class));
        }

        EntityMetadata aggregator = client.getEntityMetadata(
                new EntityInstanceId("Aggregator", "sensor-1"), true);
        if (aggregator != null) {
            AggregatorState aggState = aggregator.readStateAs(AggregatorState.class);
            System.out.printf("Aggregator state: count=%d, sum=%.1f, avg=%.1f%n",
                    aggState.count, aggState.sum, aggState.getAverage());
        }

        worker.stop();
    }

    // ---- State classes ----

    /**
     * State for the sensor entity — tracks the last recorded temperature.
     */
    public static class SensorState {
        public double lastReading;
        public int totalReadings;

        @Override
        public String toString() {
            return String.format("SensorState{lastReading=%.1f, totalReadings=%d}", lastReading, totalReadings);
        }
    }

    /**
     * State for the aggregator entity — tracks sum and count for computing averages.
     */
    public static class AggregatorState {
        public double sum;
        public int count;

        public double getAverage() {
            return count > 0 ? sum / count : 0;
        }
    }

    // ---- Entity implementations ----

    /**
     * Sensor entity that records temperature readings and forwards them to an aggregator.
     * <p>
     * Demonstrates entity-to-entity signaling via {@link TaskEntityContext#signalEntity}.
     */
    public static class SensorEntity extends TaskEntity<SensorState> {

        /**
         * Records a temperature reading and forwards it to the aggregator entity.
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

    /**
     * Aggregator entity that computes running averages and starts an alert orchestration
     * when a reading exceeds a threshold.
     * <p>
     * Demonstrates entity starting an orchestration via {@link TaskEntityContext#startNewOrchestration}.
     */
    public static class AggregatorEntity extends TaskEntity<AggregatorState> {
        private static final double ALERT_THRESHOLD = 30.0;

        /**
         * Adds a reading to the running total. If the reading exceeds the threshold,
         * starts an alert orchestration.
         */
        public void addReading(double reading, TaskEntityContext ctx) {
            this.state.sum += reading;
            this.state.count++;

            if (reading > ALERT_THRESHOLD) {
                // Entity starting an orchestration when a threshold is breached
                String orchestrationId = ctx.startNewOrchestration("TemperatureAlert", reading);
                System.out.printf("Aggregator started alert orchestration: %s (reading=%.1f)%n",
                        orchestrationId, reading);
            }
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
}
