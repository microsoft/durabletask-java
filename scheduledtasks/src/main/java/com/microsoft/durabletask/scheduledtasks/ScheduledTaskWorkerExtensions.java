// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationFactory;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Worker-side registration for the scheduled-tasks feature.
 * <p>
 * Registers the built-in {@link Schedule} entity and the {@link ExecuteScheduleOperationOrchestrator} operation
 * orchestrator, and advertises the scheduled-tasks worker capability. Schedules are then managed from the client via
 * {@link ScheduledTaskClient}.
 */
public final class ScheduledTaskWorkerExtensions {

    private ScheduledTaskWorkerExtensions() {
    }

    /**
     * Enables scheduled tasks on the given worker builder. Call this before building or starting the worker.
     *
     * @param builder the worker builder to configure
     * @return the worker builder, for chaining
     */
    public static DurableTaskGrpcWorkerBuilder useScheduledTasks(DurableTaskGrpcWorkerBuilder builder) {
        Objects.requireNonNull(builder, "builder must not be null");

        builder.addEntity(Schedule.NAME, Schedule::new);
        builder.addOrchestration(orchestrationFactory(
                ExecuteScheduleOperationOrchestrator.NAME, ExecuteScheduleOperationOrchestrator::new));
        builder.setSupportsScheduledTasks(true);

        return builder;
    }

    private static TaskOrchestrationFactory orchestrationFactory(String name, Supplier<TaskOrchestration> supplier) {
        return new TaskOrchestrationFactory() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public TaskOrchestration create() {
                return supplier.get();
            }
        };
    }
}
