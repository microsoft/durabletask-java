// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.TaskEntityFactory;
import com.microsoft.durabletask.TaskOrchestrationFactory;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Unit tests for the scheduled-tasks worker and client registration extensions. */
class ScheduledTaskExtensionsTest {

    @Test
    @SuppressWarnings("unchecked")
    void useScheduledTasksRegistersBuiltInsAndAdvertisesCapability() throws Exception {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();

        DurableTaskGrpcWorkerBuilder returned = ScheduledTaskWorkerExtensions.useScheduledTasks(builder);
        assertSame(builder, returned);

        Map<String, TaskEntityFactory> entities = (Map<String, TaskEntityFactory>)
                readField(builder, "entityFactories");
        assertTrue(entities.containsKey(Schedule.NAME.toLowerCase(Locale.ROOT)));
        assertNotNull(entities.get(Schedule.NAME.toLowerCase(Locale.ROOT)).create());

        Map<String, TaskOrchestrationFactory> orchestrations = (Map<String, TaskOrchestrationFactory>)
                readField(builder, "orchestrationFactories");
        assertTrue(orchestrations.containsKey(ExecuteScheduleOperationOrchestrator.NAME));

        boolean supportsScheduledTasks = (boolean) readField(builder, "supportsScheduledTasks");
        assertTrue(supportsScheduledTasks);
    }

    @Test
    void clientExtensionReturnsBoundClient() {
        ScheduledTaskClient client = ScheduledTaskClientExtensions.useScheduledTasks(mock(DurableTaskClient.class));
        assertNotNull(client);
    }

    private static Object readField(Object target, String name) throws Exception {
        Field field = DurableTaskGrpcWorkerBuilder.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
