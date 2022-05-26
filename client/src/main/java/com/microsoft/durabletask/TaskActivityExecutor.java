// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.util.HashMap;
import java.util.logging.Logger;

final class TaskActivityExecutor {
    private final HashMap<String, TaskActivityFactory> activityFactories;
    private final DataConverter dataConverter;
    private final Logger logger;

    public TaskActivityExecutor(
            HashMap<String, TaskActivityFactory> activityFactories,
            DataConverter dataConverter,
            Logger logger) {
        this.activityFactories = activityFactories;
        this.dataConverter = dataConverter;
        this.logger = logger;
    }

    public String execute(String taskName, String input, int taskId) throws Throwable {
        TaskActivityFactory factory = this.activityFactories.get(taskName);
        if (factory == null) {
            throw new IllegalStateException(
                    String.format("No activity task named '%s' is registered.", taskName));
        }
        
        TaskActivity activity = factory.create();
        if (activity == null) {
            throw new IllegalStateException(
                    String.format("The task factory '%s' returned a null TaskActivity object.", taskName));
        }

        TaskActivityContextImpl context = new TaskActivityContextImpl(taskName, input);

        // Unhandled exceptions are allowed to escape
        Object output = activity.run(context);
        if (output != null) {
            return this.dataConverter.serialize(output);
        }

        return null;
    }

    private class TaskActivityContextImpl implements TaskActivityContext {
        private final String name;
        private final String rawInput;

        private final DataConverter dataConverter = TaskActivityExecutor.this.dataConverter;

        public TaskActivityContextImpl(String activityName, String rawInput) {
            this.name = activityName;
            this.rawInput = rawInput;
        }

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public <T> T getInput(Class<T> targetType) {
            if (this.rawInput == null) {
                return null;
            }

            return this.dataConverter.deserialize(this.rawInput, targetType);
        }
    }
}
