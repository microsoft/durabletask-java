// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.util.HashMap;
import java.util.logging.Logger;

public class TaskActivityExecutor {
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

    public String execute(String activityName, String input) {
        TaskActivityFactory factory = this.activityFactories.get(activityName);
        // TODO: Throw if the factory is null (activity by that name doesn't exist)
        TaskActivity activity = factory.create();

        TaskActivityContextImpl context = new TaskActivityContextImpl(activityName, input);

        // TODO: What is the story for async activity functions?
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
