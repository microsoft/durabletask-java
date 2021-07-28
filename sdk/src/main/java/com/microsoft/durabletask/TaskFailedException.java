// Copyright 2021 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.microsoft.durabletask;

public class TaskFailedException extends Exception {
    private final String taskName;
    private final int taskId;

    protected TaskFailedException(String message, String taskName, int taskId) {
        super(message);
        this.taskId = taskId;
        this.taskName = taskName;
    }

    public int getTaskId() {
        return this.taskId;
    }

    public String getTaskName() {
        return this.taskName;
    }

    static TaskFailedException forTaskActivity(String name, int taskId, String reason) {
        String message = String.format("Activity '%s' with task ID %d failed: %s", name, taskId, reason);
        return new TaskFailedException(message, name, taskId);
    }
}
