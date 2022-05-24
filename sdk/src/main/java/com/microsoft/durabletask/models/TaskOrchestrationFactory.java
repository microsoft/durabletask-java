// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.models;

public interface TaskOrchestrationFactory {
    String getName();
    TaskOrchestration create();
}
