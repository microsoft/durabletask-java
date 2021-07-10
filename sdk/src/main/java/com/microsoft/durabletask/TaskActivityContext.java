// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

public interface TaskActivityContext {
    String getName();
    <T> T getInput(Class<T> targetType);
}
