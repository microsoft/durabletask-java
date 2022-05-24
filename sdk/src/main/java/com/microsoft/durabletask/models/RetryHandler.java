// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.models;

@FunctionalInterface
public interface RetryHandler {
    boolean handle(RetryContext context);
}
