// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

public class OrchestratorBlockedEvent extends Throwable {
    OrchestratorBlockedEvent(String message) {
        super(message);
    }
}
