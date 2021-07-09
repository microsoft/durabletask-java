// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

public class OrchestratorYieldEvent extends Throwable {
    OrchestratorYieldEvent(String message) {
        super(message);
    }
}
