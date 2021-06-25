// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

class OrchestratorYieldEvent extends Throwable {
    OrchestratorYieldEvent(String message) {
        super(message);
    }
}
