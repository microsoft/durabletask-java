// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

final class NonDeterministicOrchestratorException extends RuntimeException {
    public NonDeterministicOrchestratorException(String message) {
        super(message);
    }
}
