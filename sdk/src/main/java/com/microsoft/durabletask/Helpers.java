// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.time.Duration;

final class Helpers {
    final static Duration maxDuration = Duration.ofSeconds(Long.MAX_VALUE, 999999999L);

    static void throwIfArgumentNull(Object argValue, String argName) {
        if (argValue == null) {
            throw new IllegalArgumentException("The argument '" + argName + "' was null.");
        }
    }

    static void throwIfArgumentNullOrWhiteSpace(String argValue, String argName) {
        throwIfArgumentNull(argValue, argName);
        if (argValue.trim().length() == 0){
            throw new IllegalArgumentException("The argument '" + argName + "' was empty or contained only whitespace.");
        }
    }

    static void throwIfOrchestratorComplete(boolean isComplete) {
        if (isComplete) {
            throw new IllegalStateException("The orchestrator has already completed");
        }
    }

    static boolean isInfiniteTimeout(Duration timeout) {
        return timeout == null || timeout.isNegative() || timeout.equals(maxDuration);
    }

    // Cannot be instantiated
    private Helpers() {
    }
}
