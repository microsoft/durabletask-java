// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;

public final class Helpers {
    final static Duration maxDuration = Duration.ofSeconds(Long.MAX_VALUE, 999999999L);

    public static @Nonnull <V> V throwIfArgumentNull(@Nullable V argValue, String argName) {
        if (argValue == null) {
            throw new IllegalArgumentException("The argument '" + argName + "' was null.");
        }

        return argValue;
    }

    public static @Nonnull String throwIfArgumentNullOrWhiteSpace(String argValue, String argName) {
        throwIfArgumentNull(argValue, argName);
        if (argValue.trim().length() == 0){
            throw new IllegalArgumentException("The argument '" + argName + "' was empty or contained only whitespace.");
        }

        return argValue;
    }

    public static void throwIfOrchestratorComplete(boolean isComplete) {
        if (isComplete) {
            throw new IllegalStateException("The orchestrator has already completed");
        }
    }

    public static boolean isInfiniteTimeout(Duration timeout) {
        return timeout == null || timeout.isNegative() || timeout.equals(maxDuration);
    }

    public static double powExact(double base, double exponent) throws ArithmeticException {
        if (base == 0.0) {
            return 0.0;
        }

        double result = Math.pow(base, exponent);

        if (result == Double.POSITIVE_INFINITY) {
            throw new ArithmeticException("Double overflow resulting in POSITIVE_INFINITY");
        } else if (result == Double.NEGATIVE_INFINITY) {
            throw new ArithmeticException("Double overflow resulting in NEGATIVE_INFINITY");
        } else if (Double.compare(-0.0f, result) == 0) {
            throw new ArithmeticException("Double overflow resulting in negative zero");
        } else if (Double.compare(+0.0f, result) == 0) {
            throw new ArithmeticException("Double overflow resulting in positive zero");
        }

        return result;
    }

    public static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    // Cannot be instantiated
    private Helpers() {
    }
}
