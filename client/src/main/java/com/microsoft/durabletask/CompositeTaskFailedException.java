// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.util.ArrayList;
import java.util.List;

/**
 * Exception that gets thrown when multiple {@link Task}s for an activity or sub-orchestration fails with an
 * unhandled exception.
 * <p>
 * Detailed information associated with each task failure can be retrieved using the {@link #getExceptions()}
 * method.
 */
public class CompositeTaskFailedException extends RuntimeException {
    private final List<Exception> exceptions;

    CompositeTaskFailedException() {
        this.exceptions = new ArrayList<>();
    }

    CompositeTaskFailedException(List<Exception> exceptions) {
        this.exceptions = exceptions;
    }

    CompositeTaskFailedException(String message, List<Exception> exceptions) {
        super(message);
        this.exceptions = exceptions;
    }

    CompositeTaskFailedException(String message, Throwable cause, List<Exception> exceptions) {
        super(message, cause);
        this.exceptions = exceptions;
    }

    CompositeTaskFailedException(Throwable cause, List<Exception> exceptions) {
        super(cause);
        this.exceptions = exceptions;
    }

    CompositeTaskFailedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, List<Exception> exceptions) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.exceptions = exceptions;
    }

    /**
     * Gets a list of exceptions that occurred during execution of a group of {@link Task}
     * These exceptions include details of the task failure and exception information
     *
     * @return a list of exceptions
     */
    public List<Exception> getExceptions() {
        return new ArrayList<>(this.exceptions);
    }

}