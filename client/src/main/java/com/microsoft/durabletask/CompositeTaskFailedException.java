package com.microsoft.durabletask;

import java.util.ArrayList;
import java.util.List;

public class CompositeTaskFailedException extends Exception{
    private final List<Exception> exceptions;

    public CompositeTaskFailedException() {
        this.exceptions = new ArrayList<>();
    }

    public CompositeTaskFailedException(List<Exception> exceptions) {
        this.exceptions = exceptions;
    }

    public CompositeTaskFailedException(String message, List<Exception> exceptions) {
        super(message);
        this.exceptions = exceptions;
    }

    public CompositeTaskFailedException(String message, Throwable cause, List<Exception> exceptions) {
        super(message, cause);
        this.exceptions = exceptions;
    }

    public CompositeTaskFailedException(Throwable cause, List<Exception> exceptions) {
        super(cause);
        this.exceptions = exceptions;
    }

    public CompositeTaskFailedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, List<Exception> exceptions) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.exceptions = exceptions;
    }

    public List<Exception> getExceptions() {
        return new ArrayList<>(this.exceptions);
    }

}