// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.NullValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.Value;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.TaskFailureDetails;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class that represents the details of a task failure.
 * <p>
 * In most cases, failures are caused by unhandled exceptions in activity or orchestrator code, in which case instances
 * of this class will expose the details of the exception. However, it's also possible that other types of errors could
 * result in task failures, in which case there may not be any exception-specific information.
 */
public final class FailureDetails {
    private final String errorType;
    private final String errorMessage;
    private final String stackTrace;
    private final boolean isNonRetriable;
    private final FailureDetails innerFailure;
    private final Map<String, Object> properties;

    FailureDetails(
            String errorType,
            @Nullable String errorMessage,
            @Nullable String errorDetails,
            boolean isNonRetriable) {
        this(errorType, errorMessage, errorDetails, isNonRetriable, null, null);
    }

    FailureDetails(
            String errorType,
            @Nullable String errorMessage,
            @Nullable String errorDetails,
            boolean isNonRetriable,
            @Nullable FailureDetails innerFailure,
            @Nullable Map<String, Object> properties) {
        this.errorType = errorType;
        this.stackTrace = errorDetails;

        // Error message can be null for things like NullPointerException but the gRPC contract doesn't allow null
        this.errorMessage = errorMessage != null ? errorMessage : "";
        this.isNonRetriable = isNonRetriable;
        this.innerFailure = innerFailure;
        this.properties = properties != null ? Collections.unmodifiableMap(new HashMap<>(properties)) : null;
    }

    FailureDetails(Exception exception) {
        this(exception.getClass().getName(),
             exception.getMessage(),
             getFullStackTrace(exception),
             false,
             exception.getCause() != null ? fromExceptionRecursive(exception.getCause(), null) : null,
             null);
    }

    /**
     * Creates a {@code FailureDetails} from an exception, optionally using the provided
     * {@link ExceptionPropertiesProvider} to extract custom properties.
     *
     * @param exception the exception that caused the failure
     * @param provider  the provider for extracting custom properties, or {@code null}
     * @return a new {@code FailureDetails} instance
     */
    static FailureDetails fromException(Exception exception, @Nullable ExceptionPropertiesProvider provider) {
        Map<String, Object> properties = null;
        if (provider != null) {
            try {
                properties = provider.getExceptionProperties(exception);
            } catch (Exception ignored) {
                // Don't let provider errors mask the original failure
            }
        }
        return new FailureDetails(
                exception.getClass().getName(),
                exception.getMessage(),
                getFullStackTrace(exception),
                false,
                exception.getCause() != null ? fromExceptionRecursive(exception.getCause(), provider) : null,
                properties);
    }

    FailureDetails(TaskFailureDetails proto) {
        this(proto.getErrorType(),
             proto.getErrorMessage(),
             proto.getStackTrace().getValue(),
             proto.getIsNonRetriable(),
             proto.hasInnerFailure() ? new FailureDetails(proto.getInnerFailure()) : null,
             convertProtoProperties(proto.getPropertiesMap()));
    }

    /**
     * Gets the exception class name if the failure was caused by an unhandled exception. Otherwise, gets a symbolic
     * name that describes the general type of error that was encountered.
     *
     * @return the error type as a {@code String} value
     */
    @Nonnull
    public String getErrorType() {
        return this.errorType;
    }

    /**
     * Gets a summary description of the error that caused this failure. If the failure was caused by an exception, the
     * exception message is returned.
     *
     * @return a summary description of the error
     */
    @Nonnull
    public String getErrorMessage() {
        return this.errorMessage;
    }

    /**
     * Gets the stack trace of the exception that caused this failure, or {@code null} if the failure was caused by
     * a non-exception error.
     *
     * @return the stack trace of the failure exception or {@code null} if the failure was not caused by an exception
     */
    @Nullable
    public String getStackTrace() {
        return this.stackTrace;
    }

    /**
     * Returns {@code true} if the failure doesn't permit retries, otherwise {@code false}.
     * @return {@code true} if the failure doesn't permit retries, otherwise {@code false}.
     */
    public boolean isNonRetriable() {
        return this.isNonRetriable;
    }

    /**
     * Gets the inner failure that caused this failure, or {@code null} if there is no inner cause.
     *
     * @return the inner {@code FailureDetails} or {@code null}
     */
    @Nullable
    public FailureDetails getInnerFailure() {
        return this.innerFailure;
    }

    /**
     * Gets additional properties associated with the exception, or {@code null} if no properties are available.
     * <p>
     * The returned map is unmodifiable.
     *
     * @return an unmodifiable map of property names to values, or {@code null}
     */
    @Nullable
    public Map<String, Object> getProperties() {
        return this.properties;
    }

    /**
     * Returns {@code true} if the task failure was provided by the specified exception type, otherwise {@code false}.
     * <p>
     * This method allows checking if a task failed due to a specific exception type by attempting to load the class
     * specified in {@link #getErrorType()}. If the exception class cannot be loaded for any reason, this method will
     * return {@code false}. Base types are supported by this method, as shown in the following example:
     * <pre>{@code
     * boolean isRuntimeException = failureDetails.isCausedBy(RuntimeException.class);
     * }</pre>
     *
     * @param exceptionClass the class representing the exception type to test
     * @return {@code true} if the task failure was provided by the specified exception type, otherwise {@code false}
     */
    public boolean isCausedBy(Class<? extends Exception> exceptionClass) {
        String actualClassName = this.getErrorType();
        try {
            // Try using reflection to load the failure's class type and see if it's a subtype of the specified
            // exception. For example, this should always succeed if exceptionClass is System.Exception.
            Class<?> actualExceptionClass = Class.forName(actualClassName);
            return exceptionClass.isAssignableFrom(actualExceptionClass);
        } catch (ClassNotFoundException ex) {
            // Can't load the class and thus can't tell if it's related
            return false;
        }
    }

    @Override
    public String toString() {
        return this.errorType + ": " + this.errorMessage;
    }

    static String getFullStackTrace(Throwable e) {
        StackTraceElement[] elements = e.getStackTrace();

        // Plan for 256 characters per stack frame (which is likely on the high-end)
        StringBuilder sb = new StringBuilder(elements.length * 256);
        for (StackTraceElement element : elements) {
            sb.append("\tat ").append(element.toString()).append(System.lineSeparator());
        }
        return sb.toString();
    }

    TaskFailureDetails toProto() {
        TaskFailureDetails.Builder builder = TaskFailureDetails.newBuilder()
                .setErrorType(this.getErrorType())
                .setErrorMessage(this.getErrorMessage())
                .setStackTrace(StringValue.of(this.getStackTrace() != null ? this.getStackTrace() : ""))
                .setIsNonRetriable(this.isNonRetriable);

        if (this.innerFailure != null) {
            builder.setInnerFailure(this.innerFailure.toProto());
        }

        if (this.properties != null) {
            builder.putAllProperties(convertToProtoProperties(this.properties));
        }

        return builder.build();
    }

    @Nullable
    private static FailureDetails fromExceptionRecursive(
            @Nullable Throwable exception,
            @Nullable ExceptionPropertiesProvider provider) {
        if (exception == null) {
            return null;
        }
        Map<String, Object> properties = null;
        if (provider != null && exception instanceof Exception) {
            try {
                properties = provider.getExceptionProperties((Exception) exception);
            } catch (Exception ignored) {
                // Don't let provider errors mask the original failure
            }
        }
        return new FailureDetails(
                exception.getClass().getName(),
                exception.getMessage(),
                getFullStackTrace(exception),
                false,
                exception.getCause() != null ? fromExceptionRecursive(exception.getCause(), provider) : null,
                properties);
    }

    @Nullable
    private static Map<String, Object> convertProtoProperties(Map<String, Value> protoProperties) {
        if (protoProperties == null || protoProperties.isEmpty()) {
            return null;
        }

        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Value> entry : protoProperties.entrySet()) {
            result.put(entry.getKey(), convertProtoValue(entry.getValue()));
        }
        return result;
    }

    @Nullable
    private static Object convertProtoValue(Value value) {
        if (value == null) {
            return null;
        }
        switch (value.getKindCase()) {
            case NULL_VALUE:
                return null;
            case NUMBER_VALUE:
                return value.getNumberValue();
            case STRING_VALUE:
                return value.getStringValue();
            case BOOL_VALUE:
                return value.getBoolValue();
            default:
                return value.toString();
        }
    }

    private static Map<String, Value> convertToProtoProperties(Map<String, Object> properties) {
        Map<String, Value> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            result.put(entry.getKey(), convertToProtoValue(entry.getValue()));
        }
        return result;
    }

    private static Value convertToProtoValue(@Nullable Object obj) {
        if (obj == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        } else if (obj instanceof Number) {
            return Value.newBuilder().setNumberValue(((Number) obj).doubleValue()).build();
        } else if (obj instanceof Boolean) {
            return Value.newBuilder().setBoolValue((Boolean) obj).build();
        } else if (obj instanceof String) {
            return Value.newBuilder().setStringValue((String) obj).build();
        } else {
            return Value.newBuilder().setStringValue(obj.toString()).build();
        }
    }
}
