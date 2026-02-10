// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Exception thrown when a two-way entity call fails because the entity operation threw an exception.
 * <p>
 * This is analogous to {@link TaskFailedException} but specific to entity operations invoked
 * via {@code callEntity}. The {@link #getFailureDetails()} method provides detailed information
 * about the failure.
 */
public class EntityOperationFailedException extends RuntimeException {
    private final EntityInstanceId entityId;
    private final String operationName;
    private final FailureDetails failureDetails;

    /**
     * Creates a new {@code EntityOperationFailedException}.
     *
     * @param entityId       the ID of the entity that failed
     * @param operationName  the name of the operation that failed
     * @param failureDetails the details of the failure
     */
    public EntityOperationFailedException(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nonnull FailureDetails failureDetails) {
        super(String.format(
                "Entity operation '%s' on entity '%s' failed: %s",
                operationName,
                entityId.toString(),
                failureDetails.getErrorMessage()));
        this.entityId = entityId;
        this.operationName = operationName;
        this.failureDetails = failureDetails;
    }

    /**
     * Gets the ID of the entity that failed.
     *
     * @return the entity instance ID
     */
    @Nonnull
    public EntityInstanceId getEntityId() {
        return this.entityId;
    }

    /**
     * Gets the name of the operation that failed.
     *
     * @return the operation name
     */
    @Nonnull
    public String getOperationName() {
        return this.operationName;
    }

    /**
     * Gets the failure details, including the error type, message, and stack trace.
     *
     * @return the failure details
     */
    @Nonnull
    public FailureDetails getFailureDetails() {
        return this.failureDetails;
    }
}
