// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import com.microsoft.durabletask.EntityInstanceId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Request payload for the {@code ExecuteExportJobOperationOrchestrator}.
 * Encapsulates the target entity ID, operation name, and optional input.
 */
public final class ExportJobOperationRequest {

    private final EntityInstanceId entityId;
    private final String operationName;
    private final Object input;

    public ExportJobOperationRequest(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nullable Object input) {
        this.entityId = entityId;
        this.operationName = operationName;
        this.input = input;
    }

    public ExportJobOperationRequest(@Nonnull EntityInstanceId entityId, @Nonnull String operationName) {
        this(entityId, operationName, null);
    }

    // Default constructor for Jackson deserialization
    public ExportJobOperationRequest() {
        this.entityId = null;
        this.operationName = null;
        this.input = null;
    }

    @Nonnull
    public EntityInstanceId getEntityId() { return this.entityId; }

    @Nonnull
    public String getOperationName() { return this.operationName; }

    @Nullable
    public Object getInput() { return this.input; }
}
