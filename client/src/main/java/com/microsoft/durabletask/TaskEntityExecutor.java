// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executes entity batch requests by dispatching operations to registered entity factories.
 * <p>
 * Each operation in the batch is executed independently with transactional semantics:
 * successful operations commit their state and actions, while failed operations roll back
 * to the previous committed state and discard any actions enqueued during the failed operation.
 */
final class TaskEntityExecutor {
    private final HashMap<String, TaskEntityFactory> entityFactories;
    private final DataConverter dataConverter;
    private final Logger logger;

    TaskEntityExecutor(
            HashMap<String, TaskEntityFactory> entityFactories,
            DataConverter dataConverter,
            Logger logger) {
        this.entityFactories = entityFactories;
        this.dataConverter = dataConverter;
        this.logger = logger;
    }

    /**
     * Executes a batch of entity operations from an {@code EntityBatchRequest}.
     *
     * @param request the entity batch request from the sidecar
     * @return the entity batch result to send back to the sidecar
     */
    @Nonnull
    EntityBatchResult execute(@Nonnull EntityBatchRequest request) {
        String instanceId = request.getInstanceId();
        EntityInstanceId entityId = EntityInstanceId.fromString(instanceId);
        String entityName = entityId.getName();

        logger.log(Level.FINE, "Executing entity batch for '{0}' with {1} operation(s).",
                new Object[]{instanceId, request.getOperationsCount()});

        // Look up the entity factory
        TaskEntityFactory factory = this.entityFactories.get(entityName);
        if (factory == null) {
            // Try case-insensitive lookup
            for (Map.Entry<String, TaskEntityFactory> entry : this.entityFactories.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(entityName)) {
                    factory = entry.getValue();
                    break;
                }
            }
        }

        if (factory == null) {
            String errorMessage = String.format("No entity named '%s' is registered.", entityName);
            logger.log(Level.WARNING, errorMessage);
            TaskFailureDetails failureDetails = TaskFailureDetails.newBuilder()
                    .setErrorType(IllegalStateException.class.getName())
                    .setErrorMessage(errorMessage)
                    .build();
            return EntityBatchResult.newBuilder()
                    .setFailureDetails(failureDetails)
                    .build();
        }

        // Initialize state from the request
        String initialState = request.hasEntityState()
                ? request.getEntityState().getValue()
                : null;
        TaskEntityState entityState = new TaskEntityState(this.dataConverter, initialState);

        // Create the concrete context that collects actions
        TaskEntityContextImpl context = new TaskEntityContextImpl(entityId, this.dataConverter);

        // Process each operation
        List<OperationResult> results = new ArrayList<>();
        int actionIdCounter = 0;

        // Create a single entity instance for the entire batch
        ITaskEntity entity;
        try {
            entity = factory.create();
            if (entity == null) {
                String errorMsg = String.format("The entity factory for '%s' returned a null entity.", entityName);
                logger.log(Level.WARNING, errorMsg);
                TaskFailureDetails failureDetails = TaskFailureDetails.newBuilder()
                        .setErrorType(IllegalStateException.class.getName())
                        .setErrorMessage(errorMsg)
                        .build();
                return EntityBatchResult.newBuilder()
                        .setFailureDetails(failureDetails)
                        .build();
            }
        } catch (Exception e) {
            String errorMsg = String.format("Failed to create entity instance for '%s': %s", entityName, e.getMessage());
            logger.log(Level.WARNING, errorMsg, e);
            TaskFailureDetails failureDetails = TaskFailureDetails.newBuilder()
                    .setErrorType(e.getClass().getName())
                    .setErrorMessage(e.getMessage() != null ? e.getMessage() : "")
                    .setStackTrace(StringValue.of(FailureDetails.getFullStackTrace(e)))
                    .build();
            return EntityBatchResult.newBuilder()
                    .setFailureDetails(failureDetails)
                    .build();
        }

        for (OperationRequest opRequest : request.getOperationsList()) {
            String operationName = opRequest.getOperation();
            String requestId = opRequest.getRequestId();
            String serializedInput = opRequest.hasInput() ? opRequest.getInput().getValue() : null;

            logger.log(Level.FINE, "Executing operation '{0}' (requestId={1}) on entity '{2}'.",
                    new Object[]{operationName, requestId, instanceId});

            // Snapshot state and actions before each operation (for rollback on failure)
            entityState.commit();
            context.commit();

            Instant startTime = Instant.now();

            try {
                // Build the operation
                TaskEntityOperation operation = new TaskEntityOperation(
                        operationName, serializedInput, context, entityState, this.dataConverter);

                // Execute
                Object result = entity.runAsync(operation);

                Instant endTime = Instant.now();

                // Build success result
                OperationResultSuccess.Builder successBuilder = OperationResultSuccess.newBuilder()
                        .setStartTimeUtc(toTimestamp(startTime))
                        .setEndTimeUtc(toTimestamp(endTime));

                if (result != null) {
                    String serializedResult = this.dataConverter.serialize(result);
                    if (serializedResult != null) {
                        successBuilder.setResult(StringValue.of(serializedResult));
                    }
                }

                results.add(OperationResult.newBuilder()
                        .setSuccess(successBuilder.build())
                        .build());

                // Commit state and actions on success
                entityState.commit();
                context.commit();

                logger.log(Level.FINE, "Operation '{0}' on entity '{1}' completed successfully.",
                        new Object[]{operationName, instanceId});

            } catch (Exception e) {
                Instant endTime = Instant.now();

                logger.log(Level.WARNING,
                        String.format("Operation '%s' on entity '%s' failed: %s",
                                operationName, instanceId, e.getMessage()),
                        e);

                // Build failure result
                TaskFailureDetails failureDetails = TaskFailureDetails.newBuilder()
                        .setErrorType(e.getClass().getName())
                        .setErrorMessage(e.getMessage() != null ? e.getMessage() : "")
                        .setStackTrace(StringValue.of(FailureDetails.getFullStackTrace(e)))
                        .build();

                OperationResultFailure failure = OperationResultFailure.newBuilder()
                        .setFailureDetails(failureDetails)
                        .setStartTimeUtc(toTimestamp(startTime))
                        .setEndTimeUtc(toTimestamp(endTime))
                        .build();

                results.add(OperationResult.newBuilder()
                        .setFailure(failure)
                        .build());

                // Rollback state and actions on failure
                entityState.rollback();
                context.rollback();
            }
        }

        // Build the final result
        EntityBatchResult.Builder resultBuilder = EntityBatchResult.newBuilder()
                .addAllResults(results)
                .addAllActions(context.getCommittedActions(actionIdCounter));

        // Set the final entity state
        String finalState = entityState.getSerializedState();
        if (finalState != null) {
            resultBuilder.setEntityState(StringValue.of(finalState));
        }

        return resultBuilder.build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    /**
     * Concrete implementation of {@link TaskEntityContext} that collects {@link OperationAction} protos
     * during entity operation execution.
     */
    private static class TaskEntityContextImpl extends TaskEntityContext {
        private final EntityInstanceId entityId;
        private final DataConverter dataConverter;
        private final List<PendingAction> pendingActions = new ArrayList<>();
        private int committedActionCount = 0;

        TaskEntityContextImpl(EntityInstanceId entityId, DataConverter dataConverter) {
            this.entityId = entityId;
            this.dataConverter = dataConverter;
        }

        @Nonnull
        @Override
        public EntityInstanceId getId() {
            return this.entityId;
        }

        @Override
        public void signalEntity(
                @Nonnull EntityInstanceId targetEntityId,
                @Nonnull String operationName,
                @Nullable Object input,
                @Nullable SignalEntityOptions options) {
            Objects.requireNonNull(targetEntityId, "targetEntityId must not be null");
            Objects.requireNonNull(operationName, "operationName must not be null");

            SendSignalAction.Builder signalBuilder = SendSignalAction.newBuilder()
                    .setInstanceId(targetEntityId.toString())
                    .setName(operationName);

            if (input != null) {
                String serializedInput = this.dataConverter.serialize(input);
                if (serializedInput != null) {
                    signalBuilder.setInput(StringValue.of(serializedInput));
                }
            }

            if (options != null && options.getScheduledTime() != null) {
                Instant scheduledTime = options.getScheduledTime();
                signalBuilder.setScheduledTime(Timestamp.newBuilder()
                        .setSeconds(scheduledTime.getEpochSecond())
                        .setNanos(scheduledTime.getNano())
                        .build());
            }

            this.pendingActions.add(new PendingAction(PendingAction.Type.SEND_SIGNAL, signalBuilder.build(), null));
        }

        @Nonnull
        @Override
        public String startNewOrchestration(
                @Nonnull String name,
                @Nullable Object input,
                @Nullable NewOrchestrationInstanceOptions options) {
            Objects.requireNonNull(name, "orchestration name must not be null");

            String instanceId = (options != null && options.getInstanceId() != null)
                    ? options.getInstanceId()
                    : UUID.randomUUID().toString();

            StartNewOrchestrationAction.Builder orchBuilder = StartNewOrchestrationAction.newBuilder()
                    .setInstanceId(instanceId)
                    .setName(name);

            if (input != null) {
                String serializedInput = this.dataConverter.serialize(input);
                if (serializedInput != null) {
                    orchBuilder.setInput(StringValue.of(serializedInput));
                }
            }

            if (options != null) {
                if (options.getVersion() != null) {
                    orchBuilder.setVersion(StringValue.of(options.getVersion()));
                }
                if (options.getStartTime() != null) {
                    Instant startTime = options.getStartTime();
                    orchBuilder.setScheduledTime(Timestamp.newBuilder()
                            .setSeconds(startTime.getEpochSecond())
                            .setNanos(startTime.getNano())
                            .build());
                }
            }

            this.pendingActions.add(new PendingAction(
                    PendingAction.Type.START_NEW_ORCHESTRATION, null, orchBuilder.build()));

            return instanceId;
        }

        /**
         * Marks the current set of pending actions as committed (snapshot for rollback).
         */
        void commit() {
            this.committedActionCount = this.pendingActions.size();
        }

        /**
         * Rolls back any uncommitted actions (discards actions added since last commit).
         */
        void rollback() {
            while (this.pendingActions.size() > this.committedActionCount) {
                this.pendingActions.remove(this.pendingActions.size() - 1);
            }
        }

        /**
         * Returns all committed actions as proto {@link OperationAction} objects.
         *
         * @param startId the starting ID for action numbering
         * @return the list of committed operation actions
         */
        List<OperationAction> getCommittedActions(int startId) {
            List<OperationAction> actions = new ArrayList<>();
            int id = startId;
            for (PendingAction pending : this.pendingActions) {
                OperationAction.Builder actionBuilder = OperationAction.newBuilder()
                        .setId(id++);
                if (pending.type == PendingAction.Type.SEND_SIGNAL) {
                    actionBuilder.setSendSignal(pending.sendSignal);
                } else {
                    actionBuilder.setStartNewOrchestration(pending.startNewOrchestration);
                }
                actions.add(actionBuilder.build());
            }
            return actions;
        }

        /**
         * Represents a pending action (signal or orchestration start) collected during entity execution.
         */
        private static class PendingAction {
            enum Type { SEND_SIGNAL, START_NEW_ORCHESTRATION }

            final Type type;
            final SendSignalAction sendSignal;
            final StartNewOrchestrationAction startNewOrchestration;

            PendingAction(Type type, SendSignalAction sendSignal, StartNewOrchestrationAction startNewOrchestration) {
                this.type = type;
                this.sendSignal = sendSignal;
                this.startNewOrchestration = startNewOrchestration;
            }
        }
    }
}
