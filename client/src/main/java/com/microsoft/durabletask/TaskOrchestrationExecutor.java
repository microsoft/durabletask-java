// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.interruption.ContinueAsNewInterruption;
import com.microsoft.durabletask.interruption.OrchestratorBlockedException;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.ScheduleTaskAction.Builder;
import com.microsoft.durabletask.util.UUIDGenerator;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.logging.Logger;

final class TaskOrchestrationExecutor {

    private static final String EMPTY_STRING = "";
    private final HashMap<String, TaskOrchestrationFactory> orchestrationFactories;
    private final DataConverter dataConverter;
    private final Logger logger;
    private final Duration maximumTimerInterval;
    private final DurableTaskGrpcWorkerVersioningOptions versioningOptions;

    public TaskOrchestrationExecutor(
            HashMap<String, TaskOrchestrationFactory> orchestrationFactories,
            DataConverter dataConverter,
            Duration maximumTimerInterval,
            Logger logger,
            DurableTaskGrpcWorkerVersioningOptions versioningOptions) {
        this.orchestrationFactories = orchestrationFactories;
        this.dataConverter = dataConverter;
        this.maximumTimerInterval = maximumTimerInterval;
        this.logger = logger;
        this.versioningOptions = versioningOptions;
    }

    public TaskOrchestratorResult execute(List<HistoryEvent> pastEvents, List<HistoryEvent> newEvents) {
        ContextImplTask context = new ContextImplTask(pastEvents, newEvents);

        if (this.versioningOptions != null && this.versioningOptions.getDefaultVersion() != null) {
            // Set the default version for the orchestrator
            context.setDefaultVersion(this.versioningOptions.getDefaultVersion());
        }

        boolean completed = false;
        try {
            // Play through the history events until either we've played through everything
            // or we receive a yield signal
            while (context.processNextEvent()) { /* no method body */ }
            completed = true;
        } catch (OrchestratorBlockedException orchestratorBlockedException) {
            logger.fine("The orchestrator has yielded and will await for new events.");
        } catch (ContinueAsNewInterruption continueAsNewInterruption) {
            logger.fine("The orchestrator has continued as new.");
            context.complete(null);
        } catch (Exception e) {
            // The orchestrator threw an unhandled exception - fail it
            // TODO: What's the right way to log this?
            logger.warning("The orchestrator failed with an unhandled exception: " + e.toString());
            context.fail(new FailureDetails(e));
        }

        if ((context.continuedAsNew && !context.isComplete) || (completed && context.pendingActions.isEmpty() && !context.waitingForEvents())) {
            // There are no further actions for the orchestrator to take so auto-complete the orchestration.
            context.complete(null);
        }

        return new TaskOrchestratorResult(context.pendingActions.values(), context.getCustomStatus());
    }

    private class ContextImplTask implements TaskOrchestrationContext {

        private String orchestratorName;
        private String rawInput;
        private String instanceId;
        private Instant currentInstant;
        private boolean isComplete;
        private boolean isSuspended;
        private boolean isReplaying = true;
        private int newUUIDCounter;
        private String version;
        private String defaultVersion;

        // LinkedHashMap to maintain insertion order when returning the list of pending actions
        private final LinkedHashMap<Integer, OrchestratorAction> pendingActions = new LinkedHashMap<>();
        private final HashMap<Integer, TaskRecord<?>> openTasks = new HashMap<>();
        private final LinkedHashMap<String, Queue<TaskRecord<?>>> outstandingEvents = new LinkedHashMap<>();
        private final LinkedList<HistoryEvent> unprocessedEvents = new LinkedList<>();
        private final Queue<HistoryEvent> eventsWhileSuspended = new ArrayDeque<>();
        private final DataConverter dataConverter = TaskOrchestrationExecutor.this.dataConverter;
        private final Duration maximumTimerInterval = TaskOrchestrationExecutor.this.maximumTimerInterval;
        private final Logger logger = TaskOrchestrationExecutor.this.logger;
        private final OrchestrationHistoryIterator historyEventPlayer;
        private int sequenceNumber;
        private boolean continuedAsNew;
        private Object continuedAsNewInput;
        private boolean preserveUnprocessedEvents;
        private Object customStatus;

        // Entity integration state (Phase 4)
        private String executionId;
        private boolean isInCriticalSection;
        private String currentCriticalSectionId;
        private Set<String> lockedEntityIds;

        public ContextImplTask(List<HistoryEvent> pastEvents, List<HistoryEvent> newEvents) {
            this.historyEventPlayer = new OrchestrationHistoryIterator(pastEvents, newEvents);
        }

        @Override
        public String getName() {
            // TODO: Throw if name is null
            return this.orchestratorName;
        }

        private void setName(String name) {
            // TODO: Throw if name is not null
            this.orchestratorName = name;
        }

        private void setInput(String rawInput) {
            this.rawInput = rawInput;
        }

        @Override
        public <T> T getInput(Class<T> targetType) {
            if (this.rawInput == null || this.rawInput.length() == 0) {
                return null;
            }

            return this.dataConverter.deserialize(this.rawInput, targetType);
        }

        @Override
        public String getInstanceId() {
            // TODO: Throw if instance ID is null
            return this.instanceId;
        }

        private void setInstanceId(String instanceId) {
            // TODO: Throw if instance ID is not null
            this.instanceId = instanceId;
        }

        @Override
        public Instant getCurrentInstant() {
            // TODO: Throw if instant is null
            return this.currentInstant;
        }

        private void setCurrentInstant(Instant instant) {
            // This will be set multiple times as the orchestration progresses
            this.currentInstant = instant;
        }

        private String getCustomStatus()
        {
            return this.customStatus != null ? this.dataConverter.serialize(this.customStatus) : EMPTY_STRING;
        }

        @Override
        public void setCustomStatus(Object customStatus) {
            this.customStatus = customStatus;
        }

        @Override
        public void clearCustomStatus() {
            this.setCustomStatus(null);
        }

        @Override
        public boolean getIsReplaying() {
            return this.isReplaying;
        }

        private void setDoneReplaying() {
            this.isReplaying = false;
        }

        @Override
        public String getVersion() {
            return this.version;
        }

        private void setVersion(String version) {
            this.version = version;
        }

        private String getDefaultVersion() {
            return this.defaultVersion;
        }

        private void setDefaultVersion(String defaultVersion) {
            // This is used when starting sub-orchestrations
            this.defaultVersion = defaultVersion;
        }

        public <V> Task<V> completedTask(V value) {
            CompletableTask<V> task = new CompletableTask<>();
            task.complete(value);
            return task;
        }

        @Override
        public <V> Task<List<V>> allOf(List<Task<V>> tasks) {
            Helpers.throwIfArgumentNull(tasks, "tasks");

            CompletableFuture<V>[] futures = tasks.stream()
                    .map(t -> t.future)
                    .toArray((IntFunction<CompletableFuture<V>[]>) CompletableFuture[]::new);

            Function<Void, List<V>> resultPath = x -> {
                List<V> results = new ArrayList<>(futures.length);

                // All futures are expected to be completed at this point
                for (CompletableFuture<V> cf : futures) {
                    try {
                        results.add(cf.get());
                    } catch (Exception ex) {
                        results.add(null);
                    }
                }
                return results;
            };

            Function<Throwable, ? extends List<V>> exceptionPath = throwable -> {
                ArrayList<Exception> exceptions = new ArrayList<>(futures.length);
                for (CompletableFuture<V> cf : futures) {
                    try {
                        cf.get();
                    } catch (ExecutionException ex) {
                        exceptions.add((Exception) ex.getCause());
                    } catch (Exception ex) {
                        exceptions.add(ex);
                    }
                }
                throw new CompositeTaskFailedException(
                        String.format(
                                "%d out of %d tasks failed with an exception. See the exceptions list for details.",
                                exceptions.size(),
                                futures.length),
                        exceptions);
            };
            CompletableFuture<List<V>> future = CompletableFuture.allOf(futures)
                    .thenApply(resultPath)
                    .exceptionally(exceptionPath);

            return new CompoundTask<>(tasks, future);
        }

        @Override
        public Task<Task<?>> anyOf(List<Task<?>> tasks) {
            Helpers.throwIfArgumentNull(tasks, "tasks");

            CompletableFuture<?>[] futures = tasks.stream()
                    .map(t -> t.future)
                    .toArray((IntFunction<CompletableFuture<?>[]>) CompletableFuture[]::new);

            CompletableFuture<Task<?>> future = CompletableFuture.anyOf(futures).thenApply(x -> {
                // Return the first completed task in the list. Unlike the implementation in other languages,
                // this might not necessarily be the first task that completed, so calling code shouldn't make
                // assumptions about this. Note that changing this behavior later could be breaking.
                for (Task<?> task : tasks) {
                    if (task.isDone()) {
                        return task;
                    }
                }

                // Should never get here
                return completedTask(null);
            });

            return new CompoundTask(tasks, future);
        }

        @Override
        public <V> Task<V> callActivity(
                String name,
                @Nullable Object input,
                @Nullable TaskOptions options,
                Class<V> returnType) {
            Helpers.throwIfOrchestratorComplete(this.isComplete);
            Helpers.throwIfArgumentNull(name, "name");
            Helpers.throwIfArgumentNull(returnType, "returnType");

            if (input instanceof TaskOptions) {
                throw new IllegalArgumentException("TaskOptions cannot be used as an input. Did you call the wrong method overload?");
            }

            String serializedInput = this.dataConverter.serialize(input);
            Builder scheduleTaskBuilder = ScheduleTaskAction.newBuilder().setName(name);
            if (serializedInput != null) {
                scheduleTaskBuilder.setInput(StringValue.of(serializedInput));
            }

            TaskFactory<V> taskFactory = () -> {
                int id = this.sequenceNumber++;
                this.pendingActions.put(id, OrchestratorAction.newBuilder()
                        .setId(id)
                        .setScheduleTask(scheduleTaskBuilder)
                        .build());

                if (!this.isReplaying) {
                    this.logger.fine(() -> String.format(
                            "%s: calling activity '%s' (#%d) with serialized input: %s",
                            this.instanceId,
                            name,
                            id,
                            serializedInput != null ? serializedInput : "(null)"));
                }

                CompletableTask<V> task = new CompletableTask<>();
                TaskRecord<V> record = new TaskRecord<>(task, name, returnType);
                this.openTasks.put(id, record);
                return task;
            };

            return this.createAppropriateTask(taskFactory, options);
        }

        @Override
        public void continueAsNew(Object input, boolean preserveUnprocessedEvents) {
            Helpers.throwIfOrchestratorComplete(this.isComplete);

            this.continuedAsNew = true;
            this.continuedAsNewInput = input;
            this.preserveUnprocessedEvents = preserveUnprocessedEvents;

            // The ContinueAsNewInterruption exception allows the orchestration to complete immediately and return back
            // to the sidecar.
            // We can send the current set of actions back to the worker and wait for new events to come in.
            // This is *not* an exception - it's a normal part of orchestrator control flow.
            throw new ContinueAsNewInterruption(
                    "The orchestrator invoked continueAsNew. This Throwable should never be caught by user code.");
        }

        @Override
        public UUID newUUID() {
            final int version = 5;
            final String hashV5 = "SHA-1";
            final String dnsNameSpace = "9e952958-5e33-4daf-827f-2fa12937b875";
            final String name = new StringBuilder(this.instanceId)
                    .append("-")
                    .append(this.currentInstant)
                    .append("-")
                    .append(this.newUUIDCounter).toString();
            this.newUUIDCounter++;
            return UUIDGenerator.generate(version, hashV5, UUID.fromString(dnsNameSpace), name);
        }

        // region Entity integration methods (Phase 4)

        @Override
        public void signalEntity(EntityInstanceId entityId, String operationName, Object input) {
            Helpers.throwIfOrchestratorComplete(this.isComplete);
            Helpers.throwIfArgumentNull(entityId, "entityId");
            Helpers.throwIfArgumentNull(operationName, "operationName");

            int id = this.sequenceNumber++;
            String requestId = this.newUUID().toString();
            String serializedInput = this.dataConverter.serialize(input);

            EntityOperationSignaledEvent.Builder signalBuilder = EntityOperationSignaledEvent.newBuilder()
                    .setRequestId(requestId)
                    .setOperation(operationName)
                    .setTargetInstanceId(StringValue.of(entityId.toString()));
            if (serializedInput != null) {
                signalBuilder.setInput(StringValue.of(serializedInput));
            }

            this.pendingActions.put(id, OrchestratorAction.newBuilder()
                    .setId(id)
                    .setSendEntityMessage(SendEntityMessageAction.newBuilder()
                            .setEntityOperationSignaled(signalBuilder)
                            .build())
                    .build());

            if (!this.isReplaying) {
                this.logger.fine(() -> String.format(
                        "%s: signaling entity '%s' operation '%s' (#%d)",
                        this.instanceId,
                        entityId,
                        operationName,
                        id));
            }
        }

        @Override
        public <V> Task<V> callEntity(EntityInstanceId entityId, String operationName, Object input, Class<V> returnType) {
            Helpers.throwIfOrchestratorComplete(this.isComplete);
            Helpers.throwIfArgumentNull(entityId, "entityId");
            Helpers.throwIfArgumentNull(operationName, "operationName");
            Helpers.throwIfArgumentNull(returnType, "returnType");

            int id = this.sequenceNumber++;
            String requestId = this.newUUID().toString();
            String serializedInput = this.dataConverter.serialize(input);

            EntityOperationCalledEvent.Builder callBuilder = EntityOperationCalledEvent.newBuilder()
                    .setRequestId(requestId)
                    .setOperation(operationName)
                    .setTargetInstanceId(StringValue.of(entityId.toString()))
                    .setParentInstanceId(StringValue.of(this.instanceId));
            if (this.executionId != null) {
                callBuilder.setParentExecutionId(StringValue.of(this.executionId));
            }
            if (serializedInput != null) {
                callBuilder.setInput(StringValue.of(serializedInput));
            }

            this.pendingActions.put(id, OrchestratorAction.newBuilder()
                    .setId(id)
                    .setSendEntityMessage(SendEntityMessageAction.newBuilder()
                            .setEntityOperationCalled(callBuilder)
                            .build())
                    .build());

            if (!this.isReplaying) {
                this.logger.fine(() -> String.format(
                        "%s: calling entity '%s' operation '%s' (#%d) requestId=%s",
                        this.instanceId,
                        entityId,
                        operationName,
                        id,
                        requestId));
            }

            CompletableTask<V> task = new CompletableTask<>();
            TaskRecord<V> record = new TaskRecord<>(task, operationName, returnType);
            Queue<TaskRecord<?>> eventQueue = this.outstandingEvents.computeIfAbsent(requestId, k -> new LinkedList<>());
            eventQueue.add(record);
            return task;
        }

        @Override
        public Task<AutoCloseable> lockEntities(List<EntityInstanceId> entityIds) {
            Helpers.throwIfOrchestratorComplete(this.isComplete);
            Helpers.throwIfArgumentNull(entityIds, "entityIds");
            if (entityIds.isEmpty()) {
                throw new IllegalArgumentException("entityIds must not be empty");
            }
            if (this.isInCriticalSection) {
                throw new IllegalStateException(
                        "Cannot nest lock calls. The orchestration is already inside a critical section.");
            }

            // Sort entity IDs deterministically to prevent deadlocks
            List<EntityInstanceId> sortedIds = new ArrayList<>(entityIds);
            Collections.sort(sortedIds);

            String criticalSectionId = this.newUUID().toString();

            // Build lock set as string list
            List<String> lockSet = new ArrayList<>(sortedIds.size());
            for (EntityInstanceId eid : sortedIds) {
                lockSet.add(eid.toString());
            }

            // Send a lock request for each entity in sorted order
            for (int position = 0; position < sortedIds.size(); position++) {
                int id = this.sequenceNumber++;
                EntityLockRequestedEvent.Builder lockBuilder = EntityLockRequestedEvent.newBuilder()
                        .setCriticalSectionId(criticalSectionId)
                        .addAllLockSet(lockSet)
                        .setPosition(position)
                        .setParentInstanceId(StringValue.of(this.instanceId));

                this.pendingActions.put(id, OrchestratorAction.newBuilder()
                        .setId(id)
                        .setSendEntityMessage(SendEntityMessageAction.newBuilder()
                                .setEntityLockRequested(lockBuilder)
                                .build())
                        .build());
            }

            if (!this.isReplaying) {
                this.logger.fine(() -> String.format(
                        "%s: requesting locks on %d entities, criticalSectionId=%s",
                        this.instanceId,
                        sortedIds.size(),
                        criticalSectionId));
            }

            // Create a waiter keyed by criticalSectionId
            CompletableTask<AutoCloseable> lockTask = new CompletableTask<>();
            TaskRecord<AutoCloseable> record = new TaskRecord<>(lockTask, "(lock)", AutoCloseable.class);
            Queue<TaskRecord<?>> eventQueue = this.outstandingEvents.computeIfAbsent(criticalSectionId, k -> new LinkedList<>());
            eventQueue.add(record);

            // Wrap the result so that when the lock is granted, we return an AutoCloseable
            // that releases all locks on close()
            return lockTask.thenApply(ignored -> (AutoCloseable) () -> {
                // Release all locks
                for (EntityInstanceId lockedEntity : sortedIds) {
                    int unlockId = this.sequenceNumber++;
                    EntityUnlockSentEvent.Builder unlockBuilder = EntityUnlockSentEvent.newBuilder()
                            .setCriticalSectionId(criticalSectionId)
                            .setParentInstanceId(StringValue.of(this.instanceId))
                            .setTargetInstanceId(StringValue.of(lockedEntity.toString()));

                    this.pendingActions.put(unlockId, OrchestratorAction.newBuilder()
                            .setId(unlockId)
                            .setSendEntityMessage(SendEntityMessageAction.newBuilder()
                                    .setEntityUnlockSent(unlockBuilder)
                                    .build())
                            .build());
                }

                this.isInCriticalSection = false;
                this.currentCriticalSectionId = null;
                this.lockedEntityIds = null;

                if (!this.isReplaying) {
                    this.logger.fine(() -> String.format(
                            "%s: released locks for criticalSectionId=%s",
                            this.instanceId,
                            criticalSectionId));
                }
            });
        }

        @Override
        public boolean isInCriticalSection() {
            return this.isInCriticalSection;
        }

        // endregion

        @Override
        public void sendEvent(String instanceId, String eventName, Object eventData) {
            Helpers.throwIfOrchestratorComplete(this.isComplete);
            Helpers.throwIfArgumentNullOrWhiteSpace(instanceId, "instanceId");

            int id = this.sequenceNumber++;
            String serializedEventData = this.dataConverter.serialize(eventData);
            OrchestrationInstance.Builder OrchestrationInstanceBuilder = OrchestrationInstance.newBuilder().setInstanceId(instanceId);
            SendEventAction.Builder builder = SendEventAction.newBuilder().setInstance(OrchestrationInstanceBuilder).setName(eventName);
            if (serializedEventData != null){
                builder.setData(StringValue.of(serializedEventData));
            }

            this.pendingActions.put(id, OrchestratorAction.newBuilder()
                    .setId(id)
                    .setSendEvent(builder)
                    .build());

            if (!this.isReplaying) {
                this.logger.fine(() -> String.format(
                        "%s: sending event '%s' (#%d) with serialized event data: %s",
                        this.instanceId,
                        eventName,
                        id,
                        serializedEventData != null ? serializedEventData : "(null)"));
            }
        }

        @Override
        public <V> Task<V> callSubOrchestrator(
                String name,
                @Nullable Object input,
                @Nullable String instanceId,
                @Nullable TaskOptions options,
                Class<V> returnType) {
            Helpers.throwIfOrchestratorComplete(this.isComplete);
            Helpers.throwIfArgumentNull(name, "name");
            Helpers.throwIfArgumentNull(returnType, "returnType");

            if (input instanceof TaskOptions) {
                throw new IllegalArgumentException("TaskOptions cannot be used as an input. Did you call the wrong method overload?");
            }
            
            String serializedInput = this.dataConverter.serialize(input);
            CreateSubOrchestrationAction.Builder createSubOrchestrationActionBuilder = CreateSubOrchestrationAction.newBuilder().setName(name);
            if (serializedInput != null) {
                createSubOrchestrationActionBuilder.setInput(StringValue.of(serializedInput));
            }

            if (instanceId == null) {
                instanceId = this.newUUID().toString();
            }
            createSubOrchestrationActionBuilder.setInstanceId(instanceId);

            if (options instanceof NewSubOrchestrationInstanceOptions && ((NewSubOrchestrationInstanceOptions)options).getVersion() != null) {
                NewSubOrchestrationInstanceOptions subOrchestrationOptions = (NewSubOrchestrationInstanceOptions) options;
                createSubOrchestrationActionBuilder.setVersion(StringValue.of(subOrchestrationOptions.getVersion()));
            } else if (this.getDefaultVersion() != null) {
                // If the options are not of the correct type, we still allow the version to be set
                createSubOrchestrationActionBuilder.setVersion(StringValue.of(this.getDefaultVersion()));
            }

            TaskFactory<V> taskFactory = () -> {
                int id = this.sequenceNumber++;
                this.pendingActions.put(id, OrchestratorAction.newBuilder()
                        .setId(id)
                        .setCreateSubOrchestration(createSubOrchestrationActionBuilder)
                        .build());

                if (!this.isReplaying) {
                    this.logger.fine(() -> String.format(
                            "%s: calling sub-orchestration '%s' (#%d) with serialized input: %s",
                            this.instanceId,
                            name,
                            id,
                            serializedInput != null ? serializedInput : "(null)"));
                }

                CompletableTask<V> task = new CompletableTask<>();
                TaskRecord<V> record = new TaskRecord<>(task, name, returnType);
                this.openTasks.put(id, record);
                return task;
            };

            return this.createAppropriateTask(taskFactory, options);
        }

        private <V> Task<V> createAppropriateTask(TaskFactory<V> taskFactory, TaskOptions options) {
            // Retry policies and retry handlers will cause us to return a RetriableTask<V>
            if (options != null && options.hasRetryPolicy()) {
                return new RetriableTask<V>(this, taskFactory, options.getRetryPolicy());
            } if (options != null && options.hasRetryHandler()) {
                return new RetriableTask<V>(this, taskFactory, options.getRetryHandler());
            } else {
                // Return a single vanilla task without any wrapper
                return taskFactory.create();
            }
        }

        public <V> Task<V> waitForExternalEvent(String name, Duration timeout, Class<V> dataType) {
            Helpers.throwIfOrchestratorComplete(this.isComplete);
            Helpers.throwIfArgumentNull(name, "name");
            Helpers.throwIfArgumentNull(dataType, "dataType");

            int id = this.sequenceNumber++;

            CompletableTask<V> eventTask = new ExternalEventTask<>(name, id, timeout);
            
            // Check for a previously received event with the same name
            for (HistoryEvent e : this.unprocessedEvents) {
                EventRaisedEvent existing = e.getEventRaised();
                if (name.equalsIgnoreCase(existing.getName())) {
                    String rawEventData = existing.getInput().getValue();
                    V data = this.dataConverter.deserialize(rawEventData, dataType);
                    eventTask.complete(data);
                    this.unprocessedEvents.remove(e);
                    return eventTask;
                }
            }

            boolean hasTimeout = !Helpers.isInfiniteTimeout(timeout);

            // Immediately cancel the task and return if the timeout is zero.
            if (hasTimeout && timeout.isZero()) {
                eventTask.cancel();
                return eventTask;
            }

            // Add this task to the list of tasks waiting for an external event.
            TaskRecord<V> record = new TaskRecord<>(eventTask, name, dataType);
            Queue<TaskRecord<?>> eventQueue = this.outstandingEvents.computeIfAbsent(name, k -> new LinkedList<>());
            eventQueue.add(record);

            // If a non-infinite timeout is specified, schedule an internal durable timer.
            // If the timer expires and the external event task hasn't yet completed, we'll cancel the task.
            if (hasTimeout) {
                this.createTimer(timeout).future.thenRun(() -> {
                    if (!eventTask.isDone()) {
                        // Book-keeping - remove the task record for the canceled task
                        eventQueue.removeIf(t -> t.task == eventTask);
                        if (eventQueue.isEmpty()) {
                            this.outstandingEvents.remove(name);
                        }

                        eventTask.cancel();
                    }
                });
            }

            return eventTask;
        }

        private void handleTaskScheduled(HistoryEvent e) {
            int taskId = e.getEventId();

            TaskScheduledEvent taskScheduled = e.getTaskScheduled();

            // The history shows that this orchestrator created a durable task in a previous execution.
            // We can therefore remove it from the map of pending actions. If we can't find the pending
            // action, then we assume a non-deterministic code violation in the orchestrator.
            OrchestratorAction taskAction = this.pendingActions.remove(taskId);
            if (taskAction == null) {
                String message = String.format(
                        "Non-deterministic orchestrator detected: a history event scheduling an activity task with sequence ID %d and name '%s' was replayed but the current orchestrator implementation didn't actually schedule this task. Was a change made to the orchestrator code after this instance had already started running?",
                        taskId,
                        taskScheduled.getName());
                throw new NonDeterministicOrchestratorException(message);
            }
        }

        @SuppressWarnings("unchecked")
        private void handleTaskCompleted(HistoryEvent e) {
            TaskCompletedEvent completedEvent = e.getTaskCompleted();
            int taskId = completedEvent.getTaskScheduledId();
            TaskRecord<?> record = this.openTasks.remove(taskId);
            if (record == null) {
                this.logger.warning("Discarding a potentially duplicate TaskCompleted event with ID = " + taskId);
                return;
            }

            String rawResult = completedEvent.getResult().getValue();

            if (!this.isReplaying) {
                // TODO: Structured logging
                // TODO: Would it make more sense to put this log in the activity executor?
                this.logger.fine(() -> String.format(
                        "%s: Activity '%s' (#%d) completed with serialized output: %s",
                        this.instanceId,
                        record.getTaskName(),
                        taskId,
                        rawResult != null ? rawResult : "(null)"));

            }
            CompletableTask task = record.getTask();
            try {
                Object result = this.dataConverter.deserialize(rawResult, record.getDataType());
                task.complete(result);
            } catch (Exception ex) {
                task.completeExceptionally(ex);
            }
        }

        private void handleTaskFailed(HistoryEvent e) {
            TaskFailedEvent failedEvent = e.getTaskFailed();
            int taskId = failedEvent.getTaskScheduledId();
            TaskRecord<?> record = this.openTasks.remove(taskId);
            if (record == null) {
                // TODO: Log a warning about a potential duplicate task completion event
                return;
            }

            FailureDetails details = new FailureDetails(failedEvent.getFailureDetails());

            if (!this.isReplaying) {
                // TODO: Log task failure, including the number of bytes in the result
            }

            CompletableTask<?> task = record.getTask();
            TaskFailedException exception = new TaskFailedException(
                record.taskName,
                taskId,
                details);
            task.completeExceptionally(exception);
        }

        @SuppressWarnings("unchecked")
        private void handleEventRaised(HistoryEvent e) {
            EventRaisedEvent eventRaised = e.getEventRaised();
            String eventName = eventRaised.getName();

            Queue<TaskRecord<?>> outstandingEventQueue = this.outstandingEvents.get(eventName);
            if (outstandingEventQueue == null) {
                // No code is waiting for this event. Buffer it in case user-code waits for it later.
                this.unprocessedEvents.add(e);
                return;
            }

            // Signal the first waiter in the queue with this event payload.
            TaskRecord<?> matchingTaskRecord = outstandingEventQueue.remove();
            if (outstandingEventQueue.isEmpty()) {
                this.outstandingEvents.remove(eventName);
            }
            String rawResult = eventRaised.getInput().getValue();
            CompletableTask task = matchingTaskRecord.getTask();
            try {
                Object result = this.dataConverter.deserialize(
                        rawResult,
                        matchingTaskRecord.getDataType());
                task.complete(result);
            } catch (Exception ex) {
                task.completeExceptionally(ex);
            }
        }

        // region Entity event handlers (Phase 4)

        private void handleEntityOperationSignaled(HistoryEvent e) {
            int taskId = e.getEventId();
            OrchestratorAction taskAction = this.pendingActions.remove(taskId);
            if (taskAction == null) {
                String message = String.format(
                        "Non-deterministic orchestrator detected: a history event for entity signal with sequence ID %d was replayed but the current orchestrator implementation didn't schedule this signal.",
                        taskId);
                throw new NonDeterministicOrchestratorException(message);
            }
        }

        private void handleEntityOperationCalled(HistoryEvent e) {
            int taskId = e.getEventId();
            OrchestratorAction taskAction = this.pendingActions.remove(taskId);
            if (taskAction == null) {
                String message = String.format(
                        "Non-deterministic orchestrator detected: a history event for entity call with sequence ID %d was replayed but the current orchestrator implementation didn't schedule this call.",
                        taskId);
                throw new NonDeterministicOrchestratorException(message);
            }
        }

        @SuppressWarnings("unchecked")
        private void handleEntityOperationCompleted(HistoryEvent e) {
            EntityOperationCompletedEvent completedEvent = e.getEntityOperationCompleted();
            String requestId = completedEvent.getRequestId();

            Queue<TaskRecord<?>> outstandingQueue = this.outstandingEvents.get(requestId);
            if (outstandingQueue == null) {
                this.logger.warning("Discarding entity operation completed event with requestId=" + requestId + ": no waiter found");
                return;
            }

            TaskRecord<?> record = outstandingQueue.remove();
            if (outstandingQueue.isEmpty()) {
                this.outstandingEvents.remove(requestId);
            }

            String rawResult = completedEvent.hasOutput() ? completedEvent.getOutput().getValue() : null;

            if (!this.isReplaying) {
                this.logger.fine(() -> String.format(
                        "%s: Entity operation completed for requestId=%s with output: %s",
                        this.instanceId,
                        requestId,
                        rawResult != null ? rawResult : "(null)"));
            }

            CompletableTask task = record.getTask();
            try {
                Object result = this.dataConverter.deserialize(rawResult, record.getDataType());
                task.complete(result);
            } catch (Exception ex) {
                task.completeExceptionally(ex);
            }
        }

        private void handleEntityOperationFailed(HistoryEvent e) {
            EntityOperationFailedEvent failedEvent = e.getEntityOperationFailed();
            String requestId = failedEvent.getRequestId();

            Queue<TaskRecord<?>> outstandingQueue = this.outstandingEvents.get(requestId);
            if (outstandingQueue == null) {
                this.logger.warning("Discarding entity operation failed event with requestId=" + requestId + ": no waiter found");
                return;
            }

            TaskRecord<?> record = outstandingQueue.remove();
            if (outstandingQueue.isEmpty()) {
                this.outstandingEvents.remove(requestId);
            }

            FailureDetails details = new FailureDetails(failedEvent.getFailureDetails());

            if (!this.isReplaying) {
                this.logger.fine(() -> String.format(
                        "%s: Entity operation failed for requestId=%s: %s",
                        this.instanceId,
                        requestId,
                        details.getErrorMessage()));
            }

            CompletableTask<?> task = record.getTask();
            // Parse entity ID from the task name if needed; use a generic entity ID for the exception
            EntityOperationFailedException exception = new EntityOperationFailedException(
                    EntityInstanceId.fromString("@unknown@unknown"),
                    record.getTaskName(),
                    details);
            task.completeExceptionally(exception);
        }

        private void handleEntityLockRequested(HistoryEvent e) {
            int taskId = e.getEventId();
            OrchestratorAction taskAction = this.pendingActions.remove(taskId);
            if (taskAction == null) {
                String message = String.format(
                        "Non-deterministic orchestrator detected: a history event for entity lock request with sequence ID %d was replayed but the current orchestrator implementation didn't issue this lock request.",
                        taskId);
                throw new NonDeterministicOrchestratorException(message);
            }
        }

        @SuppressWarnings("unchecked")
        private void handleEntityLockGranted(HistoryEvent e) {
            EntityLockGrantedEvent lockGrantedEvent = e.getEntityLockGranted();
            String criticalSectionId = lockGrantedEvent.getCriticalSectionId();

            Queue<TaskRecord<?>> outstandingQueue = this.outstandingEvents.get(criticalSectionId);
            if (outstandingQueue == null) {
                this.logger.warning("Discarding entity lock granted event with criticalSectionId=" + criticalSectionId + ": no waiter found");
                return;
            }

            TaskRecord<?> record = outstandingQueue.remove();
            if (outstandingQueue.isEmpty()) {
                this.outstandingEvents.remove(criticalSectionId);
            }

            this.isInCriticalSection = true;
            this.currentCriticalSectionId = criticalSectionId;

            if (!this.isReplaying) {
                this.logger.fine(() -> String.format(
                        "%s: Entity lock granted for criticalSectionId=%s",
                        this.instanceId,
                        criticalSectionId));
            }

            CompletableTask task = record.getTask();
            task.complete(null); // The actual AutoCloseable is created via thenApply in lockEntities()
        }

        private void handleEntityUnlockSent(HistoryEvent e) {
            int taskId = e.getEventId();
            OrchestratorAction taskAction = this.pendingActions.remove(taskId);
            if (taskAction == null) {
                String message = String.format(
                        "Non-deterministic orchestrator detected: a history event for entity unlock with sequence ID %d was replayed but the current orchestrator implementation didn't issue this unlock.",
                        taskId);
                throw new NonDeterministicOrchestratorException(message);
            }
        }

        // endregion

        private void handleEventWhileSuspended (HistoryEvent historyEvent){
            if (historyEvent.getEventTypeCase() != HistoryEvent.EventTypeCase.EXECUTIONSUSPENDED) {
                eventsWhileSuspended.offer(historyEvent);
            }
        }

        private void handleExecutionSuspended(HistoryEvent historyEvent) {
            this.isSuspended = true;
        }

        private void handleExecutionResumed(HistoryEvent historyEvent) {
            this.isSuspended = false;
            while (!eventsWhileSuspended.isEmpty()) {
                this.processEvent(eventsWhileSuspended.poll());
            }
        }

        public Task<Void> createTimer(Duration duration) {
            Helpers.throwIfOrchestratorComplete(this.isComplete);
            Helpers.throwIfArgumentNull(duration, "duration");

            Instant finalFireAt = this.currentInstant.plus(duration);
            return createTimer(finalFireAt);
        }

        @Override
        public Task<Void> createTimer(ZonedDateTime zonedDateTime) {
            Helpers.throwIfOrchestratorComplete(this.isComplete);
            Helpers.throwIfArgumentNull(zonedDateTime, "zonedDateTime");

            Instant finalFireAt = zonedDateTime.toInstant();
            return createTimer(finalFireAt);
        }

        private Task<Void> createTimer(Instant finalFireAt) {
            TimerTask timer = new TimerTask(finalFireAt);
            return timer;
        }

        private CompletableTask<Void> createInstantTimer(int id, Instant fireAt) {
            Timestamp ts = DataConverter.getTimestampFromInstant(fireAt);
            this.pendingActions.put(id, OrchestratorAction.newBuilder()
                    .setId(id)
                    .setCreateTimer(CreateTimerAction.newBuilder().setFireAt(ts))
                    .build());

            if (!this.isReplaying) {
                // TODO: Log timer creation, including the expected fire-time
            }

            CompletableTask<Void> timerTask = new CompletableTask<>();
            TaskRecord<Void> record = new TaskRecord<>(timerTask, "(timer)", Void.class);
            this.openTasks.put(id, record);
            return timerTask;
        }

        private void handleTimerCreated(HistoryEvent e) {
            int timerEventId = e.getEventId();
            if (timerEventId == -100) {
                // Infrastructure timer used by the dispatcher to break transactions into multiple batches
                return;
            }

            TimerCreatedEvent timerCreatedEvent = e.getTimerCreated();

            // The history shows that this orchestrator created a durable timer in a previous execution.
            // We can therefore remove it from the map of pending actions. If we can't find the pending
            // action, then we assume a non-deterministic code violation in the orchestrator.
            OrchestratorAction timerAction = this.pendingActions.remove(timerEventId);
            if (timerAction == null) {
                String message = String.format(
                        "Non-deterministic orchestrator detected: a history event creating a timer with ID %d and fire-at time %s was replayed but the current orchestrator implementation didn't actually create this timer. Was a change made to the orchestrator code after this instance had already started running?",
                        timerEventId,
                        DataConverter.getInstantFromTimestamp(timerCreatedEvent.getFireAt()));
                throw new NonDeterministicOrchestratorException(message);
            }
        }

        public void handleTimerFired(HistoryEvent e) {
            TimerFiredEvent timerFiredEvent = e.getTimerFired();
            int timerEventId = timerFiredEvent.getTimerId();
            TaskRecord<?> record = this.openTasks.remove(timerEventId);
            if (record == null) {
                // TODO: Log a warning about a potential duplicate timer fired event
                return;
            }

            if (!this.isReplaying) {
                // TODO: Log timer fired, including the scheduled fire-time
            }

            CompletableTask<?> task = record.getTask();
            task.complete(null);
        }

        private void handleSubOrchestrationCreated(HistoryEvent e) {
            int taskId = e.getEventId();
            SubOrchestrationInstanceCreatedEvent subOrchestrationInstanceCreated = e.getSubOrchestrationInstanceCreated();
            OrchestratorAction taskAction = this.pendingActions.remove(taskId);
            if (taskAction == null) {
                String message = String.format(
                        "Non-deterministic orchestrator detected: a history event scheduling an sub-orchestration task with sequence ID %d and name '%s' was replayed but the current orchestrator implementation didn't actually schedule this task. Was a change made to the orchestrator code after this instance had already started running?",
                        taskId,
                        subOrchestrationInstanceCreated.getName());
                throw new NonDeterministicOrchestratorException(message);
            }
        }

        private void handleSubOrchestrationCompleted(HistoryEvent e) {
            SubOrchestrationInstanceCompletedEvent subOrchestrationInstanceCompletedEvent = e.getSubOrchestrationInstanceCompleted();
            int taskId = subOrchestrationInstanceCompletedEvent.getTaskScheduledId();
            TaskRecord<?> record = this.openTasks.remove(taskId);
            if (record == null) {
                this.logger.warning("Discarding a potentially duplicate SubOrchestrationInstanceCompleted event with ID = " + taskId);
                return;
            }
            String rawResult = subOrchestrationInstanceCompletedEvent.getResult().getValue();

            if (!this.isReplaying) {
                // TODO: Structured logging
                // TODO: Would it make more sense to put this log in the activity executor?
                this.logger.fine(() -> String.format(
                        "%s: Sub-orchestrator '%s' (#%d) completed with serialized output: %s",
                        this.instanceId,
                        record.getTaskName(),
                        taskId,
                        rawResult != null ? rawResult : "(null)"));

            }
            CompletableTask task = record.getTask();
            try {
                Object result = this.dataConverter.deserialize(rawResult, record.getDataType());
                task.complete(result);
            } catch (Exception ex) {
                task.completeExceptionally(ex);
            }
        }

        private void handleSubOrchestrationFailed(HistoryEvent e){
            SubOrchestrationInstanceFailedEvent subOrchestrationInstanceFailedEvent = e.getSubOrchestrationInstanceFailed();
            int taskId = subOrchestrationInstanceFailedEvent.getTaskScheduledId();
            TaskRecord<?> record = this.openTasks.remove(taskId);
            if (record == null) {
                // TODO: Log a warning about a potential duplicate task completion event
                return;
            }

            FailureDetails details = new FailureDetails(subOrchestrationInstanceFailedEvent.getFailureDetails());

            if (!this.isReplaying) {
                // TODO: Log task failure, including the number of bytes in the result
            }

            CompletableTask<?> task = record.getTask();
            TaskFailedException exception = new TaskFailedException(
                    record.taskName,
                    taskId,
                    details);
            task.completeExceptionally(exception);
        }

        private void handleExecutionTerminated(HistoryEvent e) {
            ExecutionTerminatedEvent executionTerminatedEvent = e.getExecutionTerminated();
            this.completeInternal(executionTerminatedEvent.getInput().getValue(), null, OrchestrationStatus.ORCHESTRATION_STATUS_TERMINATED);
        }

        @Override
        public void complete(Object output) {
            if (this.continuedAsNew) {
                this.completeInternal(this.continuedAsNewInput, OrchestrationStatus.ORCHESTRATION_STATUS_CONTINUED_AS_NEW);
            } else {
                this.completeInternal(output, OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED);
            }
        }

        public void fail(FailureDetails failureDetails) {
            // TODO: How does a parent orchestration use the output to construct an exception?
            this.completeInternal(null, failureDetails, OrchestrationStatus.ORCHESTRATION_STATUS_FAILED);
        }

        private void completeInternal(Object output, OrchestrationStatus runtimeStatus) {
            String resultAsJson = TaskOrchestrationExecutor.this.dataConverter.serialize(output);
            this.completeInternal(resultAsJson, null, runtimeStatus);
        }

        private void completeInternal(
                @Nullable String rawOutput,
                @Nullable FailureDetails failureDetails,
                OrchestrationStatus runtimeStatus) {
            Helpers.throwIfOrchestratorComplete(this.isComplete);

            int id = this.sequenceNumber++;
            CompleteOrchestrationAction.Builder builder = CompleteOrchestrationAction.newBuilder();
            builder.setOrchestrationStatus(runtimeStatus);

            if (rawOutput != null) {
                builder.setResult(StringValue.of(rawOutput));
            }

            if (failureDetails != null) {
                builder.setFailureDetails(failureDetails.toProto());
            }

            if (this.continuedAsNew && this.preserveUnprocessedEvents) {
                addCarryoverEvents(builder);
            }

            if (!this.isReplaying) {
                // TODO: Log completion, including the number of bytes in the output
            }

            OrchestratorAction action = OrchestratorAction.newBuilder()
                    .setId(id)
                    .setCompleteOrchestration(builder.build())
                    .build();
            this.pendingActions.put(id, action);
            this.isComplete = true;
        }

        private void addCarryoverEvents(CompleteOrchestrationAction.Builder builder) {
            // Add historyEvent in the unprocessedEvents buffer
            // Add historyEvent in the new event list that haven't been added to the buffer.
            // We don't check the event in the pass event list to avoid duplicated events.
            Set<HistoryEvent> externalEvents = new HashSet<>(this.unprocessedEvents);
            List<HistoryEvent> newEvents = this.historyEventPlayer.getNewEvents();
            int currentHistoryIndex = this.historyEventPlayer.getCurrentHistoryIndex();

            // Only add events that haven't been processed to the carryOverEvents
            // currentHistoryIndex will point to the first unprocessed event
            for (int i = currentHistoryIndex; i < newEvents.size(); i++) {
                HistoryEvent historyEvent = newEvents.get(i);
                if (historyEvent.getEventTypeCase() == HistoryEvent.EventTypeCase.EVENTRAISED) {
                    externalEvents.add(historyEvent);
                }
            }

            externalEvents.forEach(builder::addCarryoverEvents);
        }
        
        private boolean waitingForEvents() {
            return this.outstandingEvents.size() > 0;
        }

        private boolean processNextEvent() {
            return this.historyEventPlayer.moveNext();
        }

        private void processEvent(HistoryEvent e) {
            boolean overrideSuspension = e.getEventTypeCase() == HistoryEvent.EventTypeCase.EXECUTIONRESUMED || e.getEventTypeCase() == HistoryEvent.EventTypeCase.EXECUTIONTERMINATED;
            if (this.isSuspended && !overrideSuspension) {
                this.handleEventWhileSuspended(e);
            } else {
                switch (e.getEventTypeCase()) {
                    case ORCHESTRATORSTARTED:
                        Instant instant = DataConverter.getInstantFromTimestamp(e.getTimestamp());
                        this.setCurrentInstant(instant);
                        break;
                    case ORCHESTRATORCOMPLETED:
                        // No action
                        break;
                    case EXECUTIONSTARTED:
                        ExecutionStartedEvent startedEvent = e.getExecutionStarted();
                        String name = startedEvent.getName();
                        this.setName(name);
                        String instanceId = startedEvent.getOrchestrationInstance().getInstanceId();
                        this.setInstanceId(instanceId);
                        if (startedEvent.getOrchestrationInstance().hasExecutionId()) {
                            this.executionId = startedEvent.getOrchestrationInstance().getExecutionId().getValue();
                        }
                        String input = startedEvent.getInput().getValue();
                        this.setInput(input);
                        String version = startedEvent.getVersion().getValue();
                        this.setVersion(version);
                        TaskOrchestrationFactory factory = TaskOrchestrationExecutor.this.orchestrationFactories.get(name);
                        if (factory == null) {
                            // Try getting the default orchestrator
                            factory = TaskOrchestrationExecutor.this.orchestrationFactories.get("*");
                        }
                        if (factory == null) {
                            throw new IllegalStateException(String.format(
                                    "No orchestration factory registered for orchestration type '%s'. This usually means that a worker that doesn't support this orchestration type is connected to this task hub. Make sure the worker has a registered orchestration for '%s'.",
                                    name,
                                    name));
                        }
                        TaskOrchestration orchestrator = factory.create();
                        orchestrator.run(this);
                        break;
//                case EXECUTIONCOMPLETED:
//                    break;
//                case EXECUTIONFAILED:
//                    break;
                    case EXECUTIONTERMINATED:
                        this.handleExecutionTerminated(e);
                        break;
                    case TASKSCHEDULED:
                        this.handleTaskScheduled(e);
                        break;
                    case TASKCOMPLETED:
                        this.handleTaskCompleted(e);
                        break;
                    case TASKFAILED:
                        this.handleTaskFailed(e);
                        break;
                    case TIMERCREATED:
                        this.handleTimerCreated(e);
                        break;
                    case TIMERFIRED:
                        this.handleTimerFired(e);
                        break;
                    case SUBORCHESTRATIONINSTANCECREATED:
                        this.handleSubOrchestrationCreated(e);
                        break;
                    case SUBORCHESTRATIONINSTANCECOMPLETED:
                        this.handleSubOrchestrationCompleted(e);
                        break;
                    case SUBORCHESTRATIONINSTANCEFAILED:
                        this.handleSubOrchestrationFailed(e);
                        break;
                    case EVENTSENT:
                        // No action needed — sendEvent is fire-and-forget with no replay validation
                        break;
                    case EVENTRAISED:
                        this.handleEventRaised(e);
                        break;
//                case GENERICEVENT:
//                    break;
//                case HISTORYSTATE:
//                    break;
//                case EVENTTYPE_NOT_SET:
//                    break;
                    case EXECUTIONSUSPENDED:
                        this.handleExecutionSuspended(e);
                        break;
                    case EXECUTIONRESUMED:
                        this.handleExecutionResumed(e);
                        break;
                    // Entity event cases (Phase 4)
                    case ENTITYOPERATIONSIGNALED:
                        this.handleEntityOperationSignaled(e);
                        break;
                    case ENTITYOPERATIONCALLED:
                        this.handleEntityOperationCalled(e);
                        break;
                    case ENTITYOPERATIONCOMPLETED:
                        this.handleEntityOperationCompleted(e);
                        break;
                    case ENTITYOPERATIONFAILED:
                        this.handleEntityOperationFailed(e);
                        break;
                    case ENTITYLOCKREQUESTED:
                        this.handleEntityLockRequested(e);
                        break;
                    case ENTITYLOCKGRANTED:
                        this.handleEntityLockGranted(e);
                        break;
                    case ENTITYUNLOCKSENT:
                        this.handleEntityUnlockSent(e);
                        break;
                    default:
                        throw new IllegalStateException("Don't know how to handle history type " + e.getEventTypeCase());
                }
            }
        }

        private class TaskRecord<V> {
            private final CompletableTask<V> task;
            private final String taskName;
            private final Class<V> dataType;

            public TaskRecord(CompletableTask<V> task, String taskName, Class<V> dataType) {
                this.task = task;
                this.taskName = taskName;
                this.dataType = dataType;
            }

            public CompletableTask<V> getTask() {
                return this.task;
            }

            public String getTaskName() {
                return this.taskName;
            }

            public Class<V> getDataType() {
                return this.dataType;
            }
        }

        private class OrchestrationHistoryIterator {
            private final List<HistoryEvent> pastEvents;
            private final List<HistoryEvent> newEvents;

            private List<HistoryEvent> currentHistoryList;
            private int currentHistoryIndex;

            public OrchestrationHistoryIterator(List<HistoryEvent> pastEvents, List<HistoryEvent> newEvents) {
                this.pastEvents = pastEvents;
                this.newEvents = newEvents;
                this.currentHistoryList = pastEvents;
            }

            public boolean moveNext() {
                if (this.currentHistoryList == pastEvents && this.currentHistoryIndex >= pastEvents.size()) {
                    // Move forward to the next list
                    this.currentHistoryList = this.newEvents;
                    this.currentHistoryIndex = 0;

                    ContextImplTask.this.setDoneReplaying();
                }

                if (this.currentHistoryList == this.newEvents && this.currentHistoryIndex >= this.newEvents.size()) {
                    // We're done enumerating the history
                    return false;
                }

                // Process the next event in the history
                HistoryEvent next = this.currentHistoryList.get(this.currentHistoryIndex++);
                ContextImplTask.this.processEvent(next);
                return true;
            }

            List<HistoryEvent> getNewEvents() {
                return this.newEvents;
            }

            int getCurrentHistoryIndex() {
                return this.currentHistoryIndex;
            }
        }

        private class TimerTask extends CompletableTask<Void> {
            private Instant finalFireAt;
            CompletableTask<Void> task;

            public TimerTask(Instant finalFireAt) {
                super();
                CompletableTask<Void> firstTimer = createTimerTask(finalFireAt);
                CompletableFuture<Void> timerChain = createTimerChain(finalFireAt, firstTimer.future);
                this.task = new CompletableTask<>(timerChain);
                this.finalFireAt = finalFireAt;
            }

            // For a short timer (less than maximumTimerInterval), once the currentFuture completes, we must have reached finalFireAt,
            // so we return and no more sub-timers are created. For a long timer (more than maximumTimerInterval), once a given
            // currentFuture completes, we check if we have not yet reached finalFireAt. If that is the case, we create a new sub-timer
            // task and make a recursive call on that new sub-timer task so that once it completes, another sub-timer task is created
            // if necessary. Otherwise, we return and no more sub-timers are created.
            private CompletableFuture<Void> createTimerChain(Instant finalFireAt, CompletableFuture<Void> currentFuture) {
                return currentFuture.thenRun(() -> {
                    if (currentInstant.compareTo(finalFireAt) > 0) {
                        return;
                    }
                    Task<Void> nextTimer = createTimerTask(finalFireAt);

                    createTimerChain(finalFireAt, nextTimer.future);
                });
            }

            private CompletableTask<Void> createTimerTask(Instant finalFireAt) {
                CompletableTask<Void> nextTimer;
                Duration remainingTime = Duration.between(currentInstant, finalFireAt);
                if (remainingTime.compareTo(maximumTimerInterval) > 0) {
                    Instant nextFireAt = currentInstant.plus(maximumTimerInterval);
                    nextTimer = createInstantTimer(sequenceNumber++, nextFireAt);
                } else {
                    nextTimer = createInstantTimer(sequenceNumber++, finalFireAt);
                }
                nextTimer.setParentTask(this);
                return nextTimer;
            }

            private void handleSubTimerSuccess() {
                // check if it is the last timer
                if (currentInstant.compareTo(finalFireAt) >= 0) {
                    this.complete(null);
                }
            }

            @Override
            public Void await() {
                return this.task.await();
            }

        }

        private class ExternalEventTask<V> extends CompletableTask<V> {
            private final String eventName;
            private final Duration timeout;
            private final int taskId;

            public ExternalEventTask(String eventName, int taskId, Duration timeout) {
                this.eventName = eventName;
                this.taskId = taskId;
                this.timeout = timeout;
            }

            // TODO: Shouldn't this be throws TaskCanceledException?
            @Override
            protected void handleException(Throwable e) {
                // Cancellation is caused by user-specified timeouts
                if (e instanceof CancellationException) {
                    String message = String.format(
                            "Timeout of %s expired while waiting for an event named '%s' (ID = %d).",
                            this.timeout,
                            this.eventName,
                            this.taskId);
                    throw new TaskCanceledException(message, this.eventName, this.taskId);
                }

                super.handleException(e);
            }
        }

        // Task implementation that implements a retry policy
        private class RetriableTask<V> extends CompletableTask<V> {
            private final RetryPolicy policy;
            private final RetryHandler handler;
            private final TaskOrchestrationContext context;
            private final Instant firstAttempt;
            private final TaskFactory<V> taskFactory;

            private FailureDetails lastFailure;
            private Duration totalRetryTime;
            private Instant startTime;
            private int attemptNumber;
            private Task<V> childTask;


            public RetriableTask(TaskOrchestrationContext context, TaskFactory<V> taskFactory, RetryPolicy policy) {
                this(context, taskFactory, policy, null);
            }

            public RetriableTask(TaskOrchestrationContext context, TaskFactory<V> taskFactory, RetryHandler handler) {
                this(context, taskFactory, null, handler);
            }

            private RetriableTask(
                    TaskOrchestrationContext context,
                    TaskFactory<V> taskFactory,
                    @Nullable RetryPolicy retryPolicy,
                    @Nullable RetryHandler retryHandler) {
                this.context = context;
                this.taskFactory = taskFactory;
                this.policy = retryPolicy;
                this.handler = retryHandler;
                this.firstAttempt = context.getCurrentInstant();
                this.totalRetryTime = Duration.ZERO;
                this.createChildTask(taskFactory);
            }

            // Every RetriableTask will have a CompletableTask as a child task.
            private void createChildTask(TaskFactory<V> taskFactory) {
                CompletableTask<V> childTask = (CompletableTask<V>) taskFactory.create();
                this.setChildTask(childTask);
                childTask.setParentTask(this);
            }

            public void setChildTask(Task<V> childTask) {
                this.childTask = childTask;
            }

            public Task<V> getChildTask() {
                return this.childTask;
            }

            void handleChildSuccess(V result) {
                this.complete(result);
            }

            void handleChildException(Throwable ex) {
                tryRetry((TaskFailedException) ex);
            }

            void init() {
                this.startTime = this.startTime == null ? this.context.getCurrentInstant() : this.startTime;
                this.attemptNumber++;
            }

            public void tryRetry(TaskFailedException ex) {
                this.lastFailure = ex.getErrorDetails();
                if (!this.shouldRetry()) {
                    this.completeExceptionally(ex);
                    return;
                }

                // Overflow/runaway retry protection
                if (this.attemptNumber == Integer.MAX_VALUE) {
                    this.completeExceptionally(ex);
                    return;
                }

                Duration delay = this.getNextDelay();
                if (!delay.isZero() && !delay.isNegative()) {
                    // Use a durable timer to create the delay between retries
                    this.context.createTimer(delay).await();
                }

                this.totalRetryTime = Duration.between(this.startTime, this.context.getCurrentInstant());
                this.createChildTask(this.taskFactory);
                this.await();
            }

            @Override
            public V await() {
                this.init();
                // when awaiting the first child task, we will continue iterating over the history until a result is found
                // for that task. If the result is an exception, the child task will invoke "handleChildException" on this
                // object, which awaits a timer, *re-sets the current child task to correspond to a retry of this task*,
                // and then awaits that child.
                // This logic continues until either the operation succeeds, or are our retry quota is met.
                // At that point, we break the `await()` on the child task.
                // Therefore, once we return from the following `await`,
                // we just need to await again on the *current* child task to obtain the result of this task
                try{
                    this.getChildTask().await();
                } catch (OrchestratorBlockedException ex) {
                    throw ex;
                } catch (Exception ignored) {
                    // ignore the exception from previous child tasks.
                    // Only needs to return result from the last child task, which is on next line.
                }
                // Always return the last child task result.
                return this.getChildTask().await();
            }

            private boolean shouldRetry() {
                if (this.lastFailure.isNonRetriable()) {
                     return false;
                }

                if (this.policy != null) {
                    return this.shouldRetryBasedOnPolicy();
                } else if (this.handler != null) {
                    RetryContext retryContext = new RetryContext(
                            this.context,
                            this.attemptNumber,
                            this.lastFailure,
                            this.totalRetryTime);
                    return this.handler.handle(retryContext);
                } else {
                    // We should never get here, but if we do, returning false is the natural behavior.
                    return false;
                }
            }

            private boolean shouldRetryBasedOnPolicy() {
                if (this.attemptNumber >= this.policy.getMaxNumberOfAttempts()) {
                    // Max number of attempts exceeded
                    return false;
                }

                // Duration.ZERO is interpreted as no maximum timeout
                Duration retryTimeout = this.policy.getRetryTimeout();
                if (retryTimeout.compareTo(Duration.ZERO) > 0) {
                    Instant retryExpiration = this.firstAttempt.plus(retryTimeout);
                    if (this.context.getCurrentInstant().compareTo(retryExpiration) >= 0) {
                        // Max retry timeout exceeded
                        return false;
                    }
                }

                // Keep retrying
                return true;
            }

            private Duration getNextDelay() {
                if (this.policy != null) {
                    long maxDelayInMillis = this.policy.getMaxRetryInterval().toMillis();

                    long nextDelayInMillis;
                    try {
                        nextDelayInMillis = Math.multiplyExact(
                                this.policy.getFirstRetryInterval().toMillis(),
                                (long)Helpers.powExact(this.policy.getBackoffCoefficient(), this.attemptNumber));
                    } catch (ArithmeticException overflowException) {
                        if (maxDelayInMillis > 0) {
                            return this.policy.getMaxRetryInterval();
                        } else {
                            // If no maximum is specified, just throw
                            throw new ArithmeticException("The retry policy calculation resulted in an arithmetic overflow and no max retry interval was configured.");
                        }
                    }

                    // NOTE: A max delay of zero or less is interpreted to mean no max delay
                    if (nextDelayInMillis > maxDelayInMillis && maxDelayInMillis > 0) {
                        return this.policy.getMaxRetryInterval();
                    } else {
                        return Duration.ofMillis(nextDelayInMillis);
                    }
                }

                // If there's no declarative retry policy defined, then the custom code retry handler
                // is responsible for implementing any delays between retry attempts.
                return Duration.ZERO;
            }
        }

        private class CompoundTask<V, U> extends CompletableTask<U> {

            List<Task<V>> subTasks;

            CompoundTask(List<Task<V>> subtasks, CompletableFuture<U> future) {
                super(future);
                this.subTasks = subtasks;
            }

            @Override
            public U await() {
                this.initSubTasks();
                return super.await();
            }

            private void initSubTasks() {
                for (Task<V> subTask : this.subTasks) {
                    if (subTask instanceof RetriableTask) ((RetriableTask<V>)subTask).init();
                }
            }
        }

        private class CompletableTask<V> extends Task<V> {
            private Task<V> parentTask;

            public CompletableTask() {
                this(new CompletableFuture<>());
            }

            CompletableTask(CompletableFuture<V> future) {
                super(future);
            }

            public void setParentTask(Task<V> parentTask) {
                this.parentTask = parentTask;
            }

            public Task<V> getParentTask() {
                return this.parentTask;
            }

            @Override
            public V await() {
                do {
                    // If the future is done, return its value right away
                    if (this.future.isDone()) {
                        try {
                            return this.future.get();
                        } catch (ExecutionException e) {
                            // rethrow if it's ContinueAsNewInterruption
                            if (e.getCause() instanceof ContinueAsNewInterruption) {
                                throw (ContinueAsNewInterruption) e.getCause();
                            }
                            this.handleException(e.getCause());
                        } catch (Exception e) {
                            this.handleException(e);
                        }
                    }
                } while (processNextEvent());

                // There's no more history left to replay and the current task is still not completed. This is normal.
                // The OrchestratorBlockedException exception allows us to yield the current thread back to the executor so
                // that we can send the current set of actions back to the worker and wait for new events to come in.
                // This is *not* an exception - it's a normal part of orchestrator control flow.
                throw new OrchestratorBlockedException(
                        "The orchestrator is blocked and waiting for new inputs. This Throwable should never be caught by user code.");
            }

            private boolean processNextEvent() {
                try {
                    return ContextImplTask.this.processNextEvent();
                } catch (OrchestratorBlockedException | ContinueAsNewInterruption exception) {
                    throw exception;
                } catch (Exception e) {
                    // ignore
                    /**
                     * We ignore the exception. Any Durable Task exceptions thrown here can be obtained when calling
                     * {code#future.get()} in the implementation of 'await'. We defer to that loop to handle the exception.
                     */
                }
                // Any exception happen we return true so that we will enter to the do-while block for the last time.
                return true;
            }

            @Override
            public <U> CompletableTask<U> thenApply(Function<V, U> fn) {
                CompletableFuture<U> newFuture = this.future.thenApply(fn);
                return new CompletableTask<>(newFuture);
            }

            @Override
            public Task<Void> thenAccept(Consumer<V> fn) {
                CompletableFuture<Void> newFuture = this.future.thenAccept(fn);
                return new CompletableTask<>(newFuture);
            }

            protected void handleException(Throwable e) {
                if (e instanceof TaskFailedException) {
                    throw (TaskFailedException)e;
                }

                if (e instanceof CompositeTaskFailedException) {
                    throw (CompositeTaskFailedException)e;
                }

                if (e instanceof DataConverter.DataConverterException) {
                    throw (DataConverter.DataConverterException)e;
                }

                if (e instanceof EntityOperationFailedException) {
                    throw (EntityOperationFailedException)e;
                }

                throw new RuntimeException("Unexpected failure in the task execution", e);
            }

            @Override
            public boolean isDone() {
                return this.future.isDone();
            }

            public boolean complete(V value) {
                Task<V> parentTask = this.getParentTask();
                boolean result = this.future.complete(value);
                if (parentTask instanceof RetriableTask) {
                    // notify parent task
                    ((RetriableTask<V>) parentTask).handleChildSuccess(value);
                }
                if (parentTask instanceof TimerTask) {
                    // notify parent task
                    ((TimerTask) parentTask).handleSubTimerSuccess();
                }
                return result;
            }

            private boolean cancel() {
                return this.future.cancel(true);
            }

            public boolean completeExceptionally(Throwable ex) {
                Task<V> parentTask = this.getParentTask();
                boolean result = this.future.completeExceptionally(ex);
                if (parentTask instanceof RetriableTask) {
                    // notify parent task
                    ((RetriableTask<V>) parentTask).handleChildException(ex);
                }
                return result;
            }
        }
    }

    @FunctionalInterface
    private interface TaskFactory<V> {
        Task<V> create();
    }
}
