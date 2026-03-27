// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import io.grpc.Channel;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Builder object for constructing customized {@link DurableTaskGrpcWorker} instances.
 */
public final class DurableTaskGrpcWorkerBuilder {
    final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
    final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();
    int port;
    Channel channel;
    DataConverter dataConverter;
    Duration maximumTimerInterval;
    DurableTaskGrpcWorkerVersioningOptions versioningOptions;
    private WorkItemFilter workItemFilter;
    private boolean autoGenerateWorkItemFilters;

    /**
     * Adds an orchestration factory to be used by the constructed {@link DurableTaskGrpcWorker}.
     *
     * @param factory an orchestration factory to be used by the constructed {@link DurableTaskGrpcWorker}
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder addOrchestration(TaskOrchestrationFactory factory) {
        String key = factory.getName();
        if (key == null || key.length() == 0) {
            throw new IllegalArgumentException("A non-empty task orchestration name is required.");
        }

        if (this.orchestrationFactories.containsKey(key)) {
            throw new IllegalArgumentException(
                    String.format("A task orchestration factory named %s is already registered.", key));
        }

        this.orchestrationFactories.put(key, factory);
        return this;
    }

    /**
     * Adds an activity factory to be used by the constructed {@link DurableTaskGrpcWorker}.
     *
     * @param factory an activity factory to be used by the constructed {@link DurableTaskGrpcWorker}
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder addActivity(TaskActivityFactory factory) {
        // TODO: Input validation
        String key = factory.getName();
        if (key == null || key.length() == 0) {
            throw new IllegalArgumentException("A non-empty task activity name is required.");
        }

        if (this.activityFactories.containsKey(key)) {
            throw new IllegalArgumentException(
                    String.format("A task activity factory named %s is already registered.", key));
        }

        this.activityFactories.put(key, factory);
        return this;
    }

    /**
     * Sets the gRPC channel to use for communicating with the sidecar process.
     * <p>
     * This builder method allows you to provide your own gRPC channel for communicating with the Durable Task sidecar
     * endpoint. Channels provided using this method won't be closed when the worker is closed.
     * Rather, the caller remains responsible for shutting down the channel after disposing the worker.
     * <p>
     * If not specified, a gRPC channel will be created automatically for each constructed
     * {@link DurableTaskGrpcWorker}.
     *
     * @param channel the gRPC channel to use
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder grpcChannel(Channel channel) {
        this.channel = channel;
        return this;
    }

    /**
     * Sets the gRPC endpoint port to connect to. If not specified, the default Durable Task port number will be used.
     *
     * @param port the gRPC endpoint port to connect to
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder port(int port) {
        this.port = port;
        return this;
    }

    /**
     * Sets the {@link DataConverter} to use for converting serializable data payloads.
     *
     * @param dataConverter the {@link DataConverter} to use for converting serializable data payloads
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder dataConverter(DataConverter dataConverter) {
        this.dataConverter = dataConverter;
        return this;
    }

    /**
     * Sets the maximum timer interval. If not specified, the default maximum timer interval duration will be used.
     * The default maximum timer interval duration is 3 days.
     *
     * @param maximumTimerInterval the maximum timer interval
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder maximumTimerInterval(Duration maximumTimerInterval) {
        this.maximumTimerInterval = maximumTimerInterval;
        return this;
    }

    /**
     * Sets the versioning options for this worker.
     * 
     * @param options the versioning options to use
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder useVersioning(DurableTaskGrpcWorkerVersioningOptions options) {
        this.versioningOptions = options;
        return this;
    }

    /**
     * Sets explicit work item filters for this worker. When set, only work items matching the filters
     * will be dispatched to this worker by the backend.
     * <p>
     * Work item filtering can improve efficiency in multi-worker deployments by ensuring each worker
     * only receives work items it can handle. However, if an orchestration calls a task type
     * (e.g., an activity or sub-orchestrator) that is not registered with any connected worker,
     * the call may hang indefinitely instead of failing with an error.
     *
     * @param workItemFilter the work item filter to use, or {@code null} to disable filtering
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder useWorkItemFilters(WorkItemFilter workItemFilter) {
        this.workItemFilter = workItemFilter;
        this.autoGenerateWorkItemFilters = false;
        return this;
    }

    /**
     * Enables automatic work item filtering by generating filters from the registered
     * orchestrations and activities. When enabled, the backend will only dispatch work items
     * for registered orchestrations and activities to this worker.
     * <p>
     * Work item filtering can improve efficiency in multi-worker deployments by ensuring each worker
     * only receives work items it can handle. However, if an orchestration calls a task type
     * (e.g., an activity or sub-orchestrator) that is not registered with any connected worker,
     * the call may hang indefinitely instead of failing with an error.
     * <p>
     * Only use this method when all task types referenced by orchestrations are guaranteed to be
     * registered with at least one connected worker.
     *
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder useWorkItemFilters() {
        this.autoGenerateWorkItemFilters = true;
        this.workItemFilter = null;
        return this;
    }

    /**
     * Initializes a new {@link DurableTaskGrpcWorker} object with the settings specified in the current builder object.
     * @return a new {@link DurableTaskGrpcWorker} object
     */
    public DurableTaskGrpcWorker build() {
        WorkItemFilter resolvedFilter = this.autoGenerateWorkItemFilters
                ? buildAutoWorkItemFilter()
                : this.workItemFilter;
        return new DurableTaskGrpcWorker(this, resolvedFilter);
    }

    private WorkItemFilter buildAutoWorkItemFilter() {
        List<String> versions = Collections.emptyList();
        if (this.versioningOptions != null
                && this.versioningOptions.getMatchStrategy() == DurableTaskGrpcWorkerVersioningOptions.VersionMatchStrategy.STRICT
                && this.versioningOptions.getVersion() != null) {
            versions = Collections.singletonList(this.versioningOptions.getVersion());
        }

        WorkItemFilter.Builder builder = WorkItemFilter.newBuilder();
        for (String name : this.orchestrationFactories.keySet()) {
            builder.addOrchestration(name, versions);
        }
        for (String name : this.activityFactories.keySet()) {
            builder.addActivity(name, versions);
        }
        return builder.build();
    }
}
