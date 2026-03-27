// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents work item filters for a Durable Task worker. These filters are passed to the backend
 * and only work items matching the filters will be processed by the worker. If no filters are provided,
 * the worker will process all work items.
 * <p>
 * Work item filtering can improve efficiency in multi-worker deployments by ensuring each worker
 * only receives work items it can handle. However, if an orchestration calls a task type
 * (e.g., an activity or sub-orchestrator) that is not registered with any connected worker,
 * the call may hang indefinitely instead of failing with an error.
 * <p>
 * Use {@link DurableTaskGrpcWorkerBuilder#useWorkItemFilters(WorkItemFilter)} to provide explicit filters,
 * or {@link DurableTaskGrpcWorkerBuilder#useWorkItemFilters()} to auto-generate filters from the
 * registered orchestrations and activities.
 */
public final class WorkItemFilter {

    private final List<OrchestrationFilter> orchestrations;
    private final List<ActivityFilter> activities;

    private WorkItemFilter(List<OrchestrationFilter> orchestrations, List<ActivityFilter> activities) {
        this.orchestrations = Collections.unmodifiableList(new ArrayList<OrchestrationFilter>(orchestrations));
        this.activities = Collections.unmodifiableList(new ArrayList<ActivityFilter>(activities));
    }

    /**
     * Gets the orchestration filters.
     *
     * @return an unmodifiable list of orchestration filters
     */
    public List<OrchestrationFilter> getOrchestrations() {
        return this.orchestrations;
    }

    /**
     * Gets the activity filters.
     *
     * @return an unmodifiable list of activity filters
     */
    public List<ActivityFilter> getActivities() {
        return this.activities;
    }

    /**
     * Creates a new {@link Builder} for constructing {@link WorkItemFilter} instances.
     *
     * @return a new builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for constructing {@link WorkItemFilter} instances.
     */
    public static final class Builder {
        private final List<OrchestrationFilter> orchestrations = new ArrayList<OrchestrationFilter>();
        private final List<ActivityFilter> activities = new ArrayList<ActivityFilter>();

        Builder() {
        }

        /**
         * Adds an orchestration filter with the specified name and no version constraint.
         *
         * @param name the orchestration name to filter on
         * @return this builder
         */
        public Builder addOrchestration(String name) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Orchestration filter name must not be null or empty.");
            }
            this.orchestrations.add(new OrchestrationFilter(name, Collections.<String>emptyList()));
            return this;
        }

        /**
         * Adds an orchestration filter with the specified name and versions.
         *
         * @param name the orchestration name to filter on
         * @param versions the versions to filter on
         * @return this builder
         */
        public Builder addOrchestration(String name, List<String> versions) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Orchestration filter name must not be null or empty.");
            }
            List<String> versionsCopy = versions != null
                    ? Collections.unmodifiableList(new ArrayList<String>(versions))
                    : Collections.<String>emptyList();
            this.orchestrations.add(new OrchestrationFilter(name, versionsCopy));
            return this;
        }

        /**
         * Adds an activity filter with the specified name and no version constraint.
         *
         * @param name the activity name to filter on
         * @return this builder
         */
        public Builder addActivity(String name) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Activity filter name must not be null or empty.");
            }
            this.activities.add(new ActivityFilter(name, Collections.<String>emptyList()));
            return this;
        }

        /**
         * Adds an activity filter with the specified name and versions.
         *
         * @param name the activity name to filter on
         * @param versions the versions to filter on
         * @return this builder
         */
        public Builder addActivity(String name, List<String> versions) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Activity filter name must not be null or empty.");
            }
            List<String> versionsCopy = versions != null
                    ? Collections.unmodifiableList(new ArrayList<String>(versions))
                    : Collections.<String>emptyList();
            this.activities.add(new ActivityFilter(name, versionsCopy));
            return this;
        }

        /**
         * Builds a new {@link WorkItemFilter} from the configured filters.
         *
         * @return a new {@link WorkItemFilter} instance
         */
        public WorkItemFilter build() {
            return new WorkItemFilter(this.orchestrations, this.activities);
        }
    }

    /**
     * Specifies an orchestration filter with a name and optional versions.
     */
    public static final class OrchestrationFilter {
        private final String name;
        private final List<String> versions;

        OrchestrationFilter(String name, List<String> versions) {
            this.name = name;
            this.versions = versions;
        }

        /**
         * Gets the name of the orchestration to filter.
         *
         * @return the orchestration name
         */
        public String getName() {
            return this.name;
        }

        /**
         * Gets the versions of the orchestration to filter.
         *
         * @return an unmodifiable list of versions, or an empty list if no version constraint
         */
        public List<String> getVersions() {
            return this.versions;
        }
    }

    /**
     * Specifies an activity filter with a name and optional versions.
     */
    public static final class ActivityFilter {
        private final String name;
        private final List<String> versions;

        ActivityFilter(String name, List<String> versions) {
            this.name = name;
            this.versions = versions;
        }

        /**
         * Gets the name of the activity to filter.
         *
         * @return the activity name
         */
        public String getName() {
            return this.name;
        }

        /**
         * Gets the versions of the activity to filter.
         *
         * @return an unmodifiable list of versions, or an empty list if no version constraint
         */
        public List<String> getVersions() {
            return this.versions;
        }
    }
}
