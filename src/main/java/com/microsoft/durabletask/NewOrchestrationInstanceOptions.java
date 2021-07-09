// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.time.Instant;

public class NewOrchestrationInstanceOptions {
    private final String version;
    private final String instanceId;
    private final Object input;
    private final Instant startTime;

    private NewOrchestrationInstanceOptions(Builder builder) {
        this.version = builder.version;
        this.instanceId = builder.instanceId;
        this.input = builder.input;
        this.startTime = builder.startTime;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getVersion() {
        return this.version;
    }

    public String getInstanceId() {
        return this.instanceId;
    }

    public Object getInput() {
        return this.input;
    }

    public Instant getStartTime() {
        return this.startTime;
    }

    public static class Builder {
        private String version;
        private String instanceId;
        private Object input;
        private Instant startTime;

        public Builder setVersion(String version) {
            this.version = version;
            return this;
        }

        public Builder setInstanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder setInput(Object input) {
            this.input = input;
            return this;
        }

        public Builder setStartTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public NewOrchestrationInstanceOptions build() {
            return new NewOrchestrationInstanceOptions(this);
        }
    }
}
