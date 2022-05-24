// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.models;

import java.time.Instant;

public final class NewOrchestrationInstanceOptions {
    private String version;
    private String instanceId;
    private Object input;
    private Instant startTime;

    public NewOrchestrationInstanceOptions() {
    }

    public NewOrchestrationInstanceOptions setVersion(String version) {
        this.version = version;
        return this;
    }

    public NewOrchestrationInstanceOptions setInstanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    public NewOrchestrationInstanceOptions setInput(Object input) {
        this.input = input;
        return this;
    }

    public NewOrchestrationInstanceOptions setStartTime(Instant startTime) {
        this.startTime = startTime;
        return this;
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
}
