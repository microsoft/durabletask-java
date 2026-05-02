// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.activities;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.TaskActivity;
import com.microsoft.durabletask.TaskActivityFactory;

/**
 * Factory for {@link ListTerminalInstancesActivity}.
 * <p>
 * This activity only needs the {@link DurableTaskClient} (no storage options),
 * since it calls the gRPC {@code ListInstanceIds} RPC.
 */
public class ListTerminalInstancesActivityFactory implements TaskActivityFactory {

    private final DurableTaskClient client;

    public ListTerminalInstancesActivityFactory(DurableTaskClient client) {
        this.client = client;
    }

    @Override
    public String getName() {
        return "ListTerminalInstancesActivity";
    }

    @Override
    public TaskActivity create() {
        return new ListTerminalInstancesActivity(this.client);
    }
}
