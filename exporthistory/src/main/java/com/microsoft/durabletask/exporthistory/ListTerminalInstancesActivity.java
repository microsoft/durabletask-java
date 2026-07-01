// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.ListInstanceIdsQuery;
import com.microsoft.durabletask.ListInstanceIdsResult;
import com.microsoft.durabletask.TaskActivity;
import com.microsoft.durabletask.TaskActivityContext;

import java.util.ArrayList;

/**
 * Activity that lists terminal orchestration instances for a completion-time window using the client
 * {@code listInstanceIds} wrapper, returning a page plus the checkpoint to advance to.
 */
public final class ListTerminalInstancesActivity implements TaskActivity {

    /** The registered activity name. */
    public static final String NAME = "ListTerminalInstancesActivity";

    private final DurableTaskClient client;

    /**
     * Creates a {@code ListTerminalInstancesActivity}.
     *
     * @param client the Durable Task client used to list instances
     */
    public ListTerminalInstancesActivity(DurableTaskClient client) {
        this.client = client;
    }

    @Override
    public Object run(TaskActivityContext ctx) {
        ListTerminalInstancesRequest input = ctx.getInput(ListTerminalInstancesRequest.class);

        ListInstanceIdsQuery query = new ListInstanceIdsQuery()
                .setCompletedTimeFrom(input.getCompletedTimeFrom())
                .setCompletedTimeTo(input.getCompletedTimeTo())
                .setRuntimeStatusList(input.getRuntimeStatus())
                .setPageSize(input.getMaxInstancesPerBatch())
                .setContinuationToken(input.getLastInstanceKey());

        ListInstanceIdsResult result = this.client.listInstanceIds(query);
        return new InstancePage(
                new ArrayList<>(result.getInstanceIds()),
                new ExportCheckpoint(result.getContinuationToken()));
    }
}
