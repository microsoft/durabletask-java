// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.activities;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.InstanceIdPage;
import com.microsoft.durabletask.TaskActivity;
import com.microsoft.durabletask.TaskActivityContext;
import com.microsoft.durabletask.exporthistory.models.ExportCheckpoint;
import com.microsoft.durabletask.exporthistory.models.InstancePage;
import com.microsoft.durabletask.exporthistory.models.ListTerminalInstancesRequest;

import java.util.logging.Logger;

/**
 * Activity that lists terminal orchestration instances matching the export filter criteria.
 */
public class ListTerminalInstancesActivity implements TaskActivity {

    private static final Logger logger = Logger.getLogger(ListTerminalInstancesActivity.class.getName());

    private final DurableTaskClient client;

    public ListTerminalInstancesActivity(DurableTaskClient client) {
        this.client = client;
    }

    @Override
    public Object run(TaskActivityContext ctx) {
        ListTerminalInstancesRequest input = ctx.getInput(ListTerminalInstancesRequest.class);
        if (input == null) {
            throw new IllegalArgumentException("ListTerminalInstancesRequest input is required.");
        }

        InstanceIdPage page = this.client.listInstanceIds(
                input.getRuntimeStatus(),
                input.getCompletedTimeFrom(),
                input.getCompletedTimeTo(),
                input.getMaxInstancesPerBatch(),
                input.getLastInstanceKey());

        logger.info("ListTerminalInstancesActivity returned " + page.getInstanceIds().size() + " instance IDs.");

        return new InstancePage(
                page.getInstanceIds(),
                new ExportCheckpoint(page.getContinuationToken()));
    }
}
