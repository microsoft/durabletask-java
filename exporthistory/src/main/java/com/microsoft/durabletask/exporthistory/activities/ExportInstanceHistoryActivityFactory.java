// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.activities;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.TaskActivity;
import com.microsoft.durabletask.TaskActivityFactory;
import com.microsoft.durabletask.exporthistory.options.ExportHistoryStorageOptions;

/**
 * Factory for {@link ExportInstanceHistoryActivity}.
 * <p>
 * Captures {@link ExportHistoryStorageOptions} at registration time and injects
 * it into each activity instance (Java equivalent of .NET's {@code IOptions<T>}).
 */
public class ExportInstanceHistoryActivityFactory implements TaskActivityFactory {

    private final DurableTaskClient client;
    private final ExportHistoryStorageOptions storageOptions;

    public ExportInstanceHistoryActivityFactory(DurableTaskClient client, ExportHistoryStorageOptions storageOptions) {
        this.client = client;
        this.storageOptions = storageOptions;
    }

    @Override
    public String getName() {
        return "ExportInstanceHistoryActivity";
    }

    @Override
    public TaskActivity create() {
        return new ExportInstanceHistoryActivity(this.client, this.storageOptions);
    }
}
