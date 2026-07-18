// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.azure.core.credential.TokenCredential;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.TaskEntity;
import com.microsoft.durabletask.TaskEntityFactory;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link ExportHistoryWorkerExtensions}. */
class ExportHistoryWorkerExtensionsTest {

    @Test
    @SuppressWarnings("unchecked")
    void registeredExportJobFactoryCreatesPackagePrivateEntity() throws Exception {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        ExportHistoryStorageOptions storage = new ExportHistoryStorageOptions()
                .setAccountUri(URI.create("https://example.blob.core.windows.net"))
                .setCredential(mock(TokenCredential.class));

        ExportHistoryWorkerExtensions.useExportHistory(builder, storage, mock(DurableTaskClient.class));

        Field factoriesField = DurableTaskGrpcWorkerBuilder.class.getDeclaredField("entityFactories");
        factoriesField.setAccessible(true);
        Map<String, TaskEntityFactory> factories =
                (Map<String, TaskEntityFactory>) factoriesField.get(builder);
        TaskEntityFactory factory = factories.get(ExportJob.NAME.toLowerCase(java.util.Locale.ROOT));

        assertNotNull(factory);
        assertInstanceOf(ExportJob.class, factory.create());
    }
}