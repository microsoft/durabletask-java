// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.exporthistory.client.ExportHistoryWorkers;
import com.microsoft.durabletask.exporthistory.options.ExportHistoryStorageOptions;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Export History sample — Spring Boot web app with REST endpoints for managing export jobs.
 * <p>
 * Java equivalent of the .NET ExportHistoryWebApp sample.
 * <p>
 * Run: ./gradlew runExportHistorySample
 * API: http://localhost:8080/export-jobs
 * <p>
 * Requires DTS emulator and Azurite (or Azure Storage).
 */
@SpringBootApplication
public class ExportHistorySample {

    // Shared instances — set in main(), read by controller
    static DurableTaskClient client;
    static ExportHistoryStorageOptions storageOptions;

    public static void main(String[] args) {
        // Config
        String connStr = env("EXPORT_HISTORY_STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true");
        String container = env("EXPORT_HISTORY_CONTAINER_NAME", "export-history");

        storageOptions = ExportHistoryStorageOptions.newBuilder()
                .connectionString(connStr)
                .containerName(container)
                .build();

        // Client
        client = SampleUtils.newClientBuilder().build();

        // Worker — registers export history entity, orchestrators, activities
        DurableTaskGrpcWorkerBuilder workerBuilder = SampleUtils.newWorkerBuilder();
        ExportHistoryWorkers.register(workerBuilder, client, storageOptions);
        DurableTaskGrpcWorker worker = workerBuilder.build();
        worker.start();

        System.out.println("Export History Web App started.");
        System.out.println("REST API: http://localhost:5009/export-jobs");

        // Use port 5009 to avoid conflict with DTS emulator on 8080
        System.setProperty("server.port", "5009");
        SpringApplication.run(ExportHistorySample.class, args);
    }

    private static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isEmpty()) ? v : defaultValue;
    }
}
