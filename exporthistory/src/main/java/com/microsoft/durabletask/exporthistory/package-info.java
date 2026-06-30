// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**
 * Durable export of terminal orchestration history to Azure Blob Storage for the Durable Task Java SDK.
 * <p>
 * This module is at parity with the .NET {@code Microsoft.DurableTask.ExportHistory} (preview) feature: a durable,
 * checkpointed entity + orchestrator that pages terminal instances by completion window, fans out per-instance
 * export activities, and uploads serialized history (gzipped JSONL by default) to a customer-owned blob container.
 *
 * <h2>Scaffold status</h2>
 * This package currently contains the stable configuration/value surface only. The remaining components below are
 * the implementation work for PR 2 and should be added next, mirroring the .NET source of truth under
 * {@code durabletask-dotnet/src/ExportHistory}:
 * <ul>
 *   <li>{@code ExportJob} entity — config, status, checkpoint cursor, progress counters; signals a run on create.</li>
 *   <li>{@code ExportJobOrchestrator} — pages terminal instances, fans out export activities, commits checkpoints,
 *       handles BATCH vs CONTINUOUS, retries with backoff, and {@code continueAsNew}s periodically.</li>
 *   <li>{@code ListTerminalInstancesActivity} — calls the client {@code listInstanceIds} wrapper.</li>
 *   <li>{@code ExportInstanceHistoryActivity} — calls the client {@code getOrchestrationHistory} wrapper, serializes
 *       the {@code com.microsoft.durabletask.history} domain model to gzipped JSONL, and uploads to blob.</li>
 *   <li>{@code ExportHistoryClient} / {@code ExportHistoryJobClient} — {@code createJob}/{@code getJob}/
 *       {@code listJobs}/{@code getJobClient}, backed by entity signals/reads.</li>
 *   <li>{@code ExportHistoryWorkerExtensions.useExportHistory(...)} and
 *       {@code ExportHistoryClientExtensions.useExportHistory(...)} — registration entry points, mirroring the
 *       {@code azure-blob-payloads} add-on.</li>
 *   <li>Models/exceptions — {@code ExportCheckpoint}, {@code ExportFailure}, {@code ExportJobState},
 *       {@code ExportJobDescription}, {@code ExportJobQuery}, {@code ExportJobNotFoundException}, etc.</li>
 * </ul>
 *
 * <h2>Open design item (carried from PR 1)</h2>
 * PR 1's {@code getOrchestrationHistory} returns the structured {@code history} domain model rather than
 * protobuf-JSON strings. Byte-level export-format parity with .NET's protobuf-{@code HistoryEvent}-JSON output is an
 * open decision: either serialize the domain model to the same shape or reconstruct proto-JSON in the activity.
 *
 * @see com.microsoft.durabletask.history
 */
package com.microsoft.durabletask.exporthistory;
