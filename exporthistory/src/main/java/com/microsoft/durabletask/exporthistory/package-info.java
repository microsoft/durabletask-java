// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**
 * Durable export of terminal orchestration history to Azure Blob Storage for the Durable Task Java SDK.
 * <p>
 * A durable, checkpointed entity + orchestrator pages terminal instances by completion window, fans out
 * per-instance export activities, and uploads serialized history (gzipped JSONL by default) to a customer-owned
 * blob container.
 *
 * <h2>Components</h2>
 * <ul>
 *   <li>{@link com.microsoft.durabletask.exporthistory.ExportJob} entity — config, status, checkpoint cursor, and
 *       progress counters; signals a run on create.</li>
 *   <li>{@link com.microsoft.durabletask.exporthistory.ExportJobOrchestrator} — pages terminal instances, fans out
 *       export activities, commits checkpoints, handles BATCH vs CONTINUOUS, retries with backoff, and
 *       {@code continueAsNew}s periodically.</li>
 *   <li>{@link com.microsoft.durabletask.exporthistory.ListTerminalInstancesActivity} — calls the client
 *       {@code listInstanceIds} wrapper.</li>
 *   <li>{@link com.microsoft.durabletask.exporthistory.ExportInstanceHistoryActivity} — calls the client
 *       {@code getOrchestrationHistory} wrapper, serializes the {@code com.microsoft.durabletask.history} domain
 *       model to gzipped JSONL, and uploads to blob.</li>
 *   <li>{@link com.microsoft.durabletask.exporthistory.ExportHistoryClient} /
 *       {@link com.microsoft.durabletask.exporthistory.ExportHistoryJobClient} — {@code createJob}/{@code getJob}/
 *       {@code listJobs}/{@code getJobClient}, backed by entity operations and reads.</li>
 *   <li>{@link com.microsoft.durabletask.exporthistory.ExportHistoryWorkerExtensions} and
 *       {@link com.microsoft.durabletask.exporthistory.ExportHistoryClientExtensions} — worker/client registration
 *       entry points.</li>
 * </ul>
 *
 * @see com.microsoft.durabletask.history
 */
package com.microsoft.durabletask.exporthistory;
