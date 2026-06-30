# Durable Task Export History (Java)

Durable, resumable export of **terminal orchestration history** to Azure Blob Storage for the Durable Task Java
SDK — for compliance, audit, and offline analysis before instances age out of the task hub.

This module is at parity with the .NET `Microsoft.DurableTask.ExportHistory` (preview) feature: a checkpointed
entity + orchestrator that pages terminal instances by completion window, fans out per-instance export activities,
and uploads serialized history (gzipped JSONL by default) to a customer-owned blob container.

> **Status:** preview (`0.1.0`).

## Install

Add the module dependency alongside the core `client` (and your Durable Task Scheduler extension):

```groovy
implementation 'com.microsoft:durabletask-exporthistory:0.1.0'
```

The export activities upload to Azure Blob Storage via `azure-storage-blob`. If you authenticate with a managed
identity, also add `com.azure:azure-identity` to your application.

## Usage

```java
// Storage destination for exported history (Azure Blob)
ExportHistoryStorageOptions storage = new ExportHistoryStorageOptions()
    .setConnectionString(System.getenv("EXPORT_HISTORY_STORAGE_CONNECTION_STRING"))
    .setContainerName("orchestration-history")
    .setPrefix("exports/");                  // optional
    // identity alt: .setAccountUri(uri).setCredential(new DefaultAzureCredentialBuilder().build())

// Build a client first — the export activities need a client to the same backend.
DurableTaskGrpcClientBuilder clientBuilder = new DurableTaskGrpcClientBuilder();
DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(clientBuilder, dtsConn);
DurableTaskClient client = clientBuilder.build();

// Worker: register the export entity + orchestrators + activities (uploads run here).
DurableTaskGrpcWorkerBuilder workerBuilder = new DurableTaskGrpcWorkerBuilder();
DurableTaskSchedulerWorkerExtensions.useDurableTaskScheduler(workerBuilder, dtsConn);
ExportHistoryWorkerExtensions.useExportHistory(workerBuilder, storage, client);

// Client: obtain an ExportHistoryClient bound to the destination.
ExportHistoryClient export = ExportHistoryClientExtensions.useExportHistory(client, storage);

// Create a job: archive everything completed in a window.
ExportHistoryJobClient job = export.createJob(new ExportJobCreationOptions("nightly-archive")
    .setMode(ExportMode.BATCH)
    .setCompletedTimeFrom(Instant.parse("2026-06-01T00:00:00Z"))
    .setCompletedTimeTo(Instant.parse("2026-06-25T00:00:00Z"))
    .setRuntimeStatus(List.of(OrchestrationRuntimeStatus.COMPLETED))
    .setMaxInstancesPerBatch(200));          // 1–1000, default 100

// Inspect progress.
ExportJobDescription d = job.describe();
System.out.println(d.getStatus() + " exported=" + d.getExportedInstances());
```

### Modes

- **BATCH** — exports a fixed completion-time window and completes. Requires `completedTimeFrom` and
  `completedTimeTo` (the upper bound must not be in the future).
- **CONTINUOUS** — tails newly-completed terminal instances on a 1-minute idle loop until the job is deleted.

### Terminal statuses only

Export supports terminal orchestration statuses only: `COMPLETED`, `FAILED`, `TERMINATED`. When no status filter is
supplied, all three are exported.

## Required settings

- **DTS connection** — the one your app already uses; no new value.
- **Blob destination** — a container name plus either a storage **connection string** or **identity**
  (`AccountUri` + `TokenCredential`). Prefix and format (JSONL + gzip) are optional with defaults. The storage
  secret is held worker-side, not persisted in task-hub state.
- **Permissions** — the storage credential needs blob write on the container; the DTS credential needs
  orchestration read.

## Differences from .NET

1. **Worker registration takes an explicit client** — `useExportHistory(workerBuilder, storage, client)`. Java has
   no dependency injection, so the export activities require a `DurableTaskClient` for the same backend.
2. **Export format** — history is serialized from the structured `com.microsoft.durabletask.history` domain model
   (camelCase, null fields omitted, one event per line for JSONL). Byte-level parity with .NET's
   protobuf-`HistoryEvent`-JSON output is an open item.

## Backend requirement

The export feature relies on the `ListInstanceIds` and `StreamInstanceHistory` gRPC operations. Managed DTS serves
both; the emulator / self-hosted sidecar needs **≥ v0.4.22**. Against an older backend, a raw gRPC `UNIMPLEMENTED`
surfaces (matching .NET).

## Sample

See [`HistoryExportSample`](../samples/src/main/java/io/durabletask/samples/HistoryExportSample.java):

```
./gradlew :samples:runHistoryExportSample
```
