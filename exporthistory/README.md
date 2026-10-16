# Durable Task Export History (Java)

Durable, resumable export of **terminal orchestration history** to Azure Blob Storage for the Durable Task Java
SDK — for compliance, audit, and offline analysis before instances age out of the task hub.

This module uses the same checkpointed entity-and-orchestrator workflow as the .NET `Microsoft.DurableTask.ExportHistory` (preview) feature: it pages terminal instances by completion window, fans out per-instance export activities, and uploads serialized history (gzipped JSONL by default) to a customer-owned blob container.

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
  When `completedTimeFrom` is omitted, the lower bound is captured when `ExportJobCreationOptions` is constructed,
  matching .NET. It is preserved through job submission, so worker queue delays do not move the window forward.

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

The worker ensures the export container exists before every upload, recreating it if it was deleted between
exports. The storage credential therefore also needs permission to create the container.

## Export format

Each blob holds the instance's full history using .NET-style history-event JSON. Golden fixtures cover core event serialization and the legacy entity-message encoding produced by the .NET SDK and `Microsoft.Azure.DurableTask.Core`:

- **JSONL** (default, gzipped) is one JSON object per line; **JSON** is a single array.
- Each event is `{"eventType": "...", <type-specific fields>, "eventId": N, "isPlayed": false, "timestamp": "..."}`.
- camelCase field names, null event fields omitted, empty maps as `{}`, enum values in PascalCase (e.g. `"Completed"`), timestamps as trimmed ISO-8601 ending in `Z`, and HTML-safe string escaping (`"` → `\u0022`, `& < > ' +` and all non-ASCII → `\uXXXX`).
- Sub-millisecond timestamps are preserved when reading history and metadata. Exported timestamps and blob-name timestamps use .NET's 100-nanosecond precision; orchestration replay timestamp behavior is unchanged.
- Ordinary and nested failure details retain custom `properties`, including nested values and date tags projected
  to .NET-style timestamps. Failure details read from streamed history retain empty property objects as `{}`.
- Entity operations and locks are exported as `EventSent`/`EventRaised` events. Their `input` contains the reference SDK's JSON-encoded entity request or response, including parent orchestration context and nested failure details. This replaces the earlier Java-native `Entity...` event format; consumers of that preview format must update their event handling. The public history API continues returning typed Java entity events.
- JSONL uses LF line endings. Identical compressed bytes or identical fields across all SDK versions are not guaranteed.

Blob **names**: a lowercase-hex SHA-256 of `"<completedTimestamp>|<instanceId>"` plus the format extension. Re-exporting an instance previously named using millisecond-truncated metadata can produce a new blob name; existing export blobs are not renamed or removed.

## Backend requirement

The export feature relies on the `ListInstanceIds` and `StreamInstanceHistory` gRPC operations. Managed DTS serves
both; the emulator / self-hosted sidecar needs **≥ v0.4.22**. Against an older backend, a raw gRPC `UNIMPLEMENTED`
surfaces (matching .NET).

## Validating the export

Locally, with the DTS emulator and Azurite:

Run the automated suite with `./gradlew :exporthistory:integrationTest -PskipSigning` after starting the backends.
It checks batch export, JSON and gzipped JSONL failure properties, resolved large-payload history, container
recreation after deletion, and continuous export of an orchestration completed between options construction
and job creation.

1. Start the backends:
   ```
   docker run --name durabletask-emulator -p 4001:8080 -d mcr.microsoft.com/dts/dts-emulator:latest
   docker run --name azurite -p 10000:10000 -d mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
   ```
2. Point the app at them: DTS connection `Endpoint=http://localhost:4001;Authentication=None`, storage = the Azurite
   dev connection string, container `orchestration-history`.
3. Run an orchestration to a terminal state, then create a `BATCH` export job whose window covers its completion time.
4. Confirm the job reaches `COMPLETED` and inspect progress:
   ```java
   ExportJobDescription d = job.describe();
   // d.getStatus() == ExportJobStatus.COMPLETED, d.getExportedInstances() >= 1
   ```
5. Download the blob from the container (gunzip for JSONL) and inspect it. Every line carries an `eventType`
   discriminator, `isPlayed:false`, and a trailing `timestamp`.

## Sample

See [`HistoryExportSample`](../samples/src/main/java/io/durabletask/samples/HistoryExportSample.java):

```
./gradlew :samples:runHistoryExportSample
```
