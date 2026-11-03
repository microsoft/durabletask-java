# Durable Task Azure Blob Payloads for Java

The `durabletask-azure-blob-payloads` module transparently externalizes large Durable Task payloads to Azure Blob Storage. Payloads below the configured threshold continue to travel inline; larger payloads are replaced with opaque blob references and restored before user code receives them.

## Install

Add the module alongside the Durable Task client and your backend-specific extension:

```groovy
implementation 'com.microsoft:durabletask-azure-blob-payloads:1.10.0'
```

The module includes Azure Blob Storage support. Applications that use managed identity should also add `com.azure:azure-identity`.

## Usage

Configure the same storage location for every client and worker that exchanges externalized payloads:

```java
LargePayloadStorageOptions payloadOptions = new LargePayloadStorageOptions()
    .setConnectionString(System.getenv("PAYLOAD_STORAGE_CONNECTION_STRING"))
    .setContainerName("durabletask-payloads")
    .setThresholdBytes(256 * 1024);

PayloadStore payloadStore = new BlobPayloadStore(payloadOptions);

DurableTaskGrpcClientBuilder clientBuilder = new DurableTaskGrpcClientBuilder();
DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(clientBuilder, schedulerConnectionString);
LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, payloadStore, payloadOptions);

DurableTaskGrpcWorkerBuilder workerBuilder = new DurableTaskGrpcWorkerBuilder();
DurableTaskSchedulerWorkerExtensions.useDurableTaskScheduler(workerBuilder, schedulerConnectionString);
LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, payloadStore, payloadOptions);
```

For identity-based authentication, configure an account URI and `TokenCredential` instead of a connection string:

```java
LargePayloadStorageOptions payloadOptions = new LargePayloadStorageOptions()
    .setAccountUri(URI.create("https://<account>.blob.core.windows.net"))
    .setCredential(new DefaultAzureCredentialBuilder().build());
```

## Defaults and limits

- Payloads of at least 256 KiB (262,144 bytes) are externalized by default.
- The threshold can be configured up to 1 MiB.
- The default maximum externalized payload size is 10 MiB.
- Payloads are gzip-compressed by default.
- The default container name is `durabletask-payloads`.
- The container is created automatically on the first upload.

The storage identity needs permission to create the container and read and write blobs. Payload blobs are retained after orchestration processing, so configure an Azure Storage lifecycle policy appropriate for the application's retention requirements.

## Payload references and storage location

The built-in store emits `blob:v1:<container>:<32-lowercase-hex-blob-name>` tokens. Downloads use only the
account endpoint configured when the store is constructed (including its scheme, host, port, and account path
for path-style endpoints) and require the token's container to match the configured container. Tokens cannot
select another endpoint or account. Keep this container dedicated to payloads shared by the same trusted clients
and workers; a known blob name within that container can be resolved using the store's configured credentials.

Direct `BlobPayloadStore.download()` calls now require the same complete token grammar as automatic resolution.
Previously accepted nonstandard blob names are no longer supported. Tokens returned by `upload()` are unchanged.
Self-describing `blob:v2:<absolute-blob-URL>` references are not supported by this implementation: automatic
resolution leaves them unchanged as literal data, and direct downloads reject them without Storage requests
or credential acquisition. Custom `PayloadStore` implementations are responsible for their own destination checks.

Changing the configured storage location requires explicit migration of the payload blobs while preserving their
names, or a store configured for the original location. V1 tokens do not record the original account, so changing
the account does not locate or migrate existing payloads automatically.

This module does not implement dashboard reads or delete payload blobs during instance purge. Purge operations
are forwarded to the scheduler; payload retention is managed separately through Storage lifecycle policies.

## Upload failures

Transient upload failures (including HTTP 408, 429, and 5xx responses) propagate without sending a worker
completion, allowing the work item to be retried rather than recording a task failure. Permanent activity and
orchestration upload failures become non-retriable failure completions. Entity upload failures propagate without
completing the batch, matching .NET. The worker logs failed completions and abandons the affected work item for
scheduler redelivery without terminating its polling thread.

Uploads use conditional creation to prevent overwriting an existing payload. If a PUT commits but its response is
lost, a retried PUT can return HTTP 412 or HTTP 409 `BlobAlreadyExists`. Other HTTP 409 errors are not recovered.
The store recovers only after verifying that the stored bytes and content
encoding exactly match the attempted upload. A verification download failure propagates with its own status so
transient failures remain retryable. Payload tokens also support Azure's `$root` container.

## Offline regression tests

```text
./gradlew :azure-blob-payloads:test :exporthistory:test :client:test -PskipSigning
```

These tests do not require Azure or emulators. The blob-upload regressions run the real Azure SDK against an
in-memory HTTP transport that commits a PUT and loses its response, exercising SDK retries with compression on
and off. In-process gRPC tests cover `$root` tokens through client, worker, and streamed history paths and resolved
nested failure traces in decoded JSON and JSONL exports. An orchestration replay regression also verifies that
entity callers receive the original nested failure diagnostics after work-item payload resolution.

## Integration tests

With the DTS emulator on port 4001 and Azurite Blob Storage on port 10000, run:

```text
./gradlew :azure-blob-payloads:integrationTest -PskipSigning
```

The suite exercises payload round trips, activity and sub-orchestration outputs, events, queries, custom status,
and payload limits. It also injects a transient worker upload failure and verifies actual scheduler redelivery
and successful completion for both activity and orchestration outputs.

## Sample

See [`LargePayloadSample`](../samples/src/main/java/io/durabletask/samples/LargePayloadSample.java):

```text
./gradlew :samples:runLargePayloadSample
```

The sample uses the DTS emulator and Azurite and verifies a payload larger than 1 MiB through a client, orchestration, and activity round trip.
