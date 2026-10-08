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
    .setThresholdBytes(900_000);

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

- Payloads of at least 900,000 bytes are externalized by default.
- The threshold can be configured up to 1 MiB.
- The default maximum externalized payload size is 10 MiB.
- Payloads are gzip-compressed by default.
- The default container name is `durabletask-payloads`.
- The container is created automatically on the first upload.

The storage identity needs permission to create the container and read and write blobs. Payload blobs are retained after orchestration processing, so configure an Azure Storage lifecycle policy appropriate for the application's retention requirements.

## Sample

See [`LargePayloadSample`](../samples/src/main/java/io/durabletask/samples/LargePayloadSample.java):

```text
./gradlew :samples:runLargePayloadSample
```

The sample uses the DTS emulator and Azurite and verifies a payload larger than 1 MiB through a client, orchestration, and activity round trip.
