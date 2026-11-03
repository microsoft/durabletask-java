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

The built-in store now writes self-describing `blob:v2:<absolute-blob-URL>` tokens, matching the .NET token
format. It reads both v2 tokens and legacy `blob:v1:<container>:<blob-name>` tokens, including named and nested
blobs. New blob names remain 32 lowercase hexadecimal characters. New v2 URLs retain the blob's location but
never include user information, query parameters, fragments, or credentials. Upgrade all readers before
deploying writers that emit v2 tokens.

Both formats default to the configured storage endpoint **and container**. A reference to another container in
the same account or to another account is rejected, even when the configured identity has Storage permissions
there. Recognizing a self-describing URL does not authorize its destination. Direct downloads and automatic
resolution use the same token parsing and destination policy; blob-name shape does not authorize access.
Token-formatted strings are treated as references even when their blob names are not generated UUIDs.

### Explicit read scopes

Configure read scopes on `LargePayloadStorageOptions` before constructing the store:

| Configuration | Permitted reads |
|---|---|
| Default: `allowAllContainersInConfiguredAccount = false`, no additional account URIs | Configured account endpoint and configured container only |
| `setAllowAllContainersInConfiguredAccount(true)` | All containers at the configured account endpoint |
| `setAdditionalAccountUris(...)` | All containers at each explicitly listed additional account endpoint |

These settings are independent. Listing additional accounts does not widen primary-account container access.
To allow every container across the primary and additional accounts, enable the boolean and list those accounts.
Listing the primary endpoint is rejected during construction; use `setAllowAllContainersInConfiguredAccount(true)`
to widen its container scope. Equivalent additional endpoints are deduplicated. Uploads and upload
verification always use the configured container, regardless of read scope.

For primary-account-wide reads using the existing authentication mode:

```java
payloadOptions.setAllowAllContainersInConfiguredAccount(true);
```

For additional-account reads using an explicitly configured Storage identity:

```java
LargePayloadStorageOptions payloadOptions = new LargePayloadStorageOptions()
    .setAccountUri(URI.create("https://primary.blob.core.windows.net"))
    .setCredential(new DefaultAzureCredentialBuilder().build())
    .setAdditionalAccountUris(Arrays.asList(
        URI.create("https://archive.blob.core.windows.net")));
```

An account URI is an exact service endpoint, not a hostname allowlist, container URL, or path-prefix grant.
Authorization compares scheme, hostname (case-insensitive), effective port (including default ports), and the
complete account path. Container-scoped access additionally compares the container. Account-wide access relaxes
only that container check: it does not permit different ports, schemes, path-style accounts, or subpaths.
For Azurite, an account endpoint includes the account path, for example `http://127.0.0.1:10000/devstoreaccount1`.

Endpoint validation and the final SDK account/container/name check run before HTTP requests or credential
acquisition. Additional entries must be genuine Blob service endpoints, not container/blob URLs or arbitrary
URL prefixes. Endpoint query parameters, fragments, user information, traversal segments, encoded separators,
double-encoded account paths, duplicate account-path slashes, and control characters are rejected.
Escaped account paths must use the same spelling as the configured URI; alternate escaped spellings are not
treated as aliases. Endpoints whose Azure Blob SDK parsing changes the account boundary are rejected during
construction. One trailing slash on an account endpoint is accepted. Policy, account endpoints, configured
container, and selected authentication are captured when a store is constructed; construct a new store to
change its grants.

V2 reads discard legacy query and fragment suffixes before selecting an endpoint or constructing a client.
These suffixes never supply SAS credentials, snapshots, or version selection. User information remains invalid.
Account selection uses only configured service endpoints, then decodes the container and remaining blob name
once, preserving escaped and nested names. Encoded separators cannot change the selected account or container.
V1 names are literal names passed through the SDK: percent sequences are not pre-decoded. Traversal segments
and control characters are rejected. Denied destinations and SDK destination mismatches propagate as errors.

### Authentication and Storage permissions

Default and primary-account-wide reads support connection strings using account keys or SAS credentials, and
identity authentication using `AccountUri` plus `TokenCredential`. Same-account routing reuses the configured
authentication pipeline, including its SAS restrictions. Storage can still reject another container when a
container SAS is used; opting in does not expand the SAS's permissions.

Additional-account reads require `AccountUri` **and** `TokenCredential`, and additional endpoints must use HTTPS.
Account keys and SAS credentials are not reused across accounts. Unsupported combinations fail during store
construction. As with existing configuration, complete identity configuration takes precedence if a connection
string is also supplied. Every permitted destination uses that same selected authentication pipeline and
identity; there is no credential discovery, mode switching, or fallback after Storage rejects a request.
The identity must independently have read permission on the relevant account and containers. Storage redirects
are not followed by the configured SDK pipeline, and retries do not select a primary-account secondary host.

Keep the configured container dedicated to payloads shared by the same trusted clients and workers; a known
blob name within it can be resolved using the store's configured credentials. Custom `PayloadStore`
implementations are responsible for their own destination checks and can override `getEstimatedTokenSizeBytes()`
when their upload tokens exceed the default size estimate. The built-in store supplies an estimate based on
its configured URL length so worker action-size validation accounts for v2 tokens.

Changing the configured storage location requires explicit migration of the payload blobs while preserving their
names and updating v2 references to the new URLs, or a store configured for the original location. V1 tokens
do not record the original account, so changing the account does not locate or migrate existing payloads
automatically, and v1 references are never searched for in additional accounts. V2 tokens retain the original
location: either migrate blobs and update their references, keep a store configured for the original location,
or explicitly authorize the original account/container through the scopes above. These scopes do not migrate
payloads, rewrite URLs, or locate missing blobs.

This module does not implement dashboard reads or delete payload blobs during instance purge. Purge operations
are forwarded to the scheduler; payload retention is managed separately through Storage lifecycle policies.
Client and worker interceptors share this store for inputs, activity and orchestration results, events,
streamed history, custom status, failure traces, and entity state. The history-export module obtains resolved
history through that client path; its separate archive writer is not a payload-reference reader. There is no
service implementation or delegated dashboard credential flow in this repository to modify.

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
Access-scope tests use the real Azure SDK with an offline HTTP transport and instrumented credentials. They
cover the restrictive default, both opt-in scopes, exact endpoint/path boundaries, v1/v2 resolution across
client and worker surfaces, selected authentication modes, and failures before HTTP or token acquisition.
A loopback-only HTTP test exercises the default SDK transport and verifies that a Storage redirect is not followed.

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
