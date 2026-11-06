// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobDownloadResponse;
import com.azure.storage.blob.models.BlobDownloadHeaders;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.azure.storage.common.Utility;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Azure Blob Storage implementation of {@link PayloadStore}.
 * <p>
 * Stores payloads as blobs and returns self-describing tokens in the form {@code blob:v2:<absolute-blob-URL>}.
 * Reads both v2 and legacy {@code blob:v1:<container>:<blobName>} tokens within the configured read scopes.
 * By default, reads are restricted to the configured account endpoint and container.
 * Supports optional gzip compression. The blob container is created automatically on first upload.
 * If the container is subsequently deleted, a new upload recreates it and retries once.
 */
public final class BlobPayloadStore extends PayloadStore {

    static final String TOKEN_PREFIX = "blob:v1:";
    static final String TOKEN_PREFIX_V2 = "blob:v2:";
    private static final String CONTENT_ENCODING_GZIP = "gzip";

    private static final String PROBE_NAME = "00000000000000000000000000000000";
    private static final Pattern CONTAINER_PATTERN = Pattern.compile(
        "^(?:\\$root|[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){1,61}[a-z0-9])$");

    private final BlobContainerClient containerClient;
    private final LargePayloadStorageOptions options;
    private final URI primaryAccount;
    private final String configuredContainer;
    private final boolean allowAllContainersInConfiguredAccount;
    private final List<URI> additionalAccountUris;

    private final ReentrantLock containerInitializationLock = new ReentrantLock();
    // A stale upload failure must not invalidate a container another upload has already recreated.
    private final AtomicReference<Object> containerGeneration = new AtomicReference<>();

    /**
     * Creates a new {@code BlobPayloadStore} from the given options.
     *
     * @param options the storage options
     * @throws IllegalArgumentException if authentication, endpoint, or read-scope configuration is invalid
     */
    public BlobPayloadStore(LargePayloadStorageOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null.");
        }

        String containerName = options.getContainerName();
        if (containerName == null || containerName.isEmpty()) {
            throw new IllegalArgumentException("Container name must not be null or empty.");
        }

        boolean hasConnectionString = options.getConnectionString() != null
                && !options.getConnectionString().isEmpty();
        boolean hasIdentityAuth = options.getAccountUri() != null && options.getCredential() != null;

        if (!hasConnectionString && !hasIdentityAuth) {
            throw new IllegalArgumentException(
                "Either ConnectionString or AccountUri and Credential must be provided.");
        }
        this.allowAllContainersInConfiguredAccount = options.isAllowAllContainersInConfiguredAccount();

        // Retry policy: exponential (8 retries, 250ms base, 10s max, 2min network timeout)
        // Matches the .NET BlobPayloadStore retry configuration.
        RequestRetryOptions retryOptions = new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL,
            8,           // maxTries
            120,         // tryTimeoutInSeconds (2 min network timeout)
            250L,        // retryDelayInMs (250ms base)
            10_000L,     // maxRetryDelayInMs (10s max)
            null);       // secondaryHost

        BlobServiceClient serviceClient;
        if (hasIdentityAuth) {
            URI account = validateAccountUri(options.getAccountUri());
            serviceClient = new BlobServiceClientBuilder()
                .endpoint(account.toASCIIString())
                .credential(options.getCredential())
                .retryOptions(retryOptions)
                .buildClient();
            if (!sameDestination(account, normalizeAccountUri(URI.create(serviceClient.getAccountUrl())))) {
                throw new IllegalArgumentException("The Azure Blob client cannot preserve the configured account endpoint.");
            }
        } else {
            serviceClient = new BlobServiceClientBuilder()
                .connectionString(options.getConnectionString())
                .retryOptions(retryOptions)
                .buildClient();
        }

        URI selectedEndpoint = URI.create(serviceClient.getAccountUrl());
        this.primaryAccount = validateAccountUri(selectedEndpoint);
        if (!selectedEndpoint.equals(this.primaryAccount)) {
            serviceClient = new BlobServiceClientBuilder().endpoint(this.primaryAccount.toASCIIString())
                .pipeline(serviceClient.getHttpPipeline()).buildClient();
        }
        this.containerClient = serviceClient.getBlobContainerClient(containerName);
        this.options = options;
        this.configuredContainer = containerName;
        this.additionalAccountUris = additionalAccountUris(options, this.primaryAccount);
    }

    /**
     * Package-private constructor for testing with an injected {@link BlobContainerClient}.
     */
    BlobPayloadStore(BlobContainerClient containerClient, LargePayloadStorageOptions options) {
        this.containerClient = containerClient;
        this.options = options;
        this.allowAllContainersInConfiguredAccount = options.isAllowAllContainersInConfiguredAccount();
        this.configuredContainer = containerClient == null ? options.getContainerName()
            : containerClient.getBlobContainerName();
        if (containerClient == null) {
            this.primaryAccount = null;
        } else {
            URI configured = decodeV2Token(TOKEN_PREFIX_V2 + containerClient.getBlobClient(PROBE_NAME).getBlobUrl());
            String suffix = "/" + this.configuredContainer + "/" + PROBE_NAME;
            if (!configured.getRawPath().endsWith(suffix)) {
                throw new IllegalArgumentException("Invalid configured blob container destination.");
            }
            this.primaryAccount = validateAccountUri(withPath(configured,
                configured.getRawPath().substring(0, configured.getRawPath().length() - suffix.length())));
        }
        this.additionalAccountUris = additionalAccountUris(options, this.primaryAccount);
    }

    @Override
    public int getEstimatedTokenSizeBytes() {
        String url = this.containerClient.getBlobClient(PROBE_NAME).getBlobUrl();
        return encodeToken(url).getBytes(StandardCharsets.UTF_8).length + 16;
    }

    @Override
    public String upload(String payload) {
        String blobName = UUID.randomUUID().toString().replace("-", "");
        BlobClient blob = this.containerClient.getBlobClient(blobName);
        String token = encodeToken(blob.getBlobUrl());
        validateBlobClientDestination(blob, new BlobReference(this.primaryAccount, this.configuredContainer, blobName));

        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);

        boolean retryAfterContainerNotFound = true;
        while (true) {
            Object generation = ensureContainerExists();
            try {
                uploadBlob(blob, payloadBytes);
                return token;
            } catch (BlobStorageException e) {
                if (retryAfterContainerNotFound
                        && e.getStatusCode() == 404
                        && BlobErrorCode.CONTAINER_NOT_FOUND.equals(e.getErrorCode())) {
                    this.containerGeneration.compareAndSet(generation, null);
                    retryAfterContainerNotFound = false;
                    continue;
                }

                if (e.getStatusCode() == 409 || e.getStatusCode() == 412) {
                    throw new PayloadStorageException(
                        "Payload blob '" + blobName + "' already exists in container '"
                            + this.containerClient.getBlobContainerName()
                            + "'. Refusing to overwrite. This should not happen with random UUID blob names "
                            + "and likely indicates a bug in a custom PayloadStore implementation.", e);
                }
                throw new PayloadStorageException("Failed to upload payload blob '" + blobName + "'.", e);
            } catch (IOException e) {
                throw new PayloadStorageException("Failed to upload payload blob '" + blobName + "'.", e);
            }
        }
    }

    private void uploadBlob(BlobClient blob, byte[] payloadBytes) throws IOException {
        BlobRequestConditions conditions = new BlobRequestConditions().setIfNoneMatch("*");
        if (this.options.isCompressionEnabled()) {
            ByteArrayOutputStream compressedBuffer = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(compressedBuffer)) {
                gzip.write(payloadBytes);
            }
            byte[] compressedBytes = compressedBuffer.toByteArray();
            BlobHttpHeaders headers = new BlobHttpHeaders().setContentEncoding(CONTENT_ENCODING_GZIP);
            try (InputStream stream = new ByteArrayInputStream(compressedBytes)) {
                try {
                    blob.uploadWithResponse(
                        stream, compressedBytes.length, null, headers, null, null, conditions, null, Context.NONE);
                } catch (BlobStorageException e) {
                    if (isPossibleCommittedUpload(e) && blobContentMatches(blob, compressedBytes, CONTENT_ENCODING_GZIP)) {
                        return;
                    }
                    throw e;
                }
            }
        } else {
            try (InputStream stream = new ByteArrayInputStream(payloadBytes)) {
                try {
                    blob.uploadWithResponse(
                        stream, payloadBytes.length, null, null, null, null, conditions, null, Context.NONE);
                } catch (BlobStorageException e) {
                    if (isPossibleCommittedUpload(e) && blobContentMatches(blob, payloadBytes, null)) {
                        return;
                    }
                    throw e;
                }
            }
        }
    }

    private static boolean isPossibleCommittedUpload(BlobStorageException e) {
        return e.getStatusCode() == 412
            || (e.getStatusCode() == 409 && BlobErrorCode.BLOB_ALREADY_EXISTS.equals(e.getErrorCode()));
    }

    private static boolean blobContentMatches(BlobClient blob, byte[] expectedBytes, String expectedContentEncoding) {
        // Verification failures must propagate with their own status/cause. In particular,
        // a transient GET failure must not turn the ambiguous PUT into a permanent conflict.
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BlobDownloadResponse response = blob.downloadStreamWithResponse(
            output,
            null,
            null,
            null,
            false,
            null,
            Context.NONE);
        BlobDownloadHeaders headers = response.getDeserializedHeaders();
        String actualEncoding = headers == null ? null : headers.getContentEncoding();
        if (expectedContentEncoding == null) {
            if (actualEncoding != null && !actualEncoding.isEmpty()) {
                return false;
            }
        } else if (!expectedContentEncoding.equalsIgnoreCase(actualEncoding)) {
            return false;
        }
        return Arrays.equals(expectedBytes, output.toByteArray());
    }

    private Object ensureContainerExists() {
        Object generation = this.containerGeneration.get();
        if (generation != null) {
            return generation;
        }

        try {
            this.containerInitializationLock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PayloadStorageException("Interrupted while waiting for container creation.", e);
        }

        try {
            generation = this.containerGeneration.get();
            if (generation == null) {
                try {
                    this.containerClient.createIfNotExists();
                } catch (BlobStorageException e) {
                    if (e.getStatusCode() != 409
                            || !BlobErrorCode.CONTAINER_ALREADY_EXISTS.equals(e.getErrorCode())) {
                        throw new PayloadStorageException(
                            "Failed to create blob container '" + this.containerClient.getBlobContainerName() + "'.", e);
                    }
                }
                generation = new Object();
                this.containerGeneration.set(generation);
            }
            return generation;
        } finally {
            this.containerInitializationLock.unlock();
        }
    }

    @Override
    public String download(String token) {
        if (!isKnownPayloadToken(token)) {
            throw new IllegalArgumentException("Invalid external payload token.");
        }
        BlobReference reference = resolveReference(token);
        String container = reference.container;
        String name = reference.name;
        BlobClient blob = resolveBlobClient(reference);

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            // Use downloadStreamWithResponse to get content-encoding header in the same call,
            // avoiding a separate getProperties() round-trip.
            BlobDownloadResponse downloadResponse = blob.downloadStreamWithResponse(
                outputStream,
                null,  // range (full blob)
                null,  // options
                null,  // requestConditions
                false, // getMD5
                null,  // timeout
                Context.NONE);
            byte[] rawBytes = outputStream.toByteArray();

            // Check if the content is gzip-compressed via the response header
            String contentEncoding = downloadResponse.getDeserializedHeaders().getContentEncoding();
            boolean isGzip = CONTENT_ENCODING_GZIP.equalsIgnoreCase(contentEncoding);

            if (isGzip) {
                try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(rawBytes));
                     ByteArrayOutputStream decompressedBuffer = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[8192];
                    int len;
                    while ((len = gzip.read(buffer)) != -1) {
                        decompressedBuffer.write(buffer, 0, len);
                    }
                    return decompressedBuffer.toString(StandardCharsets.UTF_8.name());
                }
            }

            return new String(rawBytes, StandardCharsets.UTF_8);
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                throw new PayloadStorageException(
                    "The blob '" + name + "' was not found in container '" + container + "'. " +
                    "The payload may have been deleted or the container was never created.", e);
            }
            throw new PayloadStorageException("Failed to download payload blob '" + name + "'.", e);
        } catch (IOException e) {
            throw new PayloadStorageException("Failed to decompress payload blob '" + name + "'.", e);
        }
    }

    private BlobReference resolveReference(String token) {
        URI account = this.primaryAccount;
        String[] parts;
        if (token.startsWith(TOKEN_PREFIX)) {
            parts = decodeToken(token);
        } else {
            URI destination = decodeV2Token(token);
            account = matchesAccount(destination, this.primaryAccount) ? this.primaryAccount : null;
            for (URI additional : this.additionalAccountUris) {
                if (matchesAccount(destination, additional)
                        && (account == null || additional.getRawPath().length() > account.getRawPath().length())) {
                    account = additional;
                }
            }
            if (account == null) {
                throw new IllegalArgumentException("Token destination is outside the configured payload access scope.");
            }
            parts = decodeBlobPath(destination, account);
        }
        if (sameDestination(account, this.primaryAccount)
                && !this.allowAllContainersInConfiguredAccount && !parts[0].equals(this.configuredContainer)) {
            throw new IllegalArgumentException("Token destination is outside the configured payload access scope.");
        }
        return new BlobReference(account, parts[0], parts[1]);
    }

    private BlobClient resolveBlobClient(BlobReference reference) {
        BlobClient blob;
        if (sameDestination(reference.account, this.primaryAccount)
                && reference.container.equals(this.configuredContainer)) {
            blob = this.containerClient.getBlobClient(reference.name);
        } else {
            // The public constructor disables secondary-host retries; the selected pipeline never discovers credentials.
            blob = new BlobServiceClientBuilder().endpoint(reference.account.toASCIIString())
                .pipeline(this.containerClient.getHttpPipeline()).buildClient()
                .getBlobContainerClient(reference.container).getBlobClient(reference.name);
        }
        validateBlobClientDestination(blob, reference);
        return blob;
    }

    private static void validateBlobClientDestination(BlobClient blob, BlobReference reference) {
        URI destination = decodeV2Token(TOKEN_PREFIX_V2 + blob.getBlobUrl());
        String[] parts = decodeBlobPath(destination, reference.account);
        if (!parts[0].equals(reference.container) || !parts[1].equals(reference.name)) {
            throw new IllegalArgumentException("Blob client destination does not match the authorized payload location.");
        }
    }

    private static List<URI> additionalAccountUris(LargePayloadStorageOptions options, URI primaryAccount) {
        if (!options.getAdditionalAccountUris().isEmpty()
                && (options.getAccountUri() == null || options.getCredential() == null)) {
            throw new IllegalArgumentException(
                "Additional account access requires AccountUri and TokenCredential authentication.");
        }
        List<URI> endpoints = new ArrayList<>();
        for (URI endpoint : options.getAdditionalAccountUris()) {
            URI normalized = validateAccountUri(endpoint);
            if (!"https".equalsIgnoreCase(normalized.getScheme())) {
                throw new IllegalArgumentException("Additional account access with TokenCredential requires HTTPS.");
            }
            if (primaryAccount != null && sameDestination(normalized, primaryAccount)) {
                throw new IllegalArgumentException(
                    "Additional account URIs must not include the primary account; use AllowAllContainersInConfiguredAccount.");
            }
            if (endpoints.stream().noneMatch(existing -> sameDestination(existing, normalized))) {
                endpoints.add(normalized);
            }
        }
        return endpoints;
    }

    private static URI normalizeAccountUri(URI endpoint) {
        URI normalized = validateStorageUri(endpoint);
        String path = normalized.getRawPath();
        path = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        // An explicit default port makes the Blob SDK interpret a host-style URL as path-style.
        int defaultPort = "https".equalsIgnoreCase(normalized.getScheme()) ? 443 : 80;
        if (path.isEmpty() && normalized.getPort() == defaultPort) {
            return URI.create(normalized.getScheme() + "://" + normalized.getHost());
        }
        return withPath(normalized, path);
    }

    private static URI validateAccountUri(URI endpoint) {
        URI account = normalizeAccountUri(endpoint);
        BlobServiceClient client = new BlobServiceClientBuilder().endpoint(account.toASCIIString()).buildClient();
        if (!sameDestination(account, normalizeAccountUri(URI.create(client.getAccountUrl())))) {
            throw new IllegalArgumentException("The Azure Blob client cannot preserve the configured account endpoint.");
        }
        URI probe = URI.create(client.getBlobContainerClient("payloads").getBlobClient(PROBE_NAME).getBlobUrl());
        String[] parts = decodeBlobPath(probe, account);
        if (!parts[0].equals("payloads") || !parts[1].equals(PROBE_NAME)) {
            throw new IllegalArgumentException("Account URI must identify a Blob service endpoint.");
        }
        return account;
    }

    private static URI withPath(URI uri, String rawPath) {
        return URI.create(uri.getScheme() + "://" + uri.getRawAuthority() + rawPath);
    }

    @Override
    public boolean isKnownPayloadToken(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        try {
            if (value.startsWith(TOKEN_PREFIX_V2)) {
                decodeV2Token(value);
            } else {
                decodeToken(value);
            }
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    static String encodeToken(String blobUrl) {
        // Authentication stays in the configured pipeline, never in persisted tokens.
        String token = TOKEN_PREFIX_V2 + blobUrl.split("[?#]", 2)[0];
        decodeV2Token(token);
        return token;
    }

    static String encodeToken(String container, String name) {
        return TOKEN_PREFIX + container + ":" + name;
    }

    static String[] decodeToken(String token) {
        if (token.startsWith(TOKEN_PREFIX_V2)) {
            // Metadata only: routing parses names relative to an approved endpoint in resolveReference.
            BlobUrlParts parts = BlobUrlParts.parse(decodeV2Token(token).toASCIIString());
            return new String[] { parts.getBlobContainerName(), parts.getBlobName() };
        }
        if (!token.startsWith(TOKEN_PREFIX)) {
            throw new IllegalArgumentException("Invalid external payload token.");
        }
        String rest = token.substring(TOKEN_PREFIX.length());
        int sep = rest.indexOf(':');
        if (sep <= 0 || sep >= rest.length() - 1) {
            throw new IllegalArgumentException("Invalid external payload token format.");
        }
        String container = rest.substring(0, sep);
        String name = rest.substring(sep + 1);
        validateContainerAndName(container, name);
        return new String[] { container, name };
    }

    private static URI decodeV2Token(String token) {
        URI uri = validateBlobUri(URI.create(token.substring(TOKEN_PREFIX_V2.length()).split("[?#]", 2)[0]));
        String path = uri.getRawPath();
        int separator = path.indexOf('/', 1);
        if (separator <= 0 || separator == path.length() - 1) {
            throw new IllegalArgumentException("Invalid external payload URL path.");
        }
        return uri;
    }

    private static String[] decodeBlobPath(URI destination, URI account) {
        if (!matchesAccount(destination, account)) {
            throw new IllegalArgumentException("Blob client destination does not match the authorized account endpoint.");
        }
        String relative = destination.getRawPath().substring(account.getRawPath().length() + 1);
        int separator = relative.indexOf('/');
        if (separator <= 0) {
            throw new IllegalArgumentException("Payload URL must include a container and blob name.");
        }
        String container = Utility.urlDecode(relative.substring(0, separator));
        String name = Utility.urlDecode(relative.substring(separator + 1));
        validateContainerAndName(container, name);
        return new String[] { container, name };
    }

    private static void validateContainerAndName(String container, String name) {
        if (!CONTAINER_PATTERN.matcher(container).matches()) {
            throw new IllegalArgumentException("Invalid payload container name.");
        }
        validateBlobName(name);
    }

    private static void validateBlobName(String name) {
        if (name == null || name.isEmpty() || name.chars().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("Invalid payload blob name.");
        }
        for (String segment : name.split("[/\\\\]", -1)) {
            if (segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("Payload blob names must not contain traversal segments.");
            }
        }
    }

    private static boolean matchesAccount(URI destination, URI account) {
        return sameAuthority(destination, account)
            && destination.getRawPath().startsWith(account.getRawPath() + "/");
    }

    private static URI validateBlobUri(URI value) {
        URI uri = URI.create(value.toASCIIString());
        String path = uri.getRawPath();
        if (!uri.isAbsolute() || uri.getHost() == null
                || !("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
                || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null
                || uri.getPort() > 65535 || path == null) {
            throw new IllegalArgumentException("Invalid storage endpoint or payload URL.");
        }
        if (!uri.getPath().isEmpty()) {
            validateBlobName(uri.getPath());
        }
        return uri;
    }

    private static URI validateStorageUri(URI value) {
        URI uri = validateBlobUri(value);
        String path = uri.getRawPath();
        if (!path.equals(uri.normalize().getRawPath()) || path.contains("//")) {
            throw new IllegalArgumentException("Invalid storage account endpoint path.");
        }
        String decoded = uri.getPath();
        if (decoded.indexOf('\\') >= 0 || decoded.indexOf('%') >= 0
                || decoded.chars().anyMatch(Character::isISOControl)
                || decoded.chars().filter(c -> c == '/').count() != path.chars().filter(c -> c == '/').count()) {
            throw new IllegalArgumentException("Invalid escaping in storage endpoint or payload URL.");
        }
        for (String segment : decoded.split("/")) {
            if (segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("Invalid path in storage endpoint or payload URL.");
            }
        }
        return uri;
    }

    private static boolean sameDestination(URI token, URI configured) {
        return sameAuthority(token, configured) && token.getRawPath().equals(configured.getRawPath());
    }

    private static boolean sameAuthority(URI token, URI configured) {
        return token.getScheme().equalsIgnoreCase(configured.getScheme())
            && token.getHost().equalsIgnoreCase(configured.getHost())
            && effectivePort(token) == effectivePort(configured);
    }

    private static int effectivePort(URI uri) {
        return uri.getPort() >= 0 ? uri.getPort() : ("https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80);
    }

    private static final class BlobReference {
        final URI account;
        final String container;
        final String name;

        BlobReference(URI account, String container, String name) {
            this.account = account;
            this.container = container;
            this.name = name;
        }
    }
}
