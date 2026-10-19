// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * An offline HTTP service beneath the real Azure Blob SDK, including its retry policy.
 */
final class OfflineBlobService implements HttpClient {
    private final Map<String, byte[]> blobs = new HashMap<>();
    private final Map<String, String> encodings = new HashMap<>();
    final List<String> putUrls = new ArrayList<>();
    final List<byte[]> putBodies = new ArrayList<>();
    boolean loseFirstPutResponse;
    boolean changeStoredBytes;
    boolean changeStoredEncoding;
    int downloadStatus = 200;
    int downloads;

    BlobPayloadStore createStore(String container, boolean compressed) {
        return new BlobPayloadStore(new BlobContainerClientBuilder()
                .endpoint("https://offline.invalid/" + container)
                .httpClient(this)
                .retryOptions(new RequestRetryOptions(RetryPolicyType.FIXED, 2, 5, 1L, 1L, null))
                .buildClient(),
                new LargePayloadStorageOptions().setCompressionEnabled(compressed));
    }

    @Override
    public Mono<HttpResponse> send(HttpRequest request) {
        return BinaryData.fromFlux(request.getBody() == null ? Flux.empty() : request.getBody())
                .flatMap(data -> Mono.fromCallable(() -> respond(request, data.toBytes())));
    }

    private synchronized HttpResponse respond(HttpRequest request, byte[] body) throws IOException {
        String url = request.getUrl().toString();
        if (request.getHttpMethod() == HttpMethod.PUT && url.contains("restype=container")) {
            return response(request, 201, new byte[0], null);
        }
        String path = request.getUrl().getPath();
        if (request.getHttpMethod() == HttpMethod.PUT) {
            assertEquals("*", request.getHeaders().getValue("If-None-Match"));
            putUrls.add(url);
            putBodies.add(body);
            if (blobs.containsKey(path)) {
                return response(request, 412, new byte[0], "ConditionNotMet");
            }
            blobs.put(path, changeStoredBytes ? "collision".getBytes(StandardCharsets.UTF_8) : body);
            String encoding = request.getHeaders().getValue("x-ms-blob-content-encoding");
            encodings.put(path, changeStoredEncoding ? "br" : encoding);
            if (loseFirstPutResponse) {
                // Persist first, then fail the transport before the caller sees a response.
                throw new IOException("Connection lost after committing the PUT");
            }
            return response(request, 201, new byte[0], null);
        }
        if (request.getHttpMethod() == HttpMethod.GET) {
            downloads++;
            if (downloadStatus != 200) {
                return response(request, downloadStatus, new byte[0], "ServerBusy");
            }
            byte[] stored = blobs.get(path);
            if (stored == null) {
                return response(request, 404, new byte[0], "BlobNotFound");
            }
            HttpResponse response = response(request, 200, stored, null);
            String encoding = encodings.get(path);
            if (encoding != null) {
                response.getHeaders().set("Content-Encoding", encoding);
            }
            return response;
        }
        throw new AssertionError("Unexpected request: " + request.getHttpMethod() + " " + url);
    }

    private static HttpResponse response(HttpRequest request, int status, byte[] body, String errorCode) {
        HttpHeaders headers = new HttpHeaders()
                .set("Content-Length", Integer.toString(body.length))
                .set("ETag", "\"offline-etag\"")
                .set("Last-Modified", "Mon, 19 Oct 2026 00:00:00 GMT")
                .set("x-ms-request-id", "offline-request");
        if (errorCode != null) {
            headers.set("x-ms-error-code", errorCode);
        }
        HttpResponse response = mock(HttpResponse.class);
        when(response.getRequest()).thenReturn(request);
        when(response.getStatusCode()).thenReturn(status);
        when(response.getHeaders()).thenReturn(headers);
        when(response.getHeaderValue(anyString())).thenAnswer(inv -> headers.getValue(inv.<String>getArgument(0)));
        when(response.getBody()).thenAnswer(inv -> Flux.just(ByteBuffer.wrap(body)));
        when(response.getBodyAsByteArray()).thenReturn(Mono.just(body));
        when(response.getBodyAsString()).thenReturn(Mono.just(new String(body, StandardCharsets.UTF_8)));
        return response;
    }
}
