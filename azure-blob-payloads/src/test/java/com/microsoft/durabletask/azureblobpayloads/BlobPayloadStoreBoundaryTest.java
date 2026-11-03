// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class BlobPayloadStoreBoundaryTest {
    @Test
    void workerTokenEstimate_accountsForLongConfiguredUrls() throws Exception {
        String endpoint = "https://" + ("a".repeat(60) + ".").repeat(3) + "invalid/payloads";
        OfflineBlobService service = new OfflineBlobService();
        BlobContainerClient container = new BlobContainerClientBuilder()
            .endpoint(endpoint).httpClient(service).buildClient();
        LargePayloadStorageOptions options = new LargePayloadStorageOptions();
        BlobPayloadStore store = new BlobPayloadStore(container, options);
        String token = store.upload("payload");
        assertTrue(store.getEstimatedTokenSizeBytes() > 160);
        assertTrue(store.getEstimatedTokenSizeBytes() > token.length());
        com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder builder =
            new com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder();
        LargePayloadWorkerExtensions.useExternalizedPayloads(builder, store, options);
        try (com.microsoft.durabletask.DurableTaskGrpcWorker worker = builder.build()) {
            java.lang.reflect.Field estimate = worker.getClass().getDeclaredField("largePayloadTokenSizeBytes");
            estimate.setAccessible(true);
            assertEquals(store.getEstimatedTokenSizeBytes(), estimate.getInt(worker));
        }
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
        "blob:v1:other-container:0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid/other-container/0123456789abcdef0123456789abcdef",
        "blob:v2:https://other.invalid/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:http://configured.invalid/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid:8443/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid/account-b/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid:10000/account-b/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid:10001/account-a/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid.evil.invalid/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid/payloads/../other-container/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid/payloads/%2e%2e/other-container/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid/%70ayloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://user@configured.invalid/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid/payloads/0123456789abcdef0123456789abcdef?sig=example",
        "blob:v2:https://configured.invalid/payloads/0123456789abcdef0123456789abcdef#fragment",
        "blob:v1:payloads:../other-container/0123456789abcdef0123456789abcdef",
        "blob:v1:payloads:../../account-b/payloads/0123456789abcdef0123456789abcdef",
        "blob:v1:payloads:%2e%2e/other-container/0123456789abcdef0123456789abcdef",
        "blob:v1:payloads:..\\other-container\\0123456789abcdef0123456789abcdef",
        "blob:v1:payloads:https://other.invalid/payloads/0123456789abcdef0123456789abcdef",
        "blob:v1:payloads://other.invalid/payloads/0123456789abcdef0123456789abcdef",
        "blob:v1:payloads:0123456789abcdef0123456789abcdef?comp=metadata",
        "blob:v1:payloads:0123456789abcdef0123456789abcdef#fragment"
    })
    void outOfLocationOrMalformedToken_rejectsBeforeCredentialsOrHttp(String token) throws Exception {
        for (String endpoint : new String[] {
                "https://configured.invalid/payloads",
                "https://configured.invalid:10000/account-a/payloads"
        }) {
            TokenCredential credential = mock(TokenCredential.class);
            HttpClient transport = mock(HttpClient.class);
            BlobContainerClient container = new BlobContainerClientBuilder()
                .endpoint(endpoint).credential(credential).httpClient(transport).buildClient();
            BlobPayloadStore store = new BlobPayloadStore(container, new LargePayloadStorageOptions());

            assertThrows(IllegalArgumentException.class, () -> store.download(token), endpoint);
            if (token != null) {
                LargePayloadInterceptor interceptor = new LargePayloadInterceptor(
                    store, new LargePayloadStorageOptions());
                Method resolve = LargePayloadInterceptor.class.getDeclaredMethod("resolveResponsePayloads", Object.class);
                resolve.setAccessible(true);
                for (Object response : responsesWithToken(token)) {
                    if (store.isKnownPayloadToken(token)) {
                        InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                            () -> resolve.invoke(interceptor, response));
                        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
                    } else {
                        assertSame(response, resolve.invoke(interceptor, response),
                            "Unsupported references must remain literal values");
                    }
                }
            }
            verifyNoInteractions(credential, transport);
        }
    }

    @ParameterizedTest
    @CsvSource({
        "https://configured.invalid/payloads,true",
        "https://configured.invalid/payloads,false",
        "https://configured.invalid:10000/account-a/payloads,true",
        "https://configured.invalid:10000/account-a/payloads,false",
        "https://configured.invalid/$root,true",
        "https://configured.invalid/$root,false"
    })
    void configuredStore_roundTrip_usesOnlyConfiguredDestination(String endpoint, boolean compressed) throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = mock(TokenCredential.class, CALLS_REAL_METHODS);
        when(credential.getToken(any())).thenReturn(Mono.just(
            new AccessToken("offline-token", OffsetDateTime.now().plusHours(1))));
        BlobContainerClient container = new BlobContainerClientBuilder()
            .endpoint(endpoint).credential(credential).httpClient(service).buildClient();
        BlobPayloadStore store = new BlobPayloadStore(container,
            new LargePayloadStorageOptions().setCompressionEnabled(compressed));

        String token = store.upload("configured-store payload");

        assertTrue(store.isKnownPayloadToken(token));
        assertEquals("configured-store payload", store.download(token));
        assertEquals(endpoint + "/" + BlobPayloadStore.decodeToken(token)[1], service.putUrls.get(0));
        assertEquals("blob:v2:" + service.putUrls.get(0), token);
        assertTrue(store.getEstimatedTokenSizeBytes() >= token.getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 8);
        assertEquals(1, service.downloads);
        String[] parts = BlobPayloadStore.decodeToken(token);
        String legacy = BlobPayloadStore.encodeToken(parts[0], parts[1]);
        LargePayloadInterceptor interceptor = new LargePayloadInterceptor(store, new LargePayloadStorageOptions());
        Method resolve = LargePayloadInterceptor.class.getDeclaredMethod("resolveResponsePayloads", Object.class);
        resolve.setAccessible(true);
        for (String reference : new String[] {token, legacy}) {
            assertEquals("configured-store payload", store.download(reference));
            List<Object> responses = responsesWithToken(reference);
            List<Object> expected = responsesWithToken("configured-store payload");
            for (int i = 0; i < responses.size(); i++) {
                assertEquals(expected.get(i), resolve.invoke(interceptor, responses.get(i)));
            }
        }
        if (endpoint.equals("https://configured.invalid/payloads")) {
            assertEquals("configured-store payload", store.download(token.replace(
                "https://configured.invalid/", "https://CONFIGURED.invalid:443/")));
        }
        URI configured = URI.create(endpoint).normalize();
        for (String requestUrl : service.requestUrls) {
            URI destination = URI.create(requestUrl).normalize();
            assertEquals(configured.getScheme(), destination.getScheme());
            assertEquals(configured.getHost(), destination.getHost());
            assertEquals(configured.getPort(), destination.getPort());
            assertTrue(destination.getRawPath().equals(configured.getRawPath())
                || destination.getRawPath().startsWith(configured.getRawPath() + "/"));
        }
        verify(credential, atLeastOnce()).getToken(any());
    }

    private static List<Object> responsesWithToken(String token) {
        StringValue value = StringValue.of(token);
        HistoryEvent started = HistoryEvent.newBuilder()
            .setExecutionStarted(ExecutionStartedEvent.newBuilder().setInput(value)).build();
        HistoryEvent raised = HistoryEvent.newBuilder()
            .setEventRaised(EventRaisedEvent.newBuilder().setInput(value)).build();
        HistoryEvent completed = HistoryEvent.newBuilder()
            .setTaskCompleted(TaskCompletedEvent.newBuilder().setResult(value)).build();
        OrchestrationState state = OrchestrationState.newBuilder()
            .setInput(value).setOutput(value).setCustomStatus(value).build();
        return Arrays.asList(
            WorkItem.newBuilder().setActivityRequest(ActivityRequest.newBuilder().setInput(value)).build(),
            WorkItem.newBuilder().setOrchestratorRequest(OrchestratorRequest.newBuilder()
                .addPastEvents(started).addNewEvents(raised).addNewEvents(completed)).build(),
            HistoryChunk.newBuilder().addEvents(started).addEvents(raised).addEvents(completed).build(),
            GetInstanceResponse.newBuilder().setOrchestrationState(state).build(),
            QueryInstancesResponse.newBuilder().addOrchestrationState(state).build());
    }
}
