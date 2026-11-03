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
import org.junit.jupiter.params.provider.NullAndEmptySource;
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
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
        "blob:v1:other-container:0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid/other-container/0123456789abcdef0123456789abcdef",
        "blob:v2:https://other.invalid/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:http://configured.invalid/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid:8443/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://configured.invalid/account-b/payloads/0123456789abcdef0123456789abcdef",
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
    @ValueSource(strings = {
        "https://configured.invalid/payloads",
        "https://configured.invalid:10000/account-a/payloads",
        "https://configured.invalid/$root"
    })
    void configuredStore_roundTrip_usesOnlyConfiguredDestination(String endpoint) {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = mock(TokenCredential.class, CALLS_REAL_METHODS);
        when(credential.getToken(any())).thenReturn(Mono.just(
            new AccessToken("offline-token", OffsetDateTime.now().plusHours(1))));
        BlobContainerClient container = new BlobContainerClientBuilder()
            .endpoint(endpoint).credential(credential).httpClient(service).buildClient();
        BlobPayloadStore store = new BlobPayloadStore(container, new LargePayloadStorageOptions());

        String token = store.upload("configured-store payload");

        assertTrue(store.isKnownPayloadToken(token));
        assertEquals("configured-store payload", store.download(token));
        assertEquals(endpoint + "/" + BlobPayloadStore.decodeToken(token)[1], service.putUrls.get(0));
        assertEquals(1, service.downloads);
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
