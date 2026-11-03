// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.Utility;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.InetSocketAddress;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class BlobPayloadAccessScopeTest {
    private static final String NAME = "0123456789abcdef0123456789abcdef";
    private static final String PRIMARY = "https://primary.invalid:10000/account-a";
    private static final String ADDITIONAL = "https://additional.invalid:10000/account-b";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void primaryAccountWideAccess_isExplicitAndSupportsBothTokenVersions(boolean accountWide) throws Exception {
        for (String endpoint : Arrays.asList("https://primary.invalid", PRIMARY)) {
            OfflineBlobService service = new OfflineBlobService();
            TokenCredential credential = credential();
            LargePayloadStorageOptions options = new LargePayloadStorageOptions()
                .setAllowAllContainersInConfiguredAccount(accountWide);
            BlobPayloadStore store = identityStore(endpoint, options, credential, service);
            String url = endpoint + "/other-container/" + NAME;
            service.addBlob(url, "payload");
            for (String token : Arrays.asList("blob:v2:" + url, "blob:v1:other-container:" + NAME)) {
                if (accountWide) {
                    assertResolved(store, token);
                } else {
                    assertDenied(store, token);
                }
            }
            if (accountWide) {
                assertTrue(service.requestUrls.stream().allMatch(url::equals));
                assertTrue(service.authorizationHeaders.stream().allMatch("Bearer offline-token"::equals));
            } else {
                assertTrue(service.requestUrls.isEmpty());
                verifyNoInteractions(credential);
            }
            String configured = endpoint + "/payloads/" + NAME;
            service.addBlob(configured, "payload");
            assertResolved(store, "blob:v2:" + configured);
            assertResolved(store, "blob:v1:payloads:" + NAME);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "https://unlisted.invalid:10000/account-b",
        "http://additional.invalid:10000/account-b",
        "https://additional.invalid:10001/account-b",
        "https://additional.invalid:10000/account-other",
        "https://additional.invalid:10000/Account-b",
        "https://additional.invalid.evil.invalid:10000/account-b",
        "https://additional.invalid:10000/%61ccount-b",
        "https://additional.invalid:10000/account-b%2fchild",
        "https://additional.invalid:10000/account-b%5cchild",
        "https://additional.invalid:10000/account-b/%2e%2e/account-other",
        "https://additional.invalid:10000/account-b/../account-other",
        "https://additional.invalid:10000/account-b/%252e%252e",
        "https://primary.invalid:10000/account-other",
        "https://primary.invalid:10001/account-a",
        "http://primary.invalid:10000/account-a"
    })
    void broaderScopes_rejectEndpointAndPathBoundaryChangesBeforeAccess(String endpoint) throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = credential();
        BlobPayloadStore store = identityStore(PRIMARY, new LargePayloadStorageOptions()
            .setAllowAllContainersInConfiguredAccount(true)
            .setAdditionalAccountUris(Collections.singletonList(URI.create(ADDITIONAL))), credential, service);

        assertDenied(store, "blob:v2:" + endpoint + "/other-container/" + NAME);

        assertTrue(service.requestUrls.isEmpty());
        verifyNoInteractions(credential);
    }

    @Test
    void additionalAccounts_areExactOptInsAndDoNotWidenPrimaryContainer() throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = credential();
        BlobPayloadStore store = identityStore(PRIMARY, new LargePayloadStorageOptions()
            .setAdditionalAccountUris(Arrays.asList(URI.create(ADDITIONAL + "/"),
                URI.create("https://second.invalid:443/"))),
            credential, service);
        assertDenied(store, "blob:v2:" + PRIMARY + "/other-container/" + NAME);
        assertDenied(store, "blob:v1:other-container:" + NAME);
        assertTrue(service.requestUrls.isEmpty());
        verifyNoInteractions(credential);

        for (String endpoint : Arrays.asList(ADDITIONAL, "https://SECOND.invalid:443")) {
            for (String container : Arrays.asList("payloads", "other-container", "$root")) {
                String url = endpoint + "/" + container + "/" + NAME;
                service.addBlob(url, "payload");
                assertResolved(store, "blob:v2:" + url);
                URI requested = URI.create(service.requestUrls.get(service.requestUrls.size() - 1));
                URI expected = URI.create(url);
                assertEquals(expected.getScheme(), requested.getScheme());
                assertEquals(expected.getHost().toLowerCase(java.util.Locale.ROOT), requested.getHost());
                assertEquals(expected.getRawPath(), requested.getRawPath());
                assertEquals(endpoint.equals(ADDITIONAL) ? 10000 : -1, requested.getPort());
            }
        }
        assertTrue(service.authorizationHeaders.stream().allMatch("Bearer offline-token"::equals));
    }

    @ParameterizedTest
    @ValueSource(strings = {"shared-key", "sas"})
    void primaryAccountWideAccess_reusesConnectionStringAuthentication(String mode) throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
            .endpoint(PRIMARY + "/payloads").httpClient(service);
        if (mode.equals("shared-key")) {
            builder.credential(new StorageSharedKeyCredential("primary", "b2ZmbGluZS1rZXk="));
        } else {
            builder.sasToken("sv=2023-01-03&sig=offline");
        }
        BlobPayloadStore store = new BlobPayloadStore(builder.buildClient(),
            new LargePayloadStorageOptions().setAllowAllContainersInConfiguredAccount(true));
        String url = PRIMARY + "/other-container/" + NAME;
        service.addBlob(url, "payload");

        assertResolved(store, "blob:v2:" + url);
        assertResolved(store, "blob:v2:" + url + "?sig=untrusted&snapshot=ignored&versionid=ignored#ignored");
        assertResolved(store, "blob:v1:other-container:" + NAME);

        if (mode.equals("shared-key")) {
            assertTrue(service.authorizationHeaders.stream().allMatch(header -> header.startsWith("SharedKey primary:")));
        } else {
            assertTrue(service.requestUrls.stream().allMatch(request -> request.contains("sig=offline")));
            assertTrue(service.requestUrls.stream().noneMatch(request ->
                request.contains("untrusted") || request.contains("snapshot=") || request.contains("versionid=")));
            assertTrue(service.authorizationHeaders.stream().allMatch(header -> header == null));
        }
        assertDenied(store, "blob:v2:" + ADDITIONAL + "/other-container/" + NAME);
        String uploaded = store.upload("payload");
        assertTrue(uploaded.startsWith("blob:v2:" + PRIMARY + "/payloads/"));
        assertFalse(uploaded.contains("?"));
        assertFalse(uploaded.contains("#"));
        assertEquals("payload", store.download(uploaded));
    }

    @Test
    void additionalAccountStorageFailure_propagatesWithoutCredentialFallback() {
        OfflineBlobService service = new OfflineBlobService();
        service.downloadStatus = 403;
        BlobPayloadStore store = identityStore(PRIMARY, new LargePayloadStorageOptions()
            .setAdditionalAccountUris(Collections.singletonList(URI.create(ADDITIONAL))), credential(), service);
        String url = ADDITIONAL + "/payloads/" + NAME;
        PayloadStorageException failure = assertThrows(PayloadStorageException.class,
            () -> store.download("blob:v2:" + url));
        assertEquals(403, ((BlobStorageException) failure.getCause()).getStatusCode());
        assertEquals(Collections.singletonList(url), service.requestUrls);
    }

    @Test
    void scopeConfiguration_isSnapshottedWhenStoreIsConstructed() throws Exception {
        LargePayloadStorageOptions options = new LargePayloadStorageOptions();
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = credential();
        BlobPayloadStore store = identityStore(PRIMARY, options, credential, service);
        options.setAllowAllContainersInConfiguredAccount(true)
            .setAdditionalAccountUris(Collections.singletonList(URI.create(ADDITIONAL)))
            .setAccountUri(URI.create(ADDITIONAL)).setCredential(credential())
            .setContainerName("other-container");
        assertDenied(store, "blob:v2:" + PRIMARY + "/other-container/" + NAME);
        assertDenied(store, "blob:v2:" + ADDITIONAL + "/payloads/" + NAME);
        assertTrue(service.requestUrls.isEmpty());
        verifyNoInteractions(credential);
        service.addBlob(PRIMARY + "/payloads/" + NAME, "payload");
        assertResolved(store, "blob:v2:" + PRIMARY + "/payloads/" + NAME);
    }

    @Test
    void escapedAccountPaths_keepTheirExactConfiguredLocation() throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = credential();
        String endpoint = "https://primary.invalid:10000/account%20a";
        BlobPayloadStore store = identityStore(endpoint,
            new LargePayloadStorageOptions().setAllowAllContainersInConfiguredAccount(true), credential, service);
        assertDenied(store, "blob:v2:https://primary.invalid:10000/%61ccount%20a/other-container/" + NAME);
        assertDenied(store, "blob:v2:https://primary.invalid:10000/account%2fa/other-container/" + NAME);
        assertTrue(service.requestUrls.isEmpty());
        verifyNoInteractions(credential);

        String url = endpoint + "/other-container/" + NAME;
        service.addBlob(url, "payload");
        assertResolved(store, "blob:v2:" + url);
        assertResolved(store, "blob:v1:other-container:" + NAME);
        assertTrue(service.requestUrls.stream().allMatch(url::equals));
    }

    @Test
    void configuredEndpoint_cannotLoseItsAccountPathDuringClientConstruction() {
        TokenCredential credential = credential();
        LargePayloadStorageOptions options = new LargePayloadStorageOptions()
            .setAccountUri(URI.create("https://primary.invalid/account-a")).setCredential(credential);
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> new BlobPayloadStore(options));
        assertTrue(failure.getMessage().contains("preserve the configured account endpoint"));
        verifyNoInteractions(credential);
    }

    @Test
    void configuredEndpoint_acceptsAnExplicitDefaultPortAndTrailingSlash() {
        TokenCredential credential = credential();
        LargePayloadStorageOptions options = new LargePayloadStorageOptions()
            .setAccountUri(URI.create("https://primary.invalid:443/")).setCredential(credential);
        BlobPayloadStore store = new BlobPayloadStore(options);
        assertEquals(("blob:v2:https://primary.invalid/durabletask-payloads/" + NAME).length() + 16,
            store.getEstimatedTokenSizeBytes());
        BlobPayloadStore sasStore = new BlobPayloadStore(new LargePayloadStorageOptions()
            .setConnectionString("BlobEndpoint=https://primary.invalid:443/;"
                + "SharedAccessSignature=sv=2023-01-03&sig=offline"));
        assertEquals(store.getEstimatedTokenSizeBytes(), sasStore.getEstimatedTokenSizeBytes());
        verifyNoInteractions(credential);
    }

    @Test
    void additionalAccount_escapedPathMustMatchItsConfiguredSpelling() throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = credential();
        String endpoint = "https://additional.invalid:10000/account%20b";
        BlobPayloadStore store = identityStore(PRIMARY, new LargePayloadStorageOptions()
            .setAdditionalAccountUris(Collections.singletonList(URI.create(endpoint))), credential, service);
        assertDenied(store, "blob:v2:https://additional.invalid:10000/%61ccount%20b/payloads/" + NAME);
        assertTrue(service.requestUrls.isEmpty());
        verifyNoInteractions(credential);

        service.addBlob(endpoint + "/other-container/" + NAME, "payload");
        assertResolved(store, "blob:v2:" + endpoint + "/other-container/" + NAME);
    }

    @Test
    void additionalEndpoint_cannotLoseItsAccountPathBeforeAccess() throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = credential();
        String endpoint = "https://additional.invalid/account-b";
        assertThrows(IllegalArgumentException.class, () -> identityStore(PRIMARY, new LargePayloadStorageOptions()
            .setAdditionalAccountUris(Collections.singletonList(URI.create(endpoint))), credential, service));
        assertTrue(service.requestUrls.isEmpty());
        verifyNoInteractions(credential);
    }

    @ParameterizedTest
    @ValueSource(strings = {"account%2fa", "account%5ca", "account%252fa", "%2e%2e"})
    void upload_validatesFinalDestinationBeforeCredentialsOrHttp(String account) {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = credential();
        assertThrows(IllegalArgumentException.class, () -> identityStore("https://primary.invalid:10000/" + account,
            new LargePayloadStorageOptions(), credential, service).upload("payload"));
        assertTrue(service.requestUrls.isEmpty());
        verifyNoInteractions(credential);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "additional.invalid", "//additional.invalid", "ftp://additional.invalid",
        "http://additional.invalid", "https://user@additional.invalid",
        "https://additional.invalid?sig=example", "https://additional.invalid#fragment",
        "https://additional.invalid:65536", "https://additional.invalid/account/../other",
        "https://additional.invalid/account/%2e%2e", "https://additional.invalid/account%2fother",
        "https://additional.invalid/account%5cother", "https://additional.invalid/account%252fother",
        "https://additional.invalid/account//other", "https://additional.invalid/account%00other",
        "https://additional.invalid:10000/account-b/container",
        "https://additional.invalid:10000/account-b/container/blob",
        "https://additional.invalid/container", "https://*.invalid"
    })
    void invalidAdditionalAccountEndpoints_failDuringConstruction(String endpoint) {
        TokenCredential credential = credential();
        LargePayloadStorageOptions options = new LargePayloadStorageOptions()
            .setAccountUri(URI.create(PRIMARY)).setCredential(credential)
            .setAdditionalAccountUris(Collections.singletonList(URI.create(endpoint)));
        assertThrows(IllegalArgumentException.class, () -> new BlobPayloadStore(options));
        verifyNoInteractions(credential);
    }

    @Test
    void additionalAccounts_requireCompleteIdentityAuthentication() {
        for (String connection : Arrays.asList("UseDevelopmentStorage=true",
                "BlobEndpoint=https://primary.invalid;SharedAccessSignature=sv=2023-01-03&sig=offline")) {
            TokenCredential credential = credential();
            LargePayloadStorageOptions options = new LargePayloadStorageOptions()
                .setConnectionString(connection).setCredential(credential)
                .setAdditionalAccountUris(Collections.singletonList(URI.create(ADDITIONAL)));
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new BlobPayloadStore(options));
            assertTrue(failure.getMessage().contains("TokenCredential"));
            verifyNoInteractions(credential);
        }
    }

    @Test
    void additionalEndpointList_isImmutableAndDefensivelyCopied() {
        List<URI> endpoints = new ArrayList<>(Collections.singletonList(URI.create(ADDITIONAL)));
        LargePayloadStorageOptions options = new LargePayloadStorageOptions().setAdditionalAccountUris(endpoints);
        endpoints.clear();
        assertEquals(1, options.getAdditionalAccountUris().size());
        assertThrows(UnsupportedOperationException.class, () -> options.getAdditionalAccountUris().clear());
        assertThrows(IllegalArgumentException.class, () -> options.setAdditionalAccountUris(null));
        assertThrows(IllegalArgumentException.class, () -> options.setAdditionalAccountUris(Arrays.asList((URI) null)));
    }

    @ParameterizedTest
    @ValueSource(strings = {PRIMARY, "https://PRIMARY.invalid:10000/account-a/"})
    void additionalAccounts_rejectPrimaryEntriesDuringConstruction(String endpoint) {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = credential();
        assertThrows(IllegalArgumentException.class, () -> identityStore(PRIMARY, new LargePayloadStorageOptions()
            .setAllowAllContainersInConfiguredAccount(true)
            .setAdditionalAccountUris(Collections.singletonList(URI.create(endpoint))), credential, service));
        assertTrue(service.requestUrls.isEmpty());
        verifyNoInteractions(credential);
    }

    @Test
    void additionalAccounts_deduplicateEquivalentEndpoints() throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        BlobPayloadStore store = identityStore(PRIMARY, new LargePayloadStorageOptions()
            .setAdditionalAccountUris(Arrays.asList(URI.create("https://additional.invalid"),
                URI.create("https://ADDITIONAL.invalid:443/"), URI.create("https://additional.invalid/"))),
            credential(), service);
        java.lang.reflect.Field endpoints = BlobPayloadStore.class.getDeclaredField("additionalAccountUris");
        endpoints.setAccessible(true);
        assertEquals(1, ((List<?>) endpoints.get(store)).size());
        service.addBlob("https://additional.invalid/other-container/" + NAME, "payload");
        assertResolved(store, "blob:v2:https://additional.invalid/other-container/" + NAME);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "named-payload.json", "folder/nested/payload.json", "folder/name with spaces+#?.json",
        "literal%2Fname.json", "%2e%2e/literal-name", "folder//payload", "/leading/payload",
        "folder/back\\slash", "caf\u00e9/payload", "folder/name:with:colons",
        "https://other.invalid/container/payload"
    })
    void namedPayloads_preserveLiteralV1AndOnceDecodedV2Names(String name) throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        BlobPayloadStore store = identityStore(PRIMARY, new LargePayloadStorageOptions()
            .setAllowAllContainersInConfiguredAccount(true)
            .setAdditionalAccountUris(Collections.singletonList(URI.create(ADDITIONAL))), credential(), service);
        for (String endpoint : Arrays.asList(PRIMARY, ADDITIONAL)) {
            for (String container : Arrays.asList("payloads", "other-container")) {
                String url = endpoint + "/" + container + "/" + Utility.urlEncode(name);
                service.addBlob(url, "payload");
                assertEquals("payload", store.download("blob:v2:" + url));
                assertEquals(URI.create(url).getPath(),
                    URI.create(service.requestUrls.get(service.requestUrls.size() - 1)).getPath());
                if (endpoint.equals(PRIMARY)) {
                    assertEquals("payload", store.download("blob:v1:" + container + ":" + name));
                    assertEquals(URI.create(url).getPath(),
                        URI.create(service.requestUrls.get(service.requestUrls.size() - 1)).getPath());
                }
            }
        }
    }

    @Test
    void legacyV2SuffixesAndEscapedNames_resolveAcrossHydrationSurfaces() throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = credential();
        BlobPayloadStore store = identityStore(PRIMARY, new LargePayloadStorageOptions()
            .setAdditionalAccountUris(Collections.singletonList(URI.create(ADDITIONAL))), credential, service);
        String name = "folder/nested name+percent%23.json";
        for (String endpoint : Arrays.asList(PRIMARY, ADDITIONAL)) {
            String url = endpoint + "/payloads/" + Utility.urlEncode(name);
            service.addBlob(url, "payload");
            assertResolved(store, "blob:v2:" + url + "?sig=ignored&snapshot=ignored#fragment");
            assertResolved(store, "blob:v2:" + url + "#fragment?sig=ignored");
        }
        assertResolved(store, "blob:v1:payloads:" + name);
        assertResolved(store, "blob:v2:" + PRIMARY + "/%70ayloads/" + Utility.urlEncode(name));
        assertTrue(service.requestUrls.stream().noneMatch(url -> url.contains("?") || url.contains("#")));
        verify(credential, atLeastOnce()).getToken(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "blob:v2:https://primary.invalid:10000/account-a/pay%2floads/payload",
        "blob:v2:https://primary.invalid:10000/account-a/payloads/%2e%2e%2fother-container/payload",
        "blob:v2:https://primary.invalid:10000/account-a/payloads/folder%2f..%2f..%2fother-container/payload",
        "blob:v2:https://primary.invalid:10000/account-a/payloads/..%5cother-container%5cpayload",
        "blob:v1:payloads:../../other-container/payload",
        "blob:v1:payloads:folder/../../other-container/payload"
    })
    void encodedBoundariesAndTraversal_failBeforeCredentialsOrHttp(String token) throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = credential();
        BlobPayloadStore store = identityStore(PRIMARY, new LargePayloadStorageOptions()
            .setAllowAllContainersInConfiguredAccount(true)
            .setAdditionalAccountUris(Collections.singletonList(URI.create(ADDITIONAL))), credential, service);
        assertDenied(store, token);
        assertTrue(service.requestUrls.isEmpty());
        verifyNoInteractions(credential);
    }

    @Test
    void broadReadScopes_leaveUploadsInTheConfiguredContainer() {
        OfflineBlobService service = new OfflineBlobService();
        BlobPayloadStore store = identityStore(PRIMARY, new LargePayloadStorageOptions()
            .setAllowAllContainersInConfiguredAccount(true)
            .setAdditionalAccountUris(Collections.singletonList(URI.create(ADDITIONAL))), credential(), service);
        String token = store.upload("payload");
        assertTrue(token.startsWith("blob:v2:" + PRIMARY + "/payloads/"));
        assertFalse(token.contains("?"));
        assertFalse(token.contains("#"));
        assertTrue(service.requestUrls.stream().allMatch(url -> url.startsWith(PRIMARY + "/payloads")));
        assertEquals("payload", store.download(token));
    }

    @Test
    void finalSdkDestination_isCheckedBeforeCredentialsOrHttp() throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        TokenCredential credential = credential();
        BlobContainerClient container = spy(new BlobServiceClientBuilder().endpoint(PRIMARY)
            .credential(credential).httpClient(service).buildClient().getBlobContainerClient("payloads"));
        BlobPayloadStore store = new BlobPayloadStore(container, new LargePayloadStorageOptions());
        BlobClient wrongDestination = new BlobServiceClientBuilder().endpoint(ADDITIONAL)
            .pipeline(container.getHttpPipeline()).buildClient().getBlobContainerClient("payloads").getBlobClient(NAME);
        doReturn(wrongDestination).when(container).getBlobClient(NAME);
        assertDenied(store, "blob:v2:" + PRIMARY + "/payloads/" + NAME);
        assertTrue(service.requestUrls.isEmpty());
        verifyNoInteractions(credential);
    }

    @Test
    void storageRedirect_defaultTransportDoesNotFollowAnotherAccount() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger primaryRequests = new AtomicInteger();
        AtomicInteger otherRequests = new AtomicInteger();
        String authority = "http://127.0.0.1:" + server.getAddress().getPort();
        server.createContext("/account-a/payloads/" + NAME, exchange -> {
            primaryRequests.incrementAndGet();
            exchange.getResponseHeaders().set("Location", authority + "/account-b/payloads/" + NAME);
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.createContext("/account-b/payloads/" + NAME, exchange -> {
            otherRequests.incrementAndGet();
            exchange.sendResponseHeaders(403, -1);
            exchange.close();
        });
        server.start();
        try {
            BlobPayloadStore store = new BlobPayloadStore(new LargePayloadStorageOptions()
                .setContainerName("payloads")
                .setConnectionString("BlobEndpoint=" + authority
                    + "/account-a;SharedAccessSignature=sv=2023-01-03&sig=offline"));
            PayloadStorageException failure = assertThrows(PayloadStorageException.class,
                () -> store.download("blob:v2:" + authority + "/account-a/payloads/" + NAME));
            assertEquals(302, ((BlobStorageException) failure.getCause()).getStatusCode());
            assertEquals(1, primaryRequests.get());
            assertEquals(0, otherRequests.get());
        } finally {
            server.stop(0);
        }
    }

    private static TokenCredential credential() {
        TokenCredential credential = mock(TokenCredential.class, CALLS_REAL_METHODS);
        when(credential.getToken(any())).thenReturn(Mono.just(
            new AccessToken("offline-token", OffsetDateTime.now().plusHours(1))));
        return credential;
    }

    private static BlobPayloadStore identityStore(String account, LargePayloadStorageOptions options,
            TokenCredential credential, OfflineBlobService service) {
        options.setAccountUri(URI.create(account)).setCredential(credential);
        BlobContainerClient container = new BlobServiceClientBuilder().endpoint(account)
            .credential(credential).httpClient(service).buildClient().getBlobContainerClient("payloads");
        return new BlobPayloadStore(container, options);
    }

    private static void assertDenied(BlobPayloadStore store, String token) throws Exception {
        assertThrows(IllegalArgumentException.class, () -> store.download(token));
        LargePayloadInterceptor interceptor = new LargePayloadInterceptor(store, new LargePayloadStorageOptions());
        Method resolve = resolver();
        for (Object response : BlobPayloadStoreBoundaryTest.responsesWithToken(token)) {
            if (store.isKnownPayloadToken(token)) {
                InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                    () -> resolve.invoke(interceptor, response));
                assertInstanceOf(IllegalArgumentException.class, failure.getCause());
            } else {
                assertSame(response, resolve.invoke(interceptor, response));
            }
        }
    }

    private static void assertResolved(BlobPayloadStore store, String token) throws Exception {
        assertEquals("payload", store.download(token));
        LargePayloadInterceptor interceptor = new LargePayloadInterceptor(store, new LargePayloadStorageOptions());
        Method resolve = resolver();
        List<Object> responses = BlobPayloadStoreBoundaryTest.responsesWithToken(token);
        List<Object> expected = BlobPayloadStoreBoundaryTest.responsesWithToken("payload");
        for (int i = 0; i < responses.size(); i++) {
            assertEquals(expected.get(i), resolve.invoke(interceptor, responses.get(i)));
        }
    }

    private static Method resolver() throws Exception {
        Method resolve = LargePayloadInterceptor.class.getDeclaredMethod("resolveResponsePayloads", Object.class);
        resolve.setAccessible(true);
        return resolve;
    }
}
