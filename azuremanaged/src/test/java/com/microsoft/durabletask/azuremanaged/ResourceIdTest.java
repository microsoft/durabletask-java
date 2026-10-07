// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.google.protobuf.Empty;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ResourceIdTest {
    private static final String PUBLIC = "https://durabletask.io";
    private static final String GOVERNMENT = "https://durabletask.azure.us";
    private static final String ENDPOINT = "https://scheduler.example:443";
    private static final String HUB = "test-hub";
    private static final String UNICODE_WHITESPACE =
        "\u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
            + "\u2028\u2029\u202f\u205f\u3000";
    private static final Metadata.Key<String> AUTHORIZATION =
        Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
    static final MethodDescriptor<Empty, Empty> METHOD = MethodDescriptor.<Empty, Empty>newBuilder()
        .setType(MethodDescriptor.MethodType.UNARY)
        .setFullMethodName("test.Service/Call")
        .setRequestMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
        .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
        .build();

    private enum Path {
        CLIENT_OPTIONS, WORKER_OPTIONS, CLIENT_CREATE, WORKER_CREATE, CLIENT_USE, WORKER_USE,
        CLIENT_CONNECTION_OPTIONS, WORKER_CONNECTION_OPTIONS,
        CLIENT_CONNECTION_CREATE, WORKER_CONNECTION_CREATE, CLIENT_CONNECTION_USE, WORKER_CONNECTION_USE
    }

    static Stream<Arguments> audiences() {
        return Stream.of(
            Arguments.of(null, null, PUBLIC),
            Arguments.of("", null, PUBLIC),
            Arguments.of("westus2", null, PUBLIC),
            Arguments.of("chinaeast2", null, PUBLIC),
            Arguments.of("notusgov", null, PUBLIC),
            Arguments.of("notusdod", null, PUBLIC),
            Arguments.of(" usgovvirginia", null, PUBLIC),
            Arguments.of("usgovvirginia", null, GOVERNMENT),
            Arguments.of("USGOVARIZONA", null, GOVERNMENT),
            Arguments.of("UsGovTexas", null, GOVERNMENT),
            Arguments.of("usdodcentral", null, GOVERNMENT),
            Arguments.of("USDODEAST", null, GOVERNMENT),
            Arguments.of("UsDodCentral", null, GOVERNMENT),
            Arguments.of("usgov", null, GOVERNMENT),
            Arguments.of("usdod", null, GOVERNMENT),
            Arguments.of(null, "", PUBLIC),
            Arguments.of("usgovvirginia", "", GOVERNMENT),
            Arguments.of("usdodcentral", "", GOVERNMENT),
            Arguments.of("usgovvirginia", PUBLIC, PUBLIC),
            Arguments.of("usdodcentral", PUBLIC, PUBLIC),
            Arguments.of("westus2", GOVERNMENT, GOVERNMENT),
            Arguments.of("chinaeast2", "https://durabletask.example", "https://durabletask.example"),
            Arguments.of(null, GOVERNMENT + "/", GOVERNMENT),
            Arguments.of(null, GOVERNMENT + "/.default", GOVERNMENT),
            Arguments.of(null, GOVERNMENT + "//.DEFAULT//", GOVERNMENT),
            Arguments.of(null, " \t" + GOVERNMENT + "/.default/ \t", GOVERNMENT),
            Arguments.of(null, "\u2003" + GOVERNMENT + "//.DEFAULT//\u2003", GOVERNMENT),
            Arguments.of("usgovvirginia", UNICODE_WHITESPACE + PUBLIC + UNICODE_WHITESPACE, PUBLIC),
            Arguments.of(null, " \t" + UNICODE_WHITESPACE + "api://CustomAudience/resource/.DEFAULT/"
                + UNICODE_WHITESPACE + "\t ", "api://CustomAudience/resource"),
            Arguments.of("usgovvirginia", "api://CustomAudience/resource/.DEFAULT/",
                "api://CustomAudience/resource"),
            Arguments.of("westus2", "api://custom/.default/.default", "api://custom/.default")
        );
    }

    @ParameterizedTest
    @MethodSource("audiences")
    void allPublicPathsRequestSelectedScopeAndRetainItOnRefresh(
            String region, String resourceId, String expected) {
        String defaultAudience = ResourceId.getDefault(region);
        try (MockedStatic<ResourceId> defaults = defaultsFor(region)) {
            for (Path path : Path.values()) {
                RecordingCredential recording = new RecordingCredential();
                DefaultAzureCredential credential = mock(DefaultAzureCredential.class);
                when(credential.getToken(any())).thenAnswer(call -> recording.getToken(call.getArgument(0)));
                try (MockedConstruction<DefaultAzureCredentialBuilder> credentials =
                        mockConstruction(DefaultAzureCredentialBuilder.class,
                            (builder, context) -> when(builder.build()).thenReturn(credential));
                     ChannelCapture channels = new ChannelCapture()) {
                    configure(path, resourceId, credential);
                    assertTrue(recording.scopes.isEmpty(), "Construction must not acquire a token: " + path);
                    assertEquals(1, channels.interceptors.size(), path.toString());
                    assertEquals(Collections.singletonList("scheduler.example:443"), channels.authorities);

                    assertEquals("Bearer token-1", channels.start(0).get(AUTHORIZATION));
                    // The first token is expired. Refresh must keep the scope even if the region changes.
                    defaults.when(ResourceId::getDefault).thenReturn(expected.equals(PUBLIC) ? GOVERNMENT : PUBLIC);
                    assertEquals("Bearer token-2", channels.start(0).get(AUTHORIZATION));
                    assertEquals("Bearer token-2", channels.start(0).get(AUTHORIZATION));
                    assertEquals(Arrays.asList(expected + "/.default", expected + "/.default"),
                        recording.scopes, path.toString());
                    defaults.when(ResourceId::getDefault).thenReturn(defaultAudience);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        " \t ", "///", "/.default", "/.DEFAULT///", " /.DEFAULT/// ",
        "\u0085", "\u00a0", "\u1680", "\u2000", "\u2001", "\u2002", "\u2003", "\u2004", "\u2005",
        "\u2006", "\u2007", "\u2008", "\u2009", "\u200a", "\u2028", "\u2029", "\u202f", "\u205f", "\u3000",
        UNICODE_WHITESPACE, "\u2003///\u2003", "\u00a0/.DEFAULT///\u202f",
        " \t" + UNICODE_WHITESPACE + "/.default" + UNICODE_WHITESPACE + "\t "
    })
    void allPublicPathsRejectInvalidAudiencesEvenWithoutCredentials(String resourceId) {
        try (ChannelCapture channels = new ChannelCapture()) {
            for (Path path : Path.values()) {
                IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> configure(path, resourceId, null), path.toString());
                assertTrue(error.getMessage().contains("ResourceId"));
                assertTrue(error.getMessage().contains("token audience URI"));
            }
            assertTrue(channels.interceptors.isEmpty(), "Invalid audiences must fail before channel creation");
        }
    }

    @Test
    void anonymousAuthenticationStaysAnonymousOnEveryPath() {
        try (ChannelCapture channels = new ChannelCapture()) {
            for (Path path : Path.values()) {
                configure(path, GOVERNMENT, null);
                assertNull(channels.start(channels.interceptors.size() - 1).get(AUTHORIZATION), path.toString());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"westus2", "UsGovVirginia", "USDODEAST"})
    void existingConvenienceOverloadsUseRegionDefault(String region) {
        try (MockedStatic<ResourceId> defaults = defaultsFor(region);
             ChannelCapture channels = new ChannelCapture()) {
            RecordingCredential credential = new RecordingCredential();
            DurableTaskSchedulerClientExtensions.createClientBuilder(ENDPOINT, HUB, credential);
            DurableTaskSchedulerWorkerExtensions.createWorkerBuilder(ENDPOINT, HUB, credential);
            DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(
                new DurableTaskGrpcClientBuilder(), ENDPOINT, HUB, credential);
            DurableTaskSchedulerWorkerExtensions.useDurableTaskScheduler(
                new DurableTaskGrpcWorkerBuilder(), ENDPOINT, HUB, credential);
            assertTrue(credential.scopes.isEmpty());
            for (int i = 0; i < 4; i++) {
                channels.start(i);
            }
            assertEquals(Collections.nCopies(4, ResourceId.getDefault(region) + "/.default"), credential.scopes);
        }
    }

    @Test
    void defaultsArePerInstanceAndRetainedAcrossChannelRecreationAndConnectionStringConversion() {
        try (MockedStatic<ResourceId> defaults = defaultsFor("usgovvirginia");
             ChannelCapture channels = new ChannelCapture()) {
            RecordingCredential credential = new RecordingCredential();
            DurableTaskSchedulerClientOptions governmentClient = clientOptions(credential);
            DurableTaskSchedulerWorkerOptions governmentWorker = workerOptions(credential);
            DurableTaskSchedulerConnectionString governmentConnection =
                new DurableTaskSchedulerConnectionString(connectionString(null, false));

            defaults.when(ResourceId::getDefault).thenReturn(PUBLIC);
            DurableTaskSchedulerClientOptions publicClient = clientOptions(credential);
            DurableTaskSchedulerWorkerOptions publicWorker = workerOptions(credential);
            DurableTaskSchedulerConnectionString publicConnection =
                new DurableTaskSchedulerConnectionString(connectionString(null, false));

            defaults.when(ResourceId::getDefault).thenReturn(GOVERNMENT);
            for (int i = 0; i < 2; i++) {
                governmentClient.createGrpcChannel();
                governmentWorker.createGrpcChannel();
                DurableTaskSchedulerClientOptions.fromConnectionString(governmentConnection)
                    .setCredential(credential).createGrpcChannel();
                DurableTaskSchedulerWorkerOptions.fromConnectionString(governmentConnection)
                    .setCredential(credential).createGrpcChannel();
                publicClient.createGrpcChannel();
                publicWorker.createGrpcChannel();
                DurableTaskSchedulerClientOptions.fromConnectionString(publicConnection)
                    .setCredential(credential).createGrpcChannel();
                DurableTaskSchedulerWorkerOptions.fromConnectionString(publicConnection)
                    .setCredential(credential).createGrpcChannel();
            }
            assertTrue(credential.scopes.isEmpty());
            for (int i = 0; i < channels.interceptors.size(); i++) {
                channels.start(i);
                assertEquals((i % 8 < 4 ? GOVERNMENT : PUBLIC) + "/.default", credential.scopes.get(i));
            }
        }
    }

    @Test
    void defaultReadsActualEnvironment() {
        String region = System.getenv("REGION_NAME");
        String expected = region != null && region.matches("(?i)^(usgov|usdod).*") ? GOVERNMENT : PUBLIC;
        try (ChannelCapture channels = new ChannelCapture()) {
            RecordingCredential credential = new RecordingCredential();
            clientOptions(credential).createGrpcChannel();
            workerOptions(credential).createGrpcChannel();
            DurableTaskSchedulerClientOptions.fromConnectionString(connectionString(null, false))
                .setCredential(credential).createGrpcChannel();
            DurableTaskSchedulerWorkerOptions.fromConnectionString(connectionString(null, false))
                .setCredential(credential).createGrpcChannel();
            for (int i = 0; i < 4; i++) {
                channels.start(i);
            }
            assertEquals(Collections.nCopies(4, expected + "/.default"), credential.scopes);
        }
    }

    private static MockedStatic<ResourceId> defaultsFor(String region) {
        String value = ResourceId.getDefault(region);
        MockedStatic<ResourceId> defaults = mockStatic(ResourceId.class, CALLS_REAL_METHODS);
        defaults.when(ResourceId::getDefault).thenReturn(value);
        return defaults;
    }

    private static DurableTaskSchedulerClientOptions clientOptions(TokenCredential credential) {
        return new DurableTaskSchedulerClientOptions()
            .setEndpointAddress(ENDPOINT).setTaskHubName(HUB).setCredential(credential);
    }

    private static DurableTaskSchedulerWorkerOptions workerOptions(TokenCredential credential) {
        return new DurableTaskSchedulerWorkerOptions()
            .setEndpointAddress(ENDPOINT).setTaskHubName(HUB).setCredential(credential);
    }

    private static String connectionString(String resourceId, boolean authenticated) {
        return "Endpoint=" + ENDPOINT + ";TaskHub=" + HUB
            + ";Authentication=" + (authenticated ? "DefaultAzure" : "None")
            + (resourceId == null ? "" : ";ResourceId=" + resourceId);
    }

    private static void configure(Path path, String resourceId, TokenCredential credential) {
        String connection = connectionString(resourceId, credential != null);
        switch (path) {
            case CLIENT_OPTIONS:
                clientOptions(credential).setResourceId(resourceId).createGrpcChannel();
                break;
            case WORKER_OPTIONS:
                workerOptions(credential).setResourceId(resourceId).createGrpcChannel();
                break;
            case CLIENT_CREATE:
                DurableTaskSchedulerClientExtensions.createClientBuilder(ENDPOINT, HUB, credential, resourceId);
                break;
            case WORKER_CREATE:
                DurableTaskSchedulerWorkerExtensions.createWorkerBuilder(ENDPOINT, HUB, credential, resourceId);
                break;
            case CLIENT_USE:
                DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(
                    new DurableTaskGrpcClientBuilder(), ENDPOINT, HUB, credential, resourceId);
                break;
            case WORKER_USE:
                DurableTaskSchedulerWorkerExtensions.useDurableTaskScheduler(
                    new DurableTaskGrpcWorkerBuilder(), ENDPOINT, HUB, credential, resourceId);
                break;
            case CLIENT_CONNECTION_OPTIONS:
                DurableTaskSchedulerClientOptions.fromConnectionString(connection).createGrpcChannel();
                break;
            case WORKER_CONNECTION_OPTIONS:
                DurableTaskSchedulerWorkerOptions.fromConnectionString(connection).createGrpcChannel();
                break;
            case CLIENT_CONNECTION_CREATE:
                DurableTaskSchedulerClientExtensions.createClientBuilder(connection);
                break;
            case WORKER_CONNECTION_CREATE:
                DurableTaskSchedulerWorkerExtensions.createWorkerBuilder(connection);
                break;
            case CLIENT_CONNECTION_USE:
                DurableTaskSchedulerClientExtensions.useDurableTaskScheduler(
                    new DurableTaskGrpcClientBuilder(), connection);
                break;
            case WORKER_CONNECTION_USE:
                DurableTaskSchedulerWorkerExtensions.useDurableTaskScheduler(
                    new DurableTaskGrpcWorkerBuilder(), connection);
                break;
            default:
                throw new AssertionError(path);
        }
    }

    static final class RecordingCredential implements TokenCredential {
        final List<String> scopes = new ArrayList<>();

        @Override
        public Mono<AccessToken> getToken(TokenRequestContext context) {
            assertEquals(1, context.getScopes().size());
            scopes.add(context.getScopes().get(0));
            OffsetDateTime expiration = scopes.size() == 1
                ? OffsetDateTime.now().minusMinutes(1) : OffsetDateTime.now().plusHours(1);
            return Mono.just(new AccessToken("token-" + scopes.size(), expiration));
        }
    }

    // Capture real authentication interceptors without connecting to a cloud endpoint.
    static final class ChannelCapture implements AutoCloseable {
        final List<ClientInterceptor> interceptors = new ArrayList<>();
        final List<String> authorities = new ArrayList<>();
        private final MockedStatic<Grpc> grpc = mockStatic(Grpc.class);

        ChannelCapture() {
            ManagedChannelBuilder<?> builder = mock(ManagedChannelBuilder.class);
            doAnswer(call -> {
                interceptors.add(call.getArgument(0));
                return builder;
            }).when(builder).intercept(any(ClientInterceptor.class));
            when(builder.build()).thenReturn(mock(ManagedChannel.class));
            grpc.when(() -> Grpc.newChannelBuilder(anyString(), any(ChannelCredentials.class)))
                .thenAnswer(call -> {
                    authorities.add(call.getArgument(0));
                    return builder;
                });
        }

        Metadata start(int index) {
            Channel transport = mock(Channel.class);
            ClientCall<Empty, Empty> call = new ClientCall<Empty, Empty>() {
                @Override public void start(Listener<Empty> listener, Metadata headers) { }
                @Override public void request(int count) { }
                @Override public void cancel(String message, Throwable cause) { }
                @Override public void halfClose() { }
                @Override public void sendMessage(Empty message) { }
            };
            when(transport.newCall(METHOD, CallOptions.DEFAULT)).thenReturn(call);
            ClientCall<Empty, Empty> intercepted =
                interceptors.get(index).interceptCall(METHOD, CallOptions.DEFAULT, transport);
            Metadata headers = new Metadata();
            intercepted.start(new ClientCall.Listener<Empty>() { }, headers);
            assertEquals(HUB, headers.get(Metadata.Key.of("taskhub", Metadata.ASCII_STRING_MARSHALLER)));
            assertNotNull(headers.get(Metadata.Key.of("x-user-agent", Metadata.ASCII_STRING_MARSHALLER)));
            return headers;
        }

        @Override
        public void close() {
            grpc.close();
        }
    }
}
