// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azuremanaged;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConnectionStringAuthenticationTest {
    private static final String GOVERNMENT = "https://durabletask.azure.us";
    private static final String AUTHORITY = "https://login.microsoftonline.us/";
    private static final String CONNECTION = "Endpoint=https://scheduler.example;TaskHub=test-hub;Authentication=";

    static Stream<Arguments> authenticationTypes() {
        return Stream.of(
            Arguments.of("DefaultAzure", DefaultAzureCredentialBuilder.class, DefaultAzureCredential.class),
            Arguments.of("ManagedIdentity", ManagedIdentityCredentialBuilder.class, ManagedIdentityCredential.class),
            Arguments.of("WorkloadIdentity", WorkloadIdentityCredentialBuilder.class, WorkloadIdentityCredential.class),
            Arguments.of("Environment", EnvironmentCredentialBuilder.class, EnvironmentCredential.class),
            Arguments.of("AzureCli", AzureCliCredentialBuilder.class, AzureCliCredential.class),
            Arguments.of("AzurePowerShell", AzurePowerShellCredentialBuilder.class, AzurePowerShellCredential.class),
            Arguments.of("VisualStudioCode", VisualStudioCodeCredentialBuilder.class, VisualStudioCodeCredential.class),
            Arguments.of("IntelliJ", IntelliJCredentialBuilder.class, IntelliJCredential.class),
            Arguments.of("InteractiveBrowser", InteractiveBrowserCredentialBuilder.class, InteractiveBrowserCredential.class)
        );
    }

    @ParameterizedTest
    @MethodSource("authenticationTypes")
    <T> void everyCredentialTypeReceivesDefaultAndExplicitAudiences(
            String authentication, Class<T> builderType, Class<? extends TokenCredential> credentialType) {
        for (String resourceId : new String[] {null, "api://Custom/.default/.DEFAULT/"}) {
            ResourceIdTest.RecordingCredential recording = new ResourceIdTest.RecordingCredential();
            TokenCredential credential = mock(credentialType);
            when(credential.getToken(any())).thenAnswer(call -> recording.getToken(call.getArgument(0)));
            try (MockedStatic<ResourceId> defaults = mockStatic(ResourceId.class, CALLS_REAL_METHODS);
                 MockedConstruction<T> builders = mockConstruction(builderType,
                     withSettings().defaultAnswer(call -> {
                         if (call.getMethod().getName().equals("build")) {
                             return credential;
                         }
                         return RETURNS_SELF.answer(call);
                     }));
                 ResourceIdTest.ChannelCapture channels = new ResourceIdTest.ChannelCapture()) {
                defaults.when(ResourceId::getDefault).thenReturn(GOVERNMENT);
                String connection = CONNECTION + authentication
                    + (resourceId == null ? "" : ";ResourceId=" + resourceId);
                DurableTaskSchedulerClientOptions.fromConnectionString(connection).createGrpcChannel();
                DurableTaskSchedulerWorkerOptions.fromConnectionString(connection).createGrpcChannel();
                assertEquals(2, builders.constructed().size());
                assertTrue(recording.scopes.isEmpty());
                channels.start(0);
                channels.start(0);
                channels.start(1);
                channels.start(1);
                String expected = resourceId == null ? GOVERNMENT + "/.default" : "api://Custom/.default/.default";
                assertEquals(Collections.nCopies(3, expected), recording.scopes);
            }
        }
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {AUTHORITY, " https://login.microsoftonline.us/ "})
    void authorityIsForwardedOnlyWhenExplicitlyConfigured(String authority) {
        try (MockedConstruction<DefaultAzureCredentialBuilder> defaults =
                mockConstruction(DefaultAzureCredentialBuilder.class);
             MockedConstruction<EnvironmentCredentialBuilder> environments =
                mockConstruction(EnvironmentCredentialBuilder.class);
             MockedConstruction<WorkloadIdentityCredentialBuilder> workloads =
                mockConstruction(WorkloadIdentityCredentialBuilder.class);
             MockedConstruction<InteractiveBrowserCredentialBuilder> browsers =
                mockConstruction(InteractiveBrowserCredentialBuilder.class)) {
            for (String authentication : new String[] {
                    "DefaultAzure", "Environment", "WorkloadIdentity", "InteractiveBrowser"}) {
                DurableTaskSchedulerConnectionString connection = new DurableTaskSchedulerConnectionString(
                    CONNECTION + authentication + ";ResourceId=" + GOVERNMENT
                        + (authority == null ? "" : ";AuthorityHost=" + authority));
                connection.getCredential();
                assertEquals(GOVERNMENT, connection.getResourceId());
                assertEquals("https://scheduler.example", connection.getEndpoint());
                assertEquals(authority == null ? null : authority.trim(), connection.getAuthorityHost());
            }
            if (authority == null || authority.isEmpty()) {
                verify(defaults.constructed().get(0), never()).authorityHost(anyString());
                verify(environments.constructed().get(0), never()).authorityHost(anyString());
                verify(workloads.constructed().get(0), never()).authorityHost(anyString());
                verify(browsers.constructed().get(0), never()).authorityHost(anyString());
            } else {
                verify(defaults.constructed().get(0)).authorityHost(AUTHORITY);
                verify(environments.constructed().get(0)).authorityHost(AUTHORITY);
                verify(workloads.constructed().get(0)).authorityHost(AUTHORITY);
                verify(browsers.constructed().get(0)).authorityHost(AUTHORITY);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("authenticationTypes")
    <T> void regionAndAudienceDoNotConfigureCredentialAuthority(
            String authentication, Class<T> builderType, Class<? extends TokenCredential> credentialType) {
        try (MockedStatic<ResourceId> defaults = mockStatic(ResourceId.class, CALLS_REAL_METHODS);
             MockedConstruction<T> builders = mockConstruction(builderType,
                 withSettings().defaultAnswer(RETURNS_SELF))) {
            defaults.when(ResourceId::getDefault).thenReturn(GOVERNMENT);
            new DurableTaskSchedulerConnectionString(CONNECTION + authentication).getCredential();
            new DurableTaskSchedulerConnectionString(
                CONNECTION + authentication + ";ResourceId=api://CustomAudience").getCredential();
            assertEquals(2, builders.constructed().size());
            for (T builder : builders.constructed()) {
                assertTrue(mockingDetails(builder).getInvocations().stream()
                    .noneMatch(call -> call.getMethod().getName().equals("authorityHost")));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"ManagedIdentity", "AzureCli", "AzurePowerShell", "VisualStudioCode", "IntelliJ", "None"})
    void authorityDoesNotOverrideManagedIdentityOrDeveloperToolClouds(String authentication) {
        // These credentials have no authorityHost API. Creating them must not interpret this as an endpoint.
        DurableTaskSchedulerConnectionString connection = new DurableTaskSchedulerConnectionString(
            CONNECTION + authentication + ";AuthorityHost=" + AUTHORITY + ";ResourceId=" + GOVERNMENT);
        TokenCredential credential = connection.getCredential();
        assertEquals("https://scheduler.example", connection.getEndpoint());
        if (authentication.equals("None")) {
            assertNull(credential);
        } else {
            assertNotNull(credential);
        }
    }
}
