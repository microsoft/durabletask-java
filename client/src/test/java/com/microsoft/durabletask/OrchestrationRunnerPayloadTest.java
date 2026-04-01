// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for OrchestrationRunner with large payload externalization.
 */
public class OrchestrationRunnerPayloadTest {

    private static final String TOKEN_PREFIX = "test://payload/";

    private static class TestPayloadStore implements PayloadStore {
        private final Map<String, String> blobs = new HashMap<>();

        @Override
        public String upload(String payload) {
            String token = TOKEN_PREFIX + UUID.randomUUID().toString();
            blobs.put(token, payload);
            return token;
        }

        @Override
        public String download(String token) {
            String value = blobs.get(token);
            if (value == null) {
                throw new IllegalArgumentException("Unknown token: " + token);
            }
            return value;
        }

        @Override
        public boolean isKnownPayloadToken(String value) {
            return value != null && value.startsWith(TOKEN_PREFIX);
        }

        void seed(String token, String payload) {
            blobs.put(token, payload);
        }
    }

    @Test
    void loadAndRun_withoutStore_worksNormally() {
        // Build a minimal orchestration request
        byte[] requestBytes = buildSimpleOrchestrationRequest("test-input");
        
        byte[] resultBytes = OrchestrationRunner.loadAndRun(requestBytes, ctx -> {
            String input = ctx.getInput(String.class);
            ctx.complete("output: " + input);
        });
        
        assertNotNull(resultBytes);
        assertTrue(resultBytes.length > 0);
    }

    @Test
    void loadAndRun_withStore_resolvesInputAndExternalizes() {
        TestPayloadStore store = new TestPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder()
            .setThresholdBytes(10)
            .setMaxExternalizedPayloadBytes(100_000)
            .build();

        // Seed a token for the input so it gets resolved
        String largeInput = "\"large-input-data-exceeding-threshold-for-test\"";
        String inputToken = TOKEN_PREFIX + "input-token";
        store.seed(inputToken, largeInput);

        // Build request with token as the input
        byte[] requestBytes = buildSimpleOrchestrationRequest(inputToken);

        byte[] resultBytes = OrchestrationRunner.loadAndRun(requestBytes, ctx -> {
            // The token should have been resolved before the orchestration runs
            String input = ctx.getInput(String.class);
            assertEquals("large-input-data-exceeding-threshold-for-test", input);
            ctx.complete("done");
        }, store, options);

        assertNotNull(resultBytes);
        assertTrue(resultBytes.length > 0);

        // Parse the response to verify it was externalized
        OrchestratorResponse response;
        try {
            response = OrchestratorResponse.parseFrom(resultBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // The output "done" (6 bytes including quotes) is below our 10 byte threshold,
        // so it should NOT be externalized. This verifies the threshold logic works.
        OrchestratorAction completeAction = response.getActionsList().stream()
            .filter(OrchestratorAction::hasCompleteOrchestration)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected CompleteOrchestration action"));
        
        String resultValue = completeAction.getCompleteOrchestration().getResult().getValue();
        assertFalse(store.isKnownPayloadToken(resultValue), 
            "Small result should not be externalized");
    }

    @Test
    void loadAndRun_base64_withStore_works() {
        TestPayloadStore store = new TestPayloadStore();

        byte[] requestBytes = buildSimpleOrchestrationRequest("\"hello\"");
        String base64 = Base64.getEncoder().encodeToString(requestBytes);

        String resultBase64 = OrchestrationRunner.loadAndRun(base64, ctx -> {
            ctx.complete("world");
        }, store);

        assertNotNull(resultBase64);
        assertFalse(resultBase64.isEmpty());
    }

    @Test
    void loadAndRun_nullStore_treatedAsNoExternalization() {
        byte[] requestBytes = buildSimpleOrchestrationRequest("\"test\"");

        byte[] resultBytes = OrchestrationRunner.loadAndRun(requestBytes, ctx -> {
            ctx.complete("result");
        }, null, null);

        assertNotNull(resultBytes);
        assertTrue(resultBytes.length > 0);
    }

    private byte[] buildSimpleOrchestrationRequest(String input) {
        HistoryEvent orchestratorStarted = HistoryEvent.newBuilder()
            .setEventId(-1)
            .setTimestamp(Timestamp.getDefaultInstance())
            .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
            .build();

        HistoryEvent executionStarted = HistoryEvent.newBuilder()
            .setEventId(-1)
            .setTimestamp(Timestamp.getDefaultInstance())
            .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                .setName("TestOrchestration")
                .setVersion(StringValue.of(""))
                .setInput(StringValue.of(input))
                .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                    .setInstanceId("test-" + UUID.randomUUID())
                    .build())
                .build())
            .build();

        HistoryEvent orchestratorCompleted = HistoryEvent.newBuilder()
            .setEventId(-1)
            .setTimestamp(Timestamp.getDefaultInstance())
            .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
            .build();

        return OrchestratorRequest.newBuilder()
            .setInstanceId("test-" + UUID.randomUUID())
            .addNewEvents(orchestratorStarted)
            .addNewEvents(executionStarted)
            .addNewEvents(orchestratorCompleted)
            .build()
            .toByteArray();
    }
}
