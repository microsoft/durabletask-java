// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DurableTaskGrpcWorkerBuilder — chunk size validation
 * and worker capability announcement.
 */
public class DurableTaskGrpcWorkerBuilderTest {

    // ---- Chunk size validation tests (matches .NET GrpcDurableTaskWorkerOptionsTests) ----

    @Test
    void defaultChunkSize_isWithinRange() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        int chunkSize = builder.getChunkSizeBytes();
        assertTrue(chunkSize >= DurableTaskGrpcWorkerBuilder.MIN_CHUNK_SIZE_BYTES,
            "Default chunk size should be >= MIN");
        assertTrue(chunkSize <= DurableTaskGrpcWorkerBuilder.MAX_CHUNK_SIZE_BYTES,
            "Default chunk size should be <= MAX");
    }

    @Test
    void chunkSize_belowMin_throws() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class,
            () -> builder.setCompleteOrchestratorResponseChunkSizeBytes(
                DurableTaskGrpcWorkerBuilder.MIN_CHUNK_SIZE_BYTES - 1));
    }

    @Test
    void chunkSize_aboveMax_throws() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class,
            () -> builder.setCompleteOrchestratorResponseChunkSizeBytes(
                DurableTaskGrpcWorkerBuilder.MAX_CHUNK_SIZE_BYTES + 1));
    }

    @Test
    void chunkSize_atMinBoundary_succeeds() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.setCompleteOrchestratorResponseChunkSizeBytes(
            DurableTaskGrpcWorkerBuilder.MIN_CHUNK_SIZE_BYTES);
        assertEquals(DurableTaskGrpcWorkerBuilder.MIN_CHUNK_SIZE_BYTES, builder.getChunkSizeBytes());
    }

    @Test
    void chunkSize_atMaxBoundary_succeeds() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.setCompleteOrchestratorResponseChunkSizeBytes(
            DurableTaskGrpcWorkerBuilder.MAX_CHUNK_SIZE_BYTES);
        assertEquals(DurableTaskGrpcWorkerBuilder.MAX_CHUNK_SIZE_BYTES, builder.getChunkSizeBytes());
    }

    @Test
    void chunkSize_withinRange_succeeds() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        int midpoint = 2_000_000;
        builder.setCompleteOrchestratorResponseChunkSizeBytes(midpoint);
        assertEquals(midpoint, builder.getChunkSizeBytes());
    }

    // ---- Worker capability tests (matches .NET WorkerCapabilitiesTests) ----

    @Test
    void useExternalizedPayloads_setsPayloadStore() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        PayloadStore store = new InMemoryPayloadStore();
        builder.useExternalizedPayloads(store);
        assertNotNull(builder.payloadStore, "payloadStore should be set after useExternalizedPayloads");
        assertNotNull(builder.largePayloadOptions, "largePayloadOptions should be set with defaults");
    }

    @Test
    void useExternalizedPayloads_withOptions_setsPayloadStoreAndOptions() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        PayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder()
            .setThresholdBytes(500)
            .setMaxExternalizedPayloadBytes(5000)
            .build();
        builder.useExternalizedPayloads(store, options);
        assertSame(store, builder.payloadStore);
        assertSame(options, builder.largePayloadOptions);
    }

    @Test
    void useExternalizedPayloads_nullStore_throws() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class,
            () -> builder.useExternalizedPayloads(null));
    }

    @Test
    void useExternalizedPayloads_nullOptions_throws() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        PayloadStore store = new InMemoryPayloadStore();
        assertThrows(IllegalArgumentException.class,
            () -> builder.useExternalizedPayloads(store, null));
    }

    @Test
    void withoutExternalizedPayloads_payloadStoreIsNull() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertNull(builder.payloadStore, "payloadStore should be null by default");
    }

    @Test
    void useExternalizedPayloads_preservesOtherSettings() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        PayloadStore store = new InMemoryPayloadStore();

        builder.setCompleteOrchestratorResponseChunkSizeBytes(2_000_000);
        builder.useExternalizedPayloads(store);

        assertEquals(2_000_000, builder.getChunkSizeBytes(),
            "Chunk size should be preserved after useExternalizedPayloads");
        assertNotNull(builder.payloadStore);
    }

    @Test
    void settingsAreIndependentPerBuilder() {
        DurableTaskGrpcWorkerBuilder builder1 = new DurableTaskGrpcWorkerBuilder();
        DurableTaskGrpcWorkerBuilder builder2 = new DurableTaskGrpcWorkerBuilder();

        PayloadStore store = new InMemoryPayloadStore();
        builder1.useExternalizedPayloads(store);
        builder2.setCompleteOrchestratorResponseChunkSizeBytes(2_000_000);

        assertNotNull(builder1.payloadStore, "Builder1 should have payloadStore");
        assertNull(builder2.payloadStore, "Builder2 should NOT have payloadStore");
        assertEquals(DurableTaskGrpcWorkerBuilder.DEFAULT_CHUNK_SIZE_BYTES, builder1.getChunkSizeBytes(),
            "Builder1 should have default chunk size");
        assertEquals(2_000_000, builder2.getChunkSizeBytes(),
            "Builder2 should have custom chunk size");
    }

    /**
     * Simple in-memory PayloadStore for builder-level tests.
     */
    private static class InMemoryPayloadStore implements PayloadStore {
        @Override
        public String upload(String payload) {
            return "test://token";
        }

        @Override
        public String download(String token) {
            return "payload";
        }

        @Override
        public boolean isKnownPayloadToken(String value) {
            return value != null && value.startsWith("test://");
        }
    }
}
