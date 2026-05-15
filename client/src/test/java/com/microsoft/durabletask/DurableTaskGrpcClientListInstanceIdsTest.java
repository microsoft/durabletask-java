// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link DurableTaskGrpcClient#listInstanceIds} input validation.
 *
 * <p>Validation runs before any gRPC call, so an unconnected channel is sufficient
 * to construct the client.
 */
public class DurableTaskGrpcClientListInstanceIdsTest {

    private ManagedChannel channel;
    private DurableTaskClient client;

    @BeforeEach
    void setUp() {
        // gRPC channels connect lazily, so this never actually opens a socket as long as
        // we never invoke an RPC.
        this.channel = ManagedChannelBuilder.forAddress("localhost", 0)
                .usePlaintext()
                .build();
        this.client = new DurableTaskGrpcClientBuilder().grpcChannel(this.channel).build();
    }

    @AfterEach
    void tearDown() {
        if (this.client != null) {
            this.client.close();
        }
        if (this.channel != null) {
            this.channel.shutdownNow();
        }
    }

    @Test
    void listInstanceIds_pageSizeZero_throwsIllegalArgumentException() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                this.client.listInstanceIds(null, null, null, 0, null));
        assertTrue(ex.getMessage().contains("pageSize"));
    }

    @Test
    void listInstanceIds_pageSizeNegative_throwsIllegalArgumentException() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                this.client.listInstanceIds(null, null, null, -1, null));
        assertTrue(ex.getMessage().contains("pageSize"));
    }

    @Test
    void listInstanceIds_pageSizeMaxInt_throwsIllegalArgumentException() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                this.client.listInstanceIds(null, null, null, Integer.MAX_VALUE, null));
        assertTrue(ex.getMessage().contains("pageSize"));
    }

    @Test
    void listInstanceIds_pageSizeAboveMax_throwsIllegalArgumentException() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                this.client.listInstanceIds(null, null, null, 1001, null));
        assertTrue(ex.getMessage().contains("pageSize"));
    }

    @Test
    void listInstanceIds_completedTimeToBeforeFrom_throwsIllegalArgumentException() {
        Instant from = Instant.parse("2026-03-01T00:00:00Z");
        Instant to = Instant.parse("2026-02-01T00:00:00Z");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                this.client.listInstanceIds(null, from, to, 100, null));
        assertTrue(ex.getMessage().contains("completedTimeTo"));
    }

    @Test
    void listInstanceIds_completedTimeToEqualsFrom_throwsIllegalArgumentException() {
        Instant ts = Instant.parse("2026-03-01T00:00:00Z");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                this.client.listInstanceIds(null, ts, ts, 100, null));
        assertTrue(ex.getMessage().contains("completedTimeTo"));
    }
}
