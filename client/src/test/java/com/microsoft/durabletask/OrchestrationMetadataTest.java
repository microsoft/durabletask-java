// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OrchestrationMetadataTest {

    @Test
    void preservesBackendTimestampPrecision() {
        Instant created = Instant.parse("2026-06-30T12:00:00.123456700Z");
        Instant updated = Instant.parse("2026-06-30T12:05:00.765432100Z");
        OrchestratorService.OrchestrationState state = OrchestratorService.OrchestrationState.newBuilder()
                .setInstanceId("instance-1")
                .setName("Orchestrator")
                .setCreatedTimestamp(DataConverter.getTimestampFromInstant(created))
                .setLastUpdatedTimestamp(DataConverter.getTimestampFromInstant(updated))
                .build();

        OrchestrationMetadata metadata = new OrchestrationMetadata(state, new JacksonDataConverter(), false);

        assertEquals(created, metadata.getCreatedAt());
        assertEquals(updated, metadata.getLastUpdatedAt());
    }

    @Test
    void replayTimestampConversionStillUsesMilliseconds() {
        Instant precise = Instant.parse("2026-06-30T12:00:00.123456700Z");
        Timestamp timestamp = DataConverter.getTimestampFromInstant(precise);

        assertEquals(precise.truncatedTo(ChronoUnit.MILLIS), DataConverter.getInstantFromTimestamp(timestamp));
    }
}
