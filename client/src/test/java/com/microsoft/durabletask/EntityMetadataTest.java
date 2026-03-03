// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EntityMetadata}.
 */
public class EntityMetadataTest {

    private static final DataConverter dataConverter = new JacksonDataConverter();

    @Test
    void getters_returnConstructorValues() {
        Instant now = Instant.now();
        EntityMetadata metadata = new EntityMetadata(
                "@counter@myKey", now, 5, "orch-123", "42", dataConverter);

        assertEquals("@counter@myKey", metadata.getInstanceId());
        assertEquals(now, metadata.getLastModifiedTime());
        assertEquals(5, metadata.getBacklogQueueSize());
        assertEquals("orch-123", metadata.getLockedBy());
        assertEquals("42", metadata.getSerializedState());
    }

    @Test
    void getEntityInstanceId_parsesCorrectly() {
        EntityMetadata metadata = new EntityMetadata(
                "@counter@myKey", Instant.EPOCH, 0, null, null, dataConverter);

        EntityInstanceId entityId = metadata.getEntityInstanceId();
        assertEquals("counter", entityId.getName());
        assertEquals("myKey", entityId.getKey());
    }

    @Test
    void readStateAs_deserializesIntegerState() {
        EntityMetadata metadata = new EntityMetadata(
                "@counter@c1", Instant.EPOCH, 0, null, "42", dataConverter);

        Integer state = metadata.readStateAs(Integer.class);
        assertNotNull(state);
        assertEquals(42, state);
    }

    @Test
    void readStateAs_deserializesStringState() {
        EntityMetadata metadata = new EntityMetadata(
                "@myEntity@k1", Instant.EPOCH, 0, null, "\"hello\"", dataConverter);

        String state = metadata.readStateAs(String.class);
        assertEquals("hello", state);
    }

    @Test
    void readStateAs_nullState_returnsNull() {
        EntityMetadata metadata = new EntityMetadata(
                "@counter@c1", Instant.EPOCH, 0, null, null, dataConverter);

        Integer state = metadata.readStateAs(Integer.class);
        assertNull(state);
    }

    @Test
    void lockedBy_nullWhenNotLocked() {
        EntityMetadata metadata = new EntityMetadata(
                "@counter@c1", Instant.EPOCH, 0, null, null, dataConverter);
        assertNull(metadata.getLockedBy());
    }

    @Test
    void backlogQueueSize_zero() {
        EntityMetadata metadata = new EntityMetadata(
                "@counter@c1", Instant.EPOCH, 0, null, null, dataConverter);
        assertEquals(0, metadata.getBacklogQueueSize());
    }
}
