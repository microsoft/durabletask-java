// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TypedEntityMetadata}.
 */
public class TypedEntityMetadataTest {

    private static final DataConverter dataConverter = new JacksonDataConverter();

    @Test
    void getState_deserializesIntegerState() {
        EntityMetadata base = new EntityMetadata(
                "@counter@c1", Instant.EPOCH, 0, null, "42", true, dataConverter);

        TypedEntityMetadata<Integer> typed = new TypedEntityMetadata<>(base, Integer.class);
        assertNotNull(typed.getState());
        assertEquals(42, typed.getState());
    }

    @Test
    void getState_deserializesStringState() {
        EntityMetadata base = new EntityMetadata(
                "@myEntity@k1", Instant.EPOCH, 0, null, "\"hello\"", true, dataConverter);

        TypedEntityMetadata<String> typed = new TypedEntityMetadata<>(base, String.class);
        assertEquals("hello", typed.getState());
    }

    @Test
    void getState_nullWhenNoState() {
        EntityMetadata base = new EntityMetadata(
                "@counter@c1", Instant.EPOCH, 0, null, null, true, dataConverter);

        TypedEntityMetadata<Integer> typed = new TypedEntityMetadata<>(base, Integer.class);
        assertNull(typed.getState());
    }

    @Test
    void getState_throwsWhenStateNotIncluded() {
        EntityMetadata base = new EntityMetadata(
                "@counter@c1", Instant.EPOCH, 0, null, null, false, dataConverter);

        TypedEntityMetadata<Integer> typed = new TypedEntityMetadata<>(base, Integer.class);
        assertThrows(IllegalStateException.class, typed::getState);
    }

    @Test
    void getStateType_returnsCorrectClass() {
        EntityMetadata base = new EntityMetadata(
                "@counter@c1", Instant.EPOCH, 0, null, "42", true, dataConverter);

        TypedEntityMetadata<Integer> typed = new TypedEntityMetadata<>(base, Integer.class);
        assertEquals(Integer.class, typed.getStateType());
    }

    @Test
    void inheritsEntityMetadataFields() {
        Instant now = Instant.now();
        EntityMetadata base = new EntityMetadata(
                "@counter@myKey", now, 5, "orch-123", "99", true, dataConverter);

        TypedEntityMetadata<Integer> typed = new TypedEntityMetadata<>(base, Integer.class);

        // Inherited fields from EntityMetadata
        assertEquals("@counter@myKey", typed.getInstanceId());
        assertEquals(now, typed.getLastModifiedTime());
        assertEquals(5, typed.getBacklogQueueSize());
        assertEquals("orch-123", typed.getLockedBy());
        assertEquals("99", typed.getSerializedState());
        assertTrue(typed.isIncludesState());

        // Parsed entity ID
        EntityInstanceId entityId = typed.getEntityInstanceId();
        assertEquals("counter", entityId.getName());
        assertEquals("myKey", entityId.getKey());

        // Typed state
        assertEquals(99, typed.getState());
    }

    @Test
    void isInstanceOfEntityMetadata() {
        EntityMetadata base = new EntityMetadata(
                "@counter@c1", Instant.EPOCH, 0, null, "42", true, dataConverter);

        TypedEntityMetadata<Integer> typed = new TypedEntityMetadata<>(base, Integer.class);

        // TypedEntityMetadata IS-A EntityMetadata (matches .NET's EntityMetadata<T> : EntityMetadata)
        assertInstanceOf(EntityMetadata.class, typed);
    }

    @Test
    void readStateAs_stillWorksOnTypedInstance() {
        EntityMetadata base = new EntityMetadata(
                "@counter@c1", Instant.EPOCH, 0, null, "42", true, dataConverter);

        TypedEntityMetadata<Integer> typed = new TypedEntityMetadata<>(base, Integer.class);

        // readStateAs from base class should still work
        Integer state = typed.readStateAs(Integer.class);
        assertEquals(42, state);
    }

    // region Jackson serialization tests

    @Test
    void jacksonSerialization_typedEntityMetadata_hidesInternalFields() throws Exception {
        Instant now = Instant.parse("2026-01-15T10:30:00Z");
        EntityMetadata base = new EntityMetadata(
                "@counter@myKey", now, 3, "orch-lock-123", "42", true, dataConverter);

        TypedEntityMetadata<Integer> typed = new TypedEntityMetadata<>(base, Integer.class);

        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        String json = mapper.writeValueAsString(typed);
        JsonNode root = mapper.readTree(json);

        // Should include public API fields
        assertTrue(root.has("entityId"), "Should have entityId field");
        assertTrue(root.has("lastModifiedTime"), "Should have lastModifiedTime field");
        assertTrue(root.has("backlogQueueSize"), "Should have backlogQueueSize field");
        assertTrue(root.has("lockedBy"), "Should have lockedBy field");
        assertTrue(root.has("includesState"), "Should have includesState field");
        assertTrue(root.has("state"), "Should have state field");

        // Should NOT include internal fields
        assertFalse(root.has("serializedState"), "Should not expose serializedState");
        assertFalse(root.has("dataConverter"), "Should not expose dataConverter");
        assertFalse(root.has("stateType"), "Should not expose stateType");
        assertFalse(root.has("instanceId"), "Should not expose raw instanceId (use entityId instead)");

        // Verify field values
        assertEquals("@counter@myKey", root.get("entityId").asText());
        assertEquals(3, root.get("backlogQueueSize").asInt());
        assertEquals("orch-lock-123", root.get("lockedBy").asText());
        assertTrue(root.get("includesState").asBoolean());
        assertEquals(42, root.get("state").asInt());
    }

    @Test
    void jacksonSerialization_entityMetadata_hidesInternalFields() throws Exception {
        EntityMetadata base = new EntityMetadata(
                "@counter@c1", Instant.EPOCH, 0, null, "99", true, dataConverter);

        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        String json = mapper.writeValueAsString(base);
        JsonNode root = mapper.readTree(json);

        // Should include public API fields
        assertTrue(root.has("entityId"), "Should have entityId field");
        assertTrue(root.has("lastModifiedTime"), "Should have lastModifiedTime field");

        // Should NOT include internal fields
        assertFalse(root.has("serializedState"), "Should not expose serializedState");
        assertFalse(root.has("dataConverter"), "Should not expose dataConverter");
        assertFalse(root.has("instanceId"), "Should not expose raw instanceId");
    }

    // endregion
}
