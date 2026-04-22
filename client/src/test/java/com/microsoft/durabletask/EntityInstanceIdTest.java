// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EntityInstanceId}.
 */
public class EntityInstanceIdTest {

    @Test
    void constructor_validNameAndKey() {
        EntityInstanceId id = new EntityInstanceId("Counter", "myCounter");
        assertEquals("counter", id.getName());
        assertEquals("myCounter", id.getKey());
    }

    @Test
    void constructor_nullName_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> new EntityInstanceId(null, "key"));
    }

    @Test
    void constructor_emptyName_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> new EntityInstanceId("", "key"));
    }

    @Test
    void constructor_nullKey_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> new EntityInstanceId("name", null));
    }

    @Test
    void constructor_emptyKey_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> new EntityInstanceId("name", ""));
    }

    @Test
    void toString_format() {
        EntityInstanceId id = new EntityInstanceId("Counter", "myCounter");
        assertEquals("@counter@myCounter", id.toString());
    }

    @Test
    void fromString_validFormat() {
        EntityInstanceId id = EntityInstanceId.fromString("@Counter@myCounter");
        assertEquals("counter", id.getName());
        assertEquals("myCounter", id.getKey());
    }

    @Test
    void fromString_keyContainsAtSymbol() {
        // The key can contain @ symbols — only the first two @ delimiters matter
        EntityInstanceId id = EntityInstanceId.fromString("@Counter@key@with@ats");
        assertEquals("counter", id.getName());
        assertEquals("key@with@ats", id.getKey());
    }

    @Test
    void fromString_null_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> EntityInstanceId.fromString(null));
    }

    @Test
    void fromString_empty_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> EntityInstanceId.fromString(""));
    }

    @Test
    void fromString_noLeadingAt_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> EntityInstanceId.fromString("Counter@key"));
    }

    @Test
    void fromString_onlyOneAt_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> EntityInstanceId.fromString("@Counter"));
    }

    @Test
    void roundTrip_toStringAndFromString() {
        EntityInstanceId original = new EntityInstanceId("BankAccount", "acct-123");
        EntityInstanceId parsed = EntityInstanceId.fromString(original.toString());
        assertEquals(original, parsed);
    }

    @Test
    void equals_sameValues_areEqual() {
        EntityInstanceId id1 = new EntityInstanceId("Counter", "c1");
        EntityInstanceId id2 = new EntityInstanceId("Counter", "c1");
        assertEquals(id1, id2);
        assertEquals(id1.hashCode(), id2.hashCode());
    }

    @Test
    void equals_differentName_notEqual() {
        EntityInstanceId id1 = new EntityInstanceId("Counter", "c1");
        EntityInstanceId id2 = new EntityInstanceId("Timer", "c1");
        assertNotEquals(id1, id2);
    }

    @Test
    void equals_differentKey_notEqual() {
        EntityInstanceId id1 = new EntityInstanceId("Counter", "c1");
        EntityInstanceId id2 = new EntityInstanceId("Counter", "c2");
        assertNotEquals(id1, id2);
    }

    @Test
    void equals_null_notEqual() {
        EntityInstanceId id = new EntityInstanceId("Counter", "c1");
        assertNotEquals(null, id);
    }

    @Test
    void equals_differentType_notEqual() {
        EntityInstanceId id = new EntityInstanceId("Counter", "c1");
        assertNotEquals("@counter@c1", id);
    }

    @Test
    void constructor_nameIsLowercased() {
        EntityInstanceId id = new EntityInstanceId("Counter", "c1");
        assertEquals("counter", id.getName());
    }

    @Test
    void equals_differentCaseName_areEqual() {
        EntityInstanceId id1 = new EntityInstanceId("Counter", "c1");
        EntityInstanceId id2 = new EntityInstanceId("counter", "c1");
        assertEquals(id1, id2);
        assertEquals(id1.hashCode(), id2.hashCode());
    }

    @Test
    void equals_mixedCaseName_areEqual() {
        EntityInstanceId id1 = new EntityInstanceId("COUNTER", "c1");
        EntityInstanceId id2 = new EntityInstanceId("counter", "c1");
        assertEquals(id1, id2);
    }

    @Test
    void toString_nameIsLowercased() {
        EntityInstanceId id = new EntityInstanceId("MyEntity", "key1");
        assertEquals("@myentity@key1", id.toString());
    }

    @Test
    void fromString_nameIsLowercased() {
        EntityInstanceId id = EntityInstanceId.fromString("@MyEntity@key1");
        assertEquals("myentity", id.getName());
    }

    @Test
    void constructor_nameContainsAt_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> new EntityInstanceId("my@entity", "key"));
    }

    @Test
    void constructor_nameStartsWithAt_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> new EntityInstanceId("@entity", "key"));
    }

    @Test
    void constructor_nameEndsWithAt_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> new EntityInstanceId("entity@", "key"));
    }

    @Test
    void compareTo_ordering() {
        EntityInstanceId a = new EntityInstanceId("A", "1");
        EntityInstanceId b = new EntityInstanceId("B", "1");
        EntityInstanceId a2 = new EntityInstanceId("A", "2");

        // Same values
        assertEquals(0, a.compareTo(new EntityInstanceId("A", "1")));

        // Same values different case
        assertEquals(0, a.compareTo(new EntityInstanceId("a", "1")));

        // Sort by name first
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);

        // Same name, sort by key
        assertTrue(a.compareTo(a2) < 0);
        assertTrue(a2.compareTo(a) > 0);
    }

    @Test
    void compareTo_sortsList() {
        EntityInstanceId c1 = new EntityInstanceId("Counter", "1");
        EntityInstanceId b2 = new EntityInstanceId("BankAccount", "2");
        EntityInstanceId c3 = new EntityInstanceId("Counter", "3");
        EntityInstanceId a1 = new EntityInstanceId("Account", "1");

        List<EntityInstanceId> ids = Arrays.asList(c1, b2, c3, a1);
        Collections.sort(ids);

        assertEquals("account", ids.get(0).getName());
        assertEquals("bankaccount", ids.get(1).getName());
        assertEquals("counter", ids.get(2).getName());
        assertEquals("1", ids.get(2).getKey());
        assertEquals("counter", ids.get(3).getName());
        assertEquals("3", ids.get(3).getKey());
    }

    // region Jackson serialization tests

    @Test
    void jacksonSerialization_serializesToCompactString() throws Exception {
        EntityInstanceId id = new EntityInstanceId("Counter", "myKey");
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(id);
        assertEquals("\"@counter@myKey\"", json);
    }

    @Test
    void jacksonDeserialization_deserializesFromCompactString() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        EntityInstanceId id = mapper.readValue("\"@counter@myKey\"", EntityInstanceId.class);
        assertEquals("counter", id.getName());
        assertEquals("myKey", id.getKey());
    }

    @Test
    void jacksonRoundTrip_preservesIdentity() throws Exception {
        EntityInstanceId original = new EntityInstanceId("BankAccount", "acct-123");
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(original);
        EntityInstanceId deserialized = mapper.readValue(json, EntityInstanceId.class);
        assertEquals(original, deserialized);
    }

    @Test
    void jacksonDeserialization_inPojo_works() throws Exception {
        // Simulates the CounterPayload scenario where EntityInstanceId is a field
        String json = "{\"entityId\":\"@counter@c1\",\"value\":42}";
        ObjectMapper mapper = new ObjectMapper();
        TestPayload payload = mapper.readValue(json, TestPayload.class);
        assertEquals("counter", payload.entityId.getName());
        assertEquals("c1", payload.entityId.getKey());
        assertEquals(42, payload.value);
    }

    @Test
    void jacksonSerialization_inPojo_works() throws Exception {
        TestPayload payload = new TestPayload();
        payload.entityId = new EntityInstanceId("Counter", "c1");
        payload.value = 42;
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(payload);
        assertTrue(json.contains("\"@counter@c1\""));
        assertTrue(json.contains("\"value\":42"));
    }

    /** Test POJO that embeds an EntityInstanceId, mirroring CounterPayload. */
    public static class TestPayload {
        public EntityInstanceId entityId;
        public int value;
    }

    // endregion
}
