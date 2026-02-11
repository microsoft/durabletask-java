// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

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
        assertEquals("Counter", id.getName());
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
        assertEquals("@Counter@myCounter", id.toString());
    }

    @Test
    void fromString_validFormat() {
        EntityInstanceId id = EntityInstanceId.fromString("@Counter@myCounter");
        assertEquals("Counter", id.getName());
        assertEquals("myCounter", id.getKey());
    }

    @Test
    void fromString_keyContainsAtSymbol() {
        // The key can contain @ symbols — only the first two @ delimiters matter
        EntityInstanceId id = EntityInstanceId.fromString("@Counter@key@with@ats");
        assertEquals("Counter", id.getName());
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
        assertNotEquals("@Counter@c1", id);
    }

    @Test
    void compareTo_ordering() {
        EntityInstanceId a = new EntityInstanceId("A", "1");
        EntityInstanceId b = new EntityInstanceId("B", "1");
        EntityInstanceId a2 = new EntityInstanceId("A", "2");

        // Same values
        assertEquals(0, a.compareTo(new EntityInstanceId("A", "1")));

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

        assertEquals(a1, ids.get(0));
        assertEquals(b2, ids.get(1));
        assertEquals(c1, ids.get(2));
        assertEquals(c3, ids.get(3));
    }
}
