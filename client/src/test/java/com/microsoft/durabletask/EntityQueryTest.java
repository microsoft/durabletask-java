// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EntityQuery} prefix normalization.
 */
public class EntityQueryTest {

    @Test
    void setInstanceIdStartsWith_rawEntityName_prependsAtAndLowercases() {
        EntityQuery query = new EntityQuery().setInstanceIdStartsWith("Counter");
        assertEquals("@counter", query.getInstanceIdStartsWith());
    }

    @Test
    void setInstanceIdStartsWith_alreadyPrefixed_lowercasesName() {
        EntityQuery query = new EntityQuery().setInstanceIdStartsWith("@Counter");
        assertEquals("@counter", query.getInstanceIdStartsWith());
    }

    @Test
    void setInstanceIdStartsWith_fullEntityId_lowercasesNameOnly() {
        EntityQuery query = new EntityQuery().setInstanceIdStartsWith("@Counter@myKey");
        assertEquals("@counter@myKey", query.getInstanceIdStartsWith());
    }

    @Test
    void setInstanceIdStartsWith_null_staysNull() {
        EntityQuery query = new EntityQuery().setInstanceIdStartsWith(null);
        assertNull(query.getInstanceIdStartsWith());
    }

    @Test
    void setInstanceIdStartsWith_alreadyLowercase_unchanged() {
        EntityQuery query = new EntityQuery().setInstanceIdStartsWith("@counter");
        assertEquals("@counter", query.getInstanceIdStartsWith());
    }

    @Test
    void setInstanceIdStartsWith_mixedCase_lowercased() {
        EntityQuery query = new EntityQuery().setInstanceIdStartsWith("MyEntity");
        assertEquals("@myentity", query.getInstanceIdStartsWith());
    }
}
