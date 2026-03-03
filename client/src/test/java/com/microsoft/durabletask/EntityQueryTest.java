// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EntityQuery}.
 */
public class EntityQueryTest {

    // region prefix normalization tests

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

    // endregion

    // region defaults

    @Test
    void defaults_allNullOrFalse() {
        EntityQuery query = new EntityQuery();
        assertNull(query.getInstanceIdStartsWith());
        assertNull(query.getLastModifiedFrom());
        assertNull(query.getLastModifiedTo());
        assertFalse(query.isIncludeState());
        assertFalse(query.isIncludeTransient());
        assertNull(query.getPageSize());
        assertNull(query.getContinuationToken());
    }

    // endregion

    // region setter/getter round-trip tests

    @Test
    void setLastModifiedFrom_roundTrip() {
        Instant time = Instant.parse("2025-01-15T10:00:00Z");
        EntityQuery query = new EntityQuery().setLastModifiedFrom(time);
        assertEquals(time, query.getLastModifiedFrom());
    }

    @Test
    void setLastModifiedTo_roundTrip() {
        Instant time = Instant.parse("2025-12-31T23:59:59Z");
        EntityQuery query = new EntityQuery().setLastModifiedTo(time);
        assertEquals(time, query.getLastModifiedTo());
    }

    @Test
    void setIncludeState_roundTrip() {
        EntityQuery query = new EntityQuery().setIncludeState(true);
        assertTrue(query.isIncludeState());
    }

    @Test
    void setIncludeTransient_roundTrip() {
        EntityQuery query = new EntityQuery().setIncludeTransient(true);
        assertTrue(query.isIncludeTransient());
    }

    @Test
    void setPageSize_roundTrip() {
        EntityQuery query = new EntityQuery().setPageSize(50);
        assertEquals(50, query.getPageSize());
    }

    @Test
    void setPageSize_null_allowed() {
        EntityQuery query = new EntityQuery().setPageSize(50).setPageSize(null);
        assertNull(query.getPageSize());
    }

    @Test
    void setContinuationToken_roundTrip() {
        EntityQuery query = new EntityQuery().setContinuationToken("token-abc");
        assertEquals("token-abc", query.getContinuationToken());
    }

    @Test
    void setContinuationToken_null_allowed() {
        EntityQuery query = new EntityQuery()
                .setContinuationToken("token")
                .setContinuationToken(null);
        assertNull(query.getContinuationToken());
    }

    // endregion

    // region fluent chaining

    @Test
    void fluentChaining_returnsSameInstance() {
        EntityQuery query = new EntityQuery();
        assertSame(query, query.setInstanceIdStartsWith("Counter"));
        assertSame(query, query.setLastModifiedFrom(Instant.now()));
        assertSame(query, query.setLastModifiedTo(Instant.now()));
        assertSame(query, query.setIncludeState(true));
        assertSame(query, query.setIncludeTransient(true));
        assertSame(query, query.setPageSize(100));
        assertSame(query, query.setContinuationToken("t"));
    }

    // endregion
}
