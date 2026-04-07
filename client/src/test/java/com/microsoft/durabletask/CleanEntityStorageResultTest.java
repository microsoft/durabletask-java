// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link CleanEntityStorageResult}.
 */
public class CleanEntityStorageResultTest {

    @Test
    void constructor_setsAllFields() {
        CleanEntityStorageResult result = new CleanEntityStorageResult("nextToken", 5, 3);
        assertEquals("nextToken", result.getContinuationToken());
        assertEquals(5, result.getEmptyEntitiesRemoved());
        assertEquals(3, result.getOrphanedLocksReleased());
    }

    @Test
    void constructor_nullContinuationToken_allowed() {
        CleanEntityStorageResult result = new CleanEntityStorageResult(null, 0, 0);
        assertNull(result.getContinuationToken());
        assertEquals(0, result.getEmptyEntitiesRemoved());
        assertEquals(0, result.getOrphanedLocksReleased());
    }

    @Test
    void constructor_zeroCounts() {
        CleanEntityStorageResult result = new CleanEntityStorageResult("token", 0, 0);
        assertEquals("token", result.getContinuationToken());
        assertEquals(0, result.getEmptyEntitiesRemoved());
        assertEquals(0, result.getOrphanedLocksReleased());
    }
}
