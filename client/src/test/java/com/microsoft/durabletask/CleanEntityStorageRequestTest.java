// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link CleanEntityStorageRequest}.
 */
public class CleanEntityStorageRequestTest {

    @Test
    void defaults_allFalseAndNull() {
        CleanEntityStorageRequest request = new CleanEntityStorageRequest();
        assertNull(request.getContinuationToken());
        assertFalse(request.isRemoveEmptyEntities());
        assertFalse(request.isReleaseOrphanedLocks());
        assertFalse(request.isContinueUntilComplete());
    }

    @Test
    void setContinuationToken_roundTrip() {
        CleanEntityStorageRequest request = new CleanEntityStorageRequest()
                .setContinuationToken("token123");
        assertEquals("token123", request.getContinuationToken());
    }

    @Test
    void setContinuationToken_null_allowed() {
        CleanEntityStorageRequest request = new CleanEntityStorageRequest()
                .setContinuationToken("token")
                .setContinuationToken(null);
        assertNull(request.getContinuationToken());
    }

    @Test
    void setRemoveEmptyEntities_roundTrip() {
        CleanEntityStorageRequest request = new CleanEntityStorageRequest()
                .setRemoveEmptyEntities(true);
        assertTrue(request.isRemoveEmptyEntities());
    }

    @Test
    void setReleaseOrphanedLocks_roundTrip() {
        CleanEntityStorageRequest request = new CleanEntityStorageRequest()
                .setReleaseOrphanedLocks(true);
        assertTrue(request.isReleaseOrphanedLocks());
    }

    @Test
    void setContinueUntilComplete_roundTrip() {
        CleanEntityStorageRequest request = new CleanEntityStorageRequest()
                .setContinueUntilComplete(true);
        assertTrue(request.isContinueUntilComplete());
    }

    @Test
    void fluentChaining_returnsSameInstance() {
        CleanEntityStorageRequest request = new CleanEntityStorageRequest();
        assertSame(request, request.setContinuationToken("t"));
        assertSame(request, request.setRemoveEmptyEntities(true));
        assertSame(request, request.setReleaseOrphanedLocks(true));
        assertSame(request, request.setContinueUntilComplete(true));
    }
}
