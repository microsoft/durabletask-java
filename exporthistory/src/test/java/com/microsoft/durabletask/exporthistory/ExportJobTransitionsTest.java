// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ExportJobTransitions}.
 */
class ExportJobTransitionsTest {

    @Test
    void create_fromTerminalOrPending_toActive_isValid() {
        assertTrue(ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_CREATE, ExportJobStatus.PENDING, ExportJobStatus.ACTIVE));
        assertTrue(ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_CREATE, ExportJobStatus.FAILED, ExportJobStatus.ACTIVE));
        assertTrue(ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_CREATE, ExportJobStatus.COMPLETED, ExportJobStatus.ACTIVE));
    }

    @Test
    void create_fromActive_isInvalid() {
        assertFalse(ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_CREATE, ExportJobStatus.ACTIVE, ExportJobStatus.ACTIVE));
    }

    @Test
    void create_toNonActive_isInvalid() {
        assertFalse(ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_CREATE, ExportJobStatus.PENDING, ExportJobStatus.COMPLETED));
    }

    @Test
    void markAsCompleted_fromActive_isValid() {
        assertTrue(ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_MARK_AS_COMPLETED, ExportJobStatus.ACTIVE, ExportJobStatus.COMPLETED));
    }

    @Test
    void markAsCompleted_fromNonActive_isInvalid() {
        assertFalse(ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_MARK_AS_COMPLETED, ExportJobStatus.PENDING, ExportJobStatus.COMPLETED));
    }

    @Test
    void markAsFailed_fromActive_isValid() {
        assertTrue(ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_MARK_AS_FAILED, ExportJobStatus.ACTIVE, ExportJobStatus.FAILED));
    }

    @Test
    void markAsFailed_fromTerminal_isInvalid() {
        assertFalse(ExportJobTransitions.isValidTransition(
                ExportJobTransitions.OP_MARK_AS_FAILED, ExportJobStatus.COMPLETED, ExportJobStatus.FAILED));
    }

    @Test
    void unknownOrNullOperation_isInvalid() {
        assertFalse(ExportJobTransitions.isValidTransition(
                "NotAnOperation", ExportJobStatus.ACTIVE, ExportJobStatus.COMPLETED));
        assertFalse(ExportJobTransitions.isValidTransition(
                null, ExportJobStatus.ACTIVE, ExportJobStatus.COMPLETED));
    }
}
