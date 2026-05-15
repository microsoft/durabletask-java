// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import com.microsoft.durabletask.exporthistory.constants.ExportJobOperationNames;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ExportJobTransitions} state machine.
 */
class ExportJobTransitionsTest {

    // region Valid transitions

    @ParameterizedTest(name = "Create from {0} → ACTIVE should be valid")
    @MethodSource("validCreateTransitions")
    void create_fromValidState_isValid(ExportJobStatus from) {
        assertTrue(ExportJobTransitions.isValidTransition(
                ExportJobOperationNames.CREATE, from, ExportJobStatus.ACTIVE));
    }

    static Stream<ExportJobStatus> validCreateTransitions() {
        return Stream.of(ExportJobStatus.PENDING, ExportJobStatus.FAILED, ExportJobStatus.COMPLETED);
    }

    @Test
    void markAsCompleted_fromActive_isValid() {
        assertTrue(ExportJobTransitions.isValidTransition(
                ExportJobOperationNames.MARK_AS_COMPLETED, ExportJobStatus.ACTIVE, ExportJobStatus.COMPLETED));
    }

    @Test
    void markAsFailed_fromActive_isValid() {
        assertTrue(ExportJobTransitions.isValidTransition(
                ExportJobOperationNames.MARK_AS_FAILED, ExportJobStatus.ACTIVE, ExportJobStatus.FAILED));
    }

    // endregion

    // region Invalid transitions

    @Test
    void create_fromActive_isInvalid() {
        assertFalse(ExportJobTransitions.isValidTransition(
                ExportJobOperationNames.CREATE, ExportJobStatus.ACTIVE, ExportJobStatus.ACTIVE));
    }

    @Test
    void create_toNonActive_isInvalid() {
        assertFalse(ExportJobTransitions.isValidTransition(
                ExportJobOperationNames.CREATE, ExportJobStatus.PENDING, ExportJobStatus.COMPLETED));
    }

    @ParameterizedTest(name = "MarkAsCompleted from {0} should be invalid")
    @MethodSource("nonActiveStates")
    void markAsCompleted_fromNonActive_isInvalid(ExportJobStatus from) {
        assertFalse(ExportJobTransitions.isValidTransition(
                ExportJobOperationNames.MARK_AS_COMPLETED, from, ExportJobStatus.COMPLETED));
    }

    @ParameterizedTest(name = "MarkAsFailed from {0} should be invalid")
    @MethodSource("nonActiveStates")
    void markAsFailed_fromNonActive_isInvalid(ExportJobStatus from) {
        assertFalse(ExportJobTransitions.isValidTransition(
                ExportJobOperationNames.MARK_AS_FAILED, from, ExportJobStatus.FAILED));
    }

    static Stream<ExportJobStatus> nonActiveStates() {
        return Stream.of(ExportJobStatus.PENDING, ExportJobStatus.COMPLETED, ExportJobStatus.FAILED);
    }

    @Test
    void unknownOperation_isInvalid() {
        assertFalse(ExportJobTransitions.isValidTransition(
                "UnknownOp", ExportJobStatus.ACTIVE, ExportJobStatus.COMPLETED));
    }

    @Test
    void markAsCompleted_toWrongTarget_isInvalid() {
        assertFalse(ExportJobTransitions.isValidTransition(
                ExportJobOperationNames.MARK_AS_COMPLETED, ExportJobStatus.ACTIVE, ExportJobStatus.FAILED));
    }

    @Test
    void markAsFailed_toWrongTarget_isInvalid() {
        assertFalse(ExportJobTransitions.isValidTransition(
                ExportJobOperationNames.MARK_AS_FAILED, ExportJobStatus.ACTIVE, ExportJobStatus.COMPLETED));
    }

    // endregion

    // region Exhaustive: all state combinations for Create

    @ParameterizedTest(name = "Create: {0} → {1}")
    @MethodSource("allCreateCombinations")
    void create_allCombinations(ExportJobStatus from, ExportJobStatus to, boolean expected) {
        assertEquals(expected, ExportJobTransitions.isValidTransition(
                ExportJobOperationNames.CREATE, from, to));
    }

    static Stream<Arguments> allCreateCombinations() {
        return Stream.of(
                // PENDING → * : only ACTIVE is valid
                Arguments.of(ExportJobStatus.PENDING, ExportJobStatus.ACTIVE, true),
                Arguments.of(ExportJobStatus.PENDING, ExportJobStatus.COMPLETED, false),
                Arguments.of(ExportJobStatus.PENDING, ExportJobStatus.FAILED, false),
                Arguments.of(ExportJobStatus.PENDING, ExportJobStatus.PENDING, false),
                // ACTIVE → * : none valid for Create
                Arguments.of(ExportJobStatus.ACTIVE, ExportJobStatus.ACTIVE, false),
                Arguments.of(ExportJobStatus.ACTIVE, ExportJobStatus.COMPLETED, false),
                Arguments.of(ExportJobStatus.ACTIVE, ExportJobStatus.FAILED, false),
                Arguments.of(ExportJobStatus.ACTIVE, ExportJobStatus.PENDING, false),
                // FAILED → * : only ACTIVE is valid
                Arguments.of(ExportJobStatus.FAILED, ExportJobStatus.ACTIVE, true),
                Arguments.of(ExportJobStatus.FAILED, ExportJobStatus.COMPLETED, false),
                Arguments.of(ExportJobStatus.FAILED, ExportJobStatus.FAILED, false),
                Arguments.of(ExportJobStatus.FAILED, ExportJobStatus.PENDING, false),
                // COMPLETED → * : only ACTIVE is valid
                Arguments.of(ExportJobStatus.COMPLETED, ExportJobStatus.ACTIVE, true),
                Arguments.of(ExportJobStatus.COMPLETED, ExportJobStatus.COMPLETED, false),
                Arguments.of(ExportJobStatus.COMPLETED, ExportJobStatus.FAILED, false),
                Arguments.of(ExportJobStatus.COMPLETED, ExportJobStatus.PENDING, false)
        );
    }

    // endregion
}
