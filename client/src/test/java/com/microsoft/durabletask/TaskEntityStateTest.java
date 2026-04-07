// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TaskEntityState}.
 */
public class TaskEntityStateTest {

    private static final DataConverter dataConverter = new JacksonDataConverter();

    @Test
    void getState_returnsDeserializedValue() {
        TaskEntityState state = new TaskEntityState(dataConverter, "42");
        assertEquals(42, state.getState(Integer.class));
    }

    @Test
    void getState_returnsNullWhenNoState() {
        TaskEntityState state = new TaskEntityState(dataConverter, null);
        assertNull(state.getState(Integer.class));
    }

    @Test
    void getStateWithDefault_returnsValueWhenStateExists() {
        TaskEntityState state = new TaskEntityState(dataConverter, "42");
        assertEquals(42, state.getState(Integer.class, 0));
    }

    @Test
    void getStateWithDefault_returnsDefaultWhenNoState() {
        TaskEntityState state = new TaskEntityState(dataConverter, null);
        assertEquals(0, state.getState(Integer.class, 0));
    }

    @Test
    void getStateWithDefault_returnsDefaultAfterDelete() {
        TaskEntityState state = new TaskEntityState(dataConverter, "42");
        state.deleteState();
        assertEquals(99, state.getState(Integer.class, 99));
    }

    @Test
    void hasState_trueWhenStateExists() {
        TaskEntityState state = new TaskEntityState(dataConverter, "\"hello\"");
        assertTrue(state.hasState());
    }

    @Test
    void hasState_falseWhenNoState() {
        TaskEntityState state = new TaskEntityState(dataConverter, null);
        assertFalse(state.hasState());
    }

    @Test
    void setState_updatesState() {
        TaskEntityState state = new TaskEntityState(dataConverter, null);
        state.setState(100);
        assertTrue(state.hasState());
        assertEquals(100, state.getState(Integer.class));
    }

    @Test
    void deleteState_removesState() {
        TaskEntityState state = new TaskEntityState(dataConverter, "42");
        state.deleteState();
        assertFalse(state.hasState());
        assertNull(state.getState(Integer.class));
    }

    @Test
    void rollback_restoresCommittedState() {
        TaskEntityState state = new TaskEntityState(dataConverter, "42");
        state.commit();
        state.setState(100);
        assertEquals(100, state.getState(Integer.class));
        state.rollback();
        assertEquals(42, state.getState(Integer.class));
    }
}
