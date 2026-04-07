// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

/**
 * POJO state class used by {@link StateCounterEntity} for state dispatch.
 * <p>
 * When using state dispatch, the entire object is serialized/deserialized as the entity state.
 * Operations are dispatched to public methods on this class.
 */
public class StateCounterState {
    private int value;

    public StateCounterState() {
        this.value = 0;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int add(int input) {
        this.value += input;
        return this.value;
    }

    public int get() {
        return this.value;
    }

    public void reset() {
        this.value = 0;
    }
}
