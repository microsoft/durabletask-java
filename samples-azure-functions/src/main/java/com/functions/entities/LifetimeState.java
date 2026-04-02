// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

/**
 * State class for the {@link LifetimeEntity}.
 * <p>
 * This mirrors the .NET {@code MyState} record from {@code Lifetime.cs}.
 */
public class LifetimeState {
    private String propA;
    private int propB;

    public LifetimeState() {
    }

    public LifetimeState(String propA, int propB) {
        this.propA = propA;
        this.propB = propB;
    }

    public String getPropA() {
        return propA;
    }

    public void setPropA(String propA) {
        this.propA = propA;
    }

    public int getPropB() {
        return propB;
    }

    public void setPropB(int propB) {
        this.propB = propB;
    }

    @Override
    public String toString() {
        return "LifetimeState{propA='" + propA + "', propB=" + propB + "}";
    }
}
