// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

/**
 * Export execution mode.
 */
public enum ExportMode {
    /**
     * Exports a fixed time window and completes.
     */
    BATCH(1),

    /**
     * Tails terminal instances continuously.
     */
    CONTINUOUS(2);

    private final int value;

    ExportMode(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }
}
