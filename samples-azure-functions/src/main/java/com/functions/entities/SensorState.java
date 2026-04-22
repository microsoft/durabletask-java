// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

/**
 * State for the {@link SensorEntity} — tracks the last recorded temperature and total readings.
 */
class SensorState {
    public double lastReading;
    public int totalReadings;
}

/**
 * State for the {@link AggregatorEntity} — tracks sum and count for computing averages.
 */
class AggregatorState {
    public double sum;
    public int count;

    public double getAverage() {
        return count > 0 ? sum / count : 0;
    }
}
