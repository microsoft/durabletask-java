// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;

/** API parity tests for {@link ExportHistoryStorageOptions}. */
class ExportHistoryStorageOptionsTest {

    @Test
    void formatIsConfiguredPerJobNotOnStorageOptions() {
        assertFalse(Arrays.stream(ExportHistoryStorageOptions.class.getDeclaredMethods())
                .map(Method::getName)
                .anyMatch(name -> name.equals("getFormat") || name.equals("setFormat")));
    }
}