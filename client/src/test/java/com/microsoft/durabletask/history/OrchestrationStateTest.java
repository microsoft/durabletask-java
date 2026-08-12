// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link OrchestrationState}.
 */
public class OrchestrationStateTest {

    @Test
    void nullTagsNormalizeToEmptyUnmodifiableMap() {
        OrchestrationState state = new OrchestrationState(
                "instance-id",
                "orchestration-name",
                null,
                OrchestrationRuntimeStatus.COMPLETED,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);

        assertNotNull(state.getTags());
        assertTrue(state.getTags().isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> state.getTags().put("env", "prod"));
    }
}
