// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ListInstanceIdsQuery}.
 */
public class ListInstanceIdsQueryTest {

    @Test
    void getRuntimeStatusList_default_isEmptyNonNull() {
        ListInstanceIdsQuery query = new ListInstanceIdsQuery();
        assertNotNull(query.getRuntimeStatusList());
        assertTrue(query.getRuntimeStatusList().isEmpty());
    }

    @Test
    void setRuntimeStatusList_null_normalizesToEmptyList() {
        ListInstanceIdsQuery query = new ListInstanceIdsQuery().setRuntimeStatusList(null);
        assertNotNull(query.getRuntimeStatusList());
        assertTrue(query.getRuntimeStatusList().isEmpty());
    }

    @Test
    void setRuntimeStatusList_copiesInput_soExternalMutationDoesNotAffectQuery() {
        List<OrchestrationRuntimeStatus> source = new ArrayList<>();
        source.add(OrchestrationRuntimeStatus.COMPLETED);

        ListInstanceIdsQuery query = new ListInstanceIdsQuery().setRuntimeStatusList(source);
        source.add(OrchestrationRuntimeStatus.FAILED);

        assertEquals(Arrays.asList(OrchestrationRuntimeStatus.COMPLETED), query.getRuntimeStatusList());
    }

    @Test
    void setPageSize_positiveValue_updatesPageSize() {
        ListInstanceIdsQuery query = new ListInstanceIdsQuery().setPageSize(25);

        assertEquals(25, query.getPageSize());
    }

    @Test
    void setPageSize_zeroOrNegative_throws() {
        assertThrows(IllegalArgumentException.class, () -> new ListInstanceIdsQuery().setPageSize(0));
        assertThrows(IllegalArgumentException.class, () -> new ListInstanceIdsQuery().setPageSize(-1));
    }
}
