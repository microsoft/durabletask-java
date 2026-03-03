// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EntityQueryResult}.
 */
public class EntityQueryResultTest {

    private static final DataConverter dataConverter = new JacksonDataConverter();

    @Test
    void constructor_setsEntitiesAndToken() {
        List<EntityMetadata> entities = Arrays.asList(
                new EntityMetadata("@counter@c1", java.time.Instant.EPOCH, 0, null, null, dataConverter),
                new EntityMetadata("@counter@c2", java.time.Instant.EPOCH, 0, null, null, dataConverter));

        EntityQueryResult result = new EntityQueryResult(entities, "nextPage");

        assertEquals(2, result.getEntities().size());
        assertEquals("nextPage", result.getContinuationToken());
    }

    @Test
    void constructor_nullContinuationToken_meansNoMorePages() {
        EntityQueryResult result = new EntityQueryResult(Collections.emptyList(), null);

        assertTrue(result.getEntities().isEmpty());
        assertNull(result.getContinuationToken());
    }

    @Test
    void getEntities_returnsProvidedList() {
        EntityMetadata metadata = new EntityMetadata(
                "@counter@c1", java.time.Instant.EPOCH, 0, null, "42", dataConverter);
        EntityQueryResult result = new EntityQueryResult(Collections.singletonList(metadata), null);

        assertEquals(1, result.getEntities().size());
        assertEquals("@counter@c1", result.getEntities().get(0).getInstanceId());
    }
}
