// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EntityQueryPageable}.
 */
public class EntityQueryPageableTest {

    private static final DataConverter dataConverter = new JacksonDataConverter();

    private EntityMetadata makeEntity(String name, String key) {
        return new EntityMetadata(
                "@" + name + "@" + key, Instant.EPOCH, 0, null, null, false, dataConverter);
    }

    @Test
    void iterator_emptyResult_yieldsNoItems() {
        EntityQuery query = new EntityQuery();
        EntityQueryPageable pageable = new EntityQueryPageable(query, q -> {
            return new EntityQueryResult(Collections.emptyList(), null);
        });

        Iterator<EntityMetadata> it = pageable.iterator();
        assertFalse(it.hasNext());
    }

    @Test
    void iterator_singlePage_yieldsAllItems() {
        List<EntityMetadata> entities = Arrays.asList(
                makeEntity("counter", "a"),
                makeEntity("counter", "b"),
                makeEntity("counter", "c"));

        EntityQuery query = new EntityQuery();
        EntityQueryPageable pageable = new EntityQueryPageable(query, q -> {
            return new EntityQueryResult(entities, null);
        });

        List<EntityMetadata> collected = new ArrayList<>();
        for (EntityMetadata e : pageable) {
            collected.add(e);
        }
        assertEquals(3, collected.size());
        assertEquals("@counter@a", collected.get(0).getInstanceId());
        assertEquals("@counter@b", collected.get(1).getInstanceId());
        assertEquals("@counter@c", collected.get(2).getInstanceId());
    }

    @Test
    void iterator_multiplePages_yieldsAllItems() {
        List<EntityMetadata> page1 = Arrays.asList(
                makeEntity("counter", "a"),
                makeEntity("counter", "b"));
        List<EntityMetadata> page2 = Arrays.asList(
                makeEntity("counter", "c"));

        EntityQuery query = new EntityQuery();
        final int[] callCount = {0};
        EntityQueryPageable pageable = new EntityQueryPageable(query, q -> {
            callCount[0]++;
            if (callCount[0] == 1) {
                return new EntityQueryResult(page1, "token1");
            } else {
                return new EntityQueryResult(page2, null);
            }
        });

        List<EntityMetadata> collected = new ArrayList<>();
        for (EntityMetadata e : pageable) {
            collected.add(e);
        }
        assertEquals(3, collected.size());
        assertEquals("@counter@a", collected.get(0).getInstanceId());
        assertEquals("@counter@c", collected.get(2).getInstanceId());
    }

    @Test
    void iterator_propagatesContinuationToken() {
        EntityQuery query = new EntityQuery();
        List<String> tokensReceived = new ArrayList<>();

        EntityQueryPageable pageable = new EntityQueryPageable(query, q -> {
            tokensReceived.add(q.getContinuationToken());
            if (tokensReceived.size() == 1) {
                return new EntityQueryResult(
                        Collections.singletonList(makeEntity("e", "1")), "pageToken");
            } else {
                return new EntityQueryResult(
                        Collections.singletonList(makeEntity("e", "2")), null);
            }
        });

        Iterator<EntityMetadata> iterator = pageable.iterator();
        while (iterator.hasNext()) {
            iterator.next();
        }

        assertEquals(2, tokensReceived.size());
        assertEquals("pageToken", tokensReceived.get(1));
    }

    @Test
    void byPage_emptyResult_yieldsSinglePage() {
        EntityQuery query = new EntityQuery();
        EntityQueryPageable pageable = new EntityQueryPageable(query, q -> {
            return new EntityQueryResult(Collections.emptyList(), null);
        });

        List<EntityQueryResult> pages = new ArrayList<>();
        for (EntityQueryResult page : pageable.byPage()) {
            pages.add(page);
        }
        assertEquals(1, pages.size());
        assertTrue(pages.get(0).getEntities().isEmpty());
    }

    @Test
    void byPage_multiplePages_yieldsAllPages() {
        List<EntityMetadata> page1 = Arrays.asList(
                makeEntity("counter", "a"),
                makeEntity("counter", "b"));
        List<EntityMetadata> page2 = Collections.singletonList(
                makeEntity("counter", "c"));

        EntityQuery query = new EntityQuery();
        final int[] callCount = {0};
        EntityQueryPageable pageable = new EntityQueryPageable(query, q -> {
            callCount[0]++;
            if (callCount[0] == 1) {
                return new EntityQueryResult(page1, "nextToken");
            } else {
                return new EntityQueryResult(page2, null);
            }
        });

        List<EntityQueryResult> pages = new ArrayList<>();
        for (EntityQueryResult page : pageable.byPage()) {
            pages.add(page);
        }
        assertEquals(2, pages.size());
        assertEquals(2, pages.get(0).getEntities().size());
        assertEquals(1, pages.get(1).getEntities().size());
    }

    @Test
    void byPage_preservesQueryParameters() {
        EntityQuery query = new EntityQuery()
                .setInstanceIdStartsWith("counter")
                .setIncludeState(true)
                .setPageSize(10);

        EntityQueryPageable pageable = new EntityQueryPageable(query, q -> {
            // Verify query parameters are preserved (instanceIdStartsWith is normalized to @counter)
            assertEquals("@counter", q.getInstanceIdStartsWith());
            assertTrue(q.isIncludeState());
            assertEquals(10, q.getPageSize());
            return new EntityQueryResult(Collections.emptyList(), null);
        });

        pageable.byPage().forEach(result -> {
            // consume
        });
    }

    @Test
    void iterator_nextWithoutHasNext_throwsWhenExhausted() {
        EntityQuery query = new EntityQuery();
        EntityQueryPageable pageable = new EntityQueryPageable(query, q -> {
            return new EntityQueryResult(Collections.emptyList(), null);
        });

        Iterator<EntityMetadata> it = pageable.iterator();
        assertThrows(NoSuchElementException.class, it::next);
    }
}
