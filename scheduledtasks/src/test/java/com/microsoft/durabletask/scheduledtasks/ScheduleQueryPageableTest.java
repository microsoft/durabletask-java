// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies {@link ScheduleQueryPageable} auto-paginates and, critically, continues across an empty filtered page
 * that still carries a continuation token.
 */
class ScheduleQueryPageableTest {

    private static ScheduleDescription describe(String scheduleId) {
        ScheduleState state = new ScheduleState();
        state.setStatus(ScheduleStatus.ACTIVE);
        state.setScheduleConfiguration(ScheduleConfiguration.fromCreateOptions(
                new ScheduleCreationOptions(scheduleId, "orch", Duration.ofSeconds(30))));
        return ScheduleDescription.fromState(scheduleId, state);
    }

    private static Function<String, ScheduleQueryResult> threePageFetcher() {
        return token -> {
            if (token == null) {
                return new ScheduleQueryResult(Collections.singletonList(describe("a")), "p2");
            }
            if (token.equals("p2")) {
                // An empty page that still has more pages after it.
                return new ScheduleQueryResult(Collections.<ScheduleDescription>emptyList(), "p3");
            }
            if (token.equals("p3")) {
                return new ScheduleQueryResult(Collections.singletonList(describe("b")), null);
            }
            throw new IllegalStateException("unexpected continuation token: " + token);
        };
    }

    @Test
    void itemIteratorContinuesAcrossEmptyPage() {
        ScheduleQueryPageable pageable = new ScheduleQueryPageable(null, threePageFetcher());

        List<String> ids = new ArrayList<>();
        for (ScheduleDescription description : pageable) {
            ids.add(description.getScheduleId());
        }

        assertEquals(Arrays.asList("a", "b"), ids);
    }

    @Test
    void byPageYieldsEveryPageIncludingEmpty() {
        ScheduleQueryPageable pageable = new ScheduleQueryPageable(null, threePageFetcher());

        int pageCount = 0;
        int total = 0;
        for (ScheduleQueryResult page : pageable.byPage()) {
            pageCount++;
            total += page.getDescriptions().size();
        }

        assertEquals(3, pageCount);
        assertEquals(2, total);
    }

    @Test
    void emptyResultTerminates() {
        Function<String, ScheduleQueryResult> fetcher =
                token -> new ScheduleQueryResult(Collections.<ScheduleDescription>emptyList(), null);
        ScheduleQueryPageable pageable = new ScheduleQueryPageable(null, fetcher);

        Iterator<ScheduleDescription> iterator = pageable.iterator();
        assertEquals(false, iterator.hasNext());
    }
}
