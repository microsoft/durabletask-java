// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.SignalEntityOptions;
import com.microsoft.durabletask.TaskEntityContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the {@link Schedule} entity lifecycle, timing, token invalidation, and re-arming behavior by driving its
 * operations directly against a mocked {@link TaskEntityContext}.
 */
class ScheduleEntityTest {

    private static final String SCHEDULE_ID = "s1";

    private TaskEntityContext context;
    private EntityInstanceId entityId;
    private TestableSchedule entity;

    /** Test subclass that exposes the inherited protected state/context so tests can drive operations directly. */
    static final class TestableSchedule extends Schedule {
        void init(ScheduleState initialState, TaskEntityContext ctx) {
            this.state = initialState;
            this.context = ctx;
        }

        ScheduleState currentState() {
            return this.state;
        }
    }

    @BeforeEach
    void setUp() {
        this.context = mock(TaskEntityContext.class);
        this.entityId = new EntityInstanceId(Schedule.NAME, SCHEDULE_ID);
        when(this.context.getId()).thenReturn(this.entityId);
        this.entity = new TestableSchedule();
        this.entity.init(new ScheduleState(), this.context);
    }

    private ScheduleCreationOptions options() {
        return new ScheduleCreationOptions(SCHEDULE_ID, "orch", Duration.ofSeconds(30));
    }

    @Test
    void createActivatesAndSignalsRun() {
        this.entity.createSchedule(options());

        ScheduleState state = this.entity.currentState();
        assertEquals(ScheduleStatus.ACTIVE, state.getStatus());
        assertNotNull(state.getScheduleCreatedAt());
        verify(this.context).signalEntity(this.entityId, ScheduleTransitions.RUN_SCHEDULE, state.getExecutionToken());
    }

    @Test
    void createTwiceReplacesInPlaceAndRefreshesToken() {
        this.entity.createSchedule(options());
        OffsetDateTime createdAt = this.entity.currentState().getScheduleCreatedAt();
        String firstToken = this.entity.currentState().getExecutionToken();

        this.entity.createSchedule(new ScheduleCreationOptions(SCHEDULE_ID, "orch", Duration.ofSeconds(60)));

        assertEquals(ScheduleStatus.ACTIVE, this.entity.currentState().getStatus());
        assertEquals(createdAt, this.entity.currentState().getScheduleCreatedAt());
        assertNotEquals(firstToken, this.entity.currentState().getExecutionToken());
    }

    @Test
    void pauseThenResume() {
        this.entity.createSchedule(options());

        this.entity.pauseSchedule();
        assertEquals(ScheduleStatus.PAUSED, this.entity.currentState().getStatus());
        assertNull(this.entity.currentState().getNextRunAt());

        this.entity.resumeSchedule();
        assertEquals(ScheduleStatus.ACTIVE, this.entity.currentState().getStatus());
        // One run signal on create, one on resume (both 3-arg immediate signals).
        verify(this.context, times(2)).signalEntity(eq(this.entityId), eq(ScheduleTransitions.RUN_SCHEDULE),
                any(String.class));
    }

    @Test
    void pauseWhenNotActiveThrows() {
        this.entity.createSchedule(options());
        this.entity.pauseSchedule();
        assertThrows(ScheduleInvalidTransitionException.class, () -> this.entity.pauseSchedule());
    }

    @Test
    void updateChangesConfigAndResignals() {
        this.entity.createSchedule(options());
        this.entity.updateSchedule(new ScheduleUpdateOptions().setInterval(Duration.ofSeconds(120)));

        assertEquals(Duration.ofSeconds(120),
                this.entity.currentState().getScheduleConfiguration().getInterval());
        // create + update each emit an immediate run signal.
        verify(this.context, times(2)).signalEntity(eq(this.entityId), eq(ScheduleTransitions.RUN_SCHEDULE),
                any(String.class));
    }

    @Test
    void noOpUpdateDoesNotSignal() {
        this.entity.createSchedule(options());
        this.entity.updateSchedule(new ScheduleUpdateOptions().setOrchestrationName("orch"));
        // Only the create signal; the no-op update emits nothing.
        verify(this.context, times(1)).signalEntity(eq(this.entityId), eq(ScheduleTransitions.RUN_SCHEDULE),
                any(String.class));
    }

    @Test
    void runStartsOrchestrationWhenDueAndRearms() {
        OffsetDateTime past = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);
        this.entity.createSchedule(new ScheduleCreationOptions(SCHEDULE_ID, "orch", Duration.ofSeconds(30))
                .setStartAt(past)
                .setStartImmediatelyIfLate(true));
        String token = this.entity.currentState().getExecutionToken();

        this.entity.runSchedule(token);

        assertNotNull(this.entity.currentState().getLastRunAt());
        verify(this.context).startNewOrchestration(eq("orch"), any(), any());
        // Re-arm carries the current token and a scheduled time.
        verify(this.context).signalEntity(eq(this.entityId), eq(ScheduleTransitions.RUN_SCHEDULE), eq(token),
                any(SignalEntityOptions.class));
    }

    @Test
    void runIgnoresStaleToken() {
        this.entity.createSchedule(options());
        this.entity.runSchedule("stale-token");

        assertNull(this.entity.currentState().getLastRunAt());
        verify(this.context, never()).startNewOrchestration(any(), any(), any());
        verify(this.context, never()).signalEntity(any(), any(), any(), any(SignalEntityOptions.class));
    }

    @Test
    void runWithFutureStartDoesNotStart() {
        OffsetDateTime future = OffsetDateTime.now(ZoneOffset.UTC).plusDays(1);
        this.entity.createSchedule(new ScheduleCreationOptions(SCHEDULE_ID, "orch", Duration.ofSeconds(30))
                .setStartAt(future));
        String token = this.entity.currentState().getExecutionToken();

        this.entity.runSchedule(token);

        assertNull(this.entity.currentState().getLastRunAt());
        verify(this.context, never()).startNewOrchestration(any(), any(), any());
        // Still re-arms for the future occurrence.
        verify(this.context).signalEntity(eq(this.entityId), eq(ScheduleTransitions.RUN_SCHEDULE), eq(token),
                any(SignalEntityOptions.class));
    }

    @Test
    void runPastEndTimeDeletes() {
        OffsetDateTime start = OffsetDateTime.now(ZoneOffset.UTC).minusHours(2);
        OffsetDateTime end = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);
        this.entity.createSchedule(new ScheduleCreationOptions(SCHEDULE_ID, "orch", Duration.ofSeconds(30))
                .setStartAt(start)
                .setEndAt(end));
        String token = this.entity.currentState().getExecutionToken();

        this.entity.runSchedule(token);

        verify(this.context).signalEntity(this.entityId, ScheduleTransitions.DELETE);
        verify(this.context, never()).startNewOrchestration(any(), any(), any());
    }

    @Test
    void runOnUninitializedClearsState() {
        this.entity.init(new ScheduleState(), this.context);
        this.entity.runSchedule("any");
        assertNull(this.entity.currentState());
    }
}
