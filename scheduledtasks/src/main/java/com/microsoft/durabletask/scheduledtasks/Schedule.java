// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.microsoft.durabletask.AbstractTaskEntity;
import com.microsoft.durabletask.NewOrchestrationInstanceOptions;
import com.microsoft.durabletask.SignalEntityOptions;
import com.microsoft.durabletask.TaskEntityOperation;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Durable entity that owns a schedule's configuration and runtime state and drives recurring execution.
 * <p>
 * The entity is self-driving: after creation, update, or resume it signals {@code RunSchedule}, which starts the
 * target orchestration when an occurrence is due and re-arms itself with a delayed self-signal for the next
 * occurrence. This is a direct port of the .NET {@code Schedule} entity. Operations are dispatched by method name
 * (case-insensitive): {@code CreateSchedule}, {@code UpdateSchedule}, {@code PauseSchedule}, {@code ResumeSchedule},
 * {@code RunSchedule}, and the implicit {@code delete}.
 */
public class Schedule extends AbstractTaskEntity<ScheduleState> {

    /** The registered entity name. Matches the .NET {@code nameof(Schedule)} entity name. */
    public static final String NAME = "Schedule";

    private static final Logger LOGGER = Logger.getLogger(Schedule.class.getName());

    @Override
    protected Class<ScheduleState> getStateType() {
        return ScheduleState.class;
    }

    @Override
    protected ScheduleState initializeState(TaskEntityOperation operation) {
        return new ScheduleState();
    }

    /**
     * Creates a new schedule, or replaces the configuration of an existing one in place.
     *
     * @param options the creation options
     */
    public void createSchedule(ScheduleCreationOptions options) {
        ScheduleStatus current = this.state.getStatus();
        if (!ScheduleTransitions.isValidTransition(
                ScheduleTransitions.CREATE_SCHEDULE, current, ScheduleStatus.ACTIVE)) {
            throw new ScheduleInvalidTransitionException(
                    options == null ? "" : options.getScheduleId(),
                    current, ScheduleStatus.ACTIVE, ScheduleTransitions.CREATE_SCHEDULE);
        }
        if (options == null) {
            throw new ScheduleClientValidationException("", "Schedule creation options cannot be null.");
        }

        boolean alreadyExists = this.state.getScheduleCreatedAt() != null;
        this.state.setScheduleConfiguration(ScheduleConfiguration.fromCreateOptions(options));

        OffsetDateTime now = now();
        if (alreadyExists) {
            this.state.setScheduleLastModifiedAt(now);
            this.state.refreshExecutionToken();
            this.state.setNextRunAt(null);
        } else {
            this.state.setStatus(ScheduleStatus.ACTIVE);
            this.state.setScheduleCreatedAt(now);
            this.state.setScheduleLastModifiedAt(now);
        }

        LOGGER.log(Level.INFO, "Created schedule {0}", options.getScheduleId());

        // Signal RunSchedule and let it decide whether to run now or later.
        this.context.signalEntity(this.context.getId(), ScheduleTransitions.RUN_SCHEDULE,
                this.state.getExecutionToken());
    }

    /**
     * Updates an existing schedule's configuration in place.
     *
     * @param options the update options
     */
    public void updateSchedule(ScheduleUpdateOptions options) {
        ScheduleStatus current = this.state.getStatus();
        if (!ScheduleTransitions.isValidTransition(ScheduleTransitions.UPDATE_SCHEDULE, current, current)) {
            throw new ScheduleInvalidTransitionException(
                    scheduleIdOrEmpty(), current, current, ScheduleTransitions.UPDATE_SCHEDULE);
        }
        if (options == null) {
            throw new ScheduleClientValidationException(scheduleIdOrEmpty(),
                    "Schedule update options cannot be null.");
        }
        ScheduleConfiguration config = this.state.getScheduleConfiguration();
        if (config == null) {
            throw new IllegalStateException("Schedule configuration is missing.");
        }

        Set<String> updated = config.update(options);
        if (updated.isEmpty()) {
            return;
        }

        this.state.setScheduleLastModifiedAt(now());
        if (updated.contains("StartAt") || updated.contains("Interval")
                || updated.contains("StartImmediatelyIfLate")) {
            this.state.setNextRunAt(null);
        }
        this.state.refreshExecutionToken();
        LOGGER.log(Level.INFO, "Updated schedule {0}", config.getScheduleId());

        if (this.state.getStatus() == ScheduleStatus.ACTIVE) {
            this.context.signalEntity(this.context.getId(), ScheduleTransitions.RUN_SCHEDULE,
                    this.state.getExecutionToken());
        }
    }

    /** Pauses the schedule. */
    public void pauseSchedule() {
        ScheduleStatus current = this.state.getStatus();
        if (!ScheduleTransitions.isValidTransition(
                ScheduleTransitions.PAUSE_SCHEDULE, current, ScheduleStatus.PAUSED)) {
            throw new ScheduleInvalidTransitionException(
                    scheduleIdOrEmpty(), current, ScheduleStatus.PAUSED, ScheduleTransitions.PAUSE_SCHEDULE);
        }
        if (this.state.getScheduleConfiguration() == null) {
            throw new IllegalStateException("Schedule configuration is missing.");
        }

        this.state.setStatus(ScheduleStatus.PAUSED);
        this.state.setNextRunAt(null);
        this.state.refreshExecutionToken();
        LOGGER.log(Level.INFO, "Paused schedule {0}", scheduleIdOrEmpty());
    }

    /** Resumes a paused schedule. */
    public void resumeSchedule() {
        ScheduleStatus current = this.state.getStatus();
        if (!ScheduleTransitions.isValidTransition(
                ScheduleTransitions.RESUME_SCHEDULE, current, ScheduleStatus.ACTIVE)) {
            throw new ScheduleInvalidTransitionException(
                    scheduleIdOrEmpty(), current, ScheduleStatus.ACTIVE, ScheduleTransitions.RESUME_SCHEDULE);
        }
        ScheduleConfiguration config = this.state.getScheduleConfiguration();
        if (config == null) {
            throw new IllegalStateException("Schedule configuration is missing.");
        }

        this.state.setStatus(ScheduleStatus.ACTIVE);
        this.state.setNextRunAt(null);
        LOGGER.log(Level.INFO, "Resumed schedule {0}", config.getScheduleId());

        // Resume preserves the current execution token, matching the .NET SDK.
        this.context.signalEntity(this.context.getId(), ScheduleTransitions.RUN_SCHEDULE,
                this.state.getExecutionToken());
    }

    /**
     * Heartbeat operation: starts the target orchestration when an occurrence is due, then re-arms itself with a
     * delayed self-signal for the next occurrence.
     *
     * @param executionToken the execution token carried by the signal that scheduled this heartbeat
     */
    public void runSchedule(String executionToken) {
        if (this.state.getStatus() == ScheduleStatus.UNINITIALIZED) {
            // This signal is no longer useful since the schedule was deleted; delete the state again.
            this.state = null;
            return;
        }

        ScheduleConfiguration config = this.state.getScheduleConfiguration();
        if (config == null) {
            throw new IllegalStateException("Schedule configuration is missing.");
        }

        if (!Objects.equals(executionToken, this.state.getExecutionToken())) {
            LOGGER.log(Level.FINE, "Ignoring stale run signal for schedule {0}", config.getScheduleId());
            return;
        }

        if (this.state.getStatus() != ScheduleStatus.ACTIVE) {
            throw new IllegalStateException("Schedule must be in Active status to run.");
        }

        OffsetDateTime now = now();
        OffsetDateTime endAt = config.getEndAt();
        if (endAt != null && now.toInstant().isAfter(endAt.toInstant())) {
            LOGGER.log(Level.INFO, "Schedule {0} has passed its end time; deleting.", config.getScheduleId());
            this.state.setNextRunAt(null);
            this.context.signalEntity(this.context.getId(), ScheduleTransitions.DELETE);
            return;
        }

        this.state.setNextRunAt(determineNextRunTime(config));

        // Recompute the current time after determining the next run so the due-check is never racing a slightly
        // earlier timestamp, matching the .NET SDK.
        OffsetDateTime currentTime = now();
        if (!this.state.getNextRunAt().toInstant().isAfter(currentTime.toInstant())) {
            startOrchestration(config, this.state.getNextRunAt());
            this.state.setLastRunAt(this.state.getNextRunAt());
            this.state.setNextRunAt(null);
            this.state.setNextRunAt(determineNextRunTime(config));
        }

        this.context.signalEntity(
                this.context.getId(),
                ScheduleTransitions.RUN_SCHEDULE,
                this.state.getExecutionToken(),
                new SignalEntityOptions().setScheduledTime(this.state.getNextRunAt().toInstant()));
    }

    private void startOrchestration(ScheduleConfiguration config, OffsetDateTime scheduledRunTime) {
        String instanceId = config.getOrchestrationInstanceId();
        if (instanceId == null || instanceId.isEmpty()) {
            // Match the .NET default instance ID: "{scheduleId}-{scheduledRunTime:o}".
            instanceId = config.getScheduleId() + "-" + DotNetDateTimeOffset.format(scheduledRunTime);
        }

        try {
            this.context.startNewOrchestration(
                    config.getOrchestrationName(),
                    config.getOrchestrationInput(),
                    new NewOrchestrationInstanceOptions().setInstanceId(instanceId));
        } catch (RuntimeException ex) {
            // Match the .NET SDK: a failure to schedule the target orchestration is logged and swallowed so the
            // schedule continues to re-arm rather than failing the heartbeat operation.
            LOGGER.log(Level.WARNING,
                    "Failed to start orchestration '" + config.getOrchestrationName() + "' for schedule "
                            + config.getScheduleId(), ex);
        }
    }

    private OffsetDateTime determineNextRunTime(ScheduleConfiguration config) {
        if (this.state.getNextRunAt() != null) {
            return this.state.getNextRunAt();
        }

        OffsetDateTime now = now();
        OffsetDateTime startTime = config.getStartAt() != null
                ? config.getStartAt()
                : (this.state.getScheduleCreatedAt() != null ? this.state.getScheduleCreatedAt() : now);

        Duration sinceStart = Duration.between(startTime.toInstant(), now.toInstant());
        if (sinceStart.isNegative()) {
            return startTime;
        }

        boolean isFirstRun = this.state.getLastRunAt() == null;
        if (isFirstRun && config.isStartImmediatelyIfLate()) {
            return now;
        }

        // Tick-based integer division matches the .NET TimeSpan.Ticks arithmetic exactly.
        long sinceTicks = DotNetTimeSpan.toTicks(sinceStart);
        long intervalTicks = DotNetTimeSpan.toTicks(config.getInterval());
        long intervalsElapsed = sinceTicks / intervalTicks;
        return startTime.plus(config.getInterval().multipliedBy(intervalsElapsed + 1));
    }

    private String scheduleIdOrEmpty() {
        ScheduleConfiguration config = this.state.getScheduleConfiguration();
        return config == null ? "" : config.getScheduleId();
    }

    private static OffsetDateTime now() {
        return OffsetDateTime.now(ZoneOffset.UTC);
    }
}
