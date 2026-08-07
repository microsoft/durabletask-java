// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**
 * Recurring scheduled tasks for the Durable Task Java SDK.
 * <p>
 * A schedule is a durable, persisted recurring instruction that starts a target orchestration on a fixed interval.
 * Enable the feature on a worker with
 * {@link com.microsoft.durabletask.scheduledtasks.ScheduledTaskWorkerExtensions#useScheduledTasks}, then manage
 * schedules from a client obtained via
 * {@link com.microsoft.durabletask.scheduledtasks.ScheduledTaskClientExtensions#useScheduledTasks}.
 * <p>
 * The feature is a behavioral port of the .NET {@code Microsoft.DurableTask.ScheduledTasks} package. Persisted entity
 * state uses the .NET wire shape so schedules interoperate with the .NET SDK and the Durable Task Scheduler
 * dashboard, subject to the connected backend treating entity names case-insensitively.
 */
package com.microsoft.durabletask.scheduledtasks;
