// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.microsoft.durabletask.history.*;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Converts protobuf {@code HistoryEvent} messages into the public {@link HistoryEvent} domain model.
 */
final class HistoryEventConverter {
    private HistoryEventConverter() {
    }

    /**
     * Converts a protobuf history event into its domain representation.
     *
     * @param proto the protobuf history event
     * @return the domain {@link HistoryEvent}
     * @throws IllegalArgumentException if the event type is not set
     * @throws UnsupportedOperationException if the event type is not recognized
     */
    static HistoryEvent fromProto(OrchestratorService.HistoryEvent proto) {
        int id = proto.getEventId();
        Instant ts = DataConverter.getInstantFromTimestamp(proto.getTimestamp());
        switch (proto.getEventTypeCase()) {
            case EXECUTIONSTARTED: {
                OrchestratorService.ExecutionStartedEvent p = proto.getExecutionStarted();
                return new ExecutionStartedEvent(id, ts,
                        p.getName(),
                        stringOrNull(p.hasVersion(), p.getVersion()),
                        stringOrNull(p.hasInput(), p.getInput()),
                        p.hasOrchestrationInstance() ? toInstance(p.getOrchestrationInstance()) : null,
                        p.hasParentInstance() ? toParentInfo(p.getParentInstance()) : null,
                        p.hasScheduledStartTimestamp()
                                ? DataConverter.getInstantFromTimestamp(p.getScheduledStartTimestamp()) : null,
                        p.hasParentTraceContext() ? toTrace(p.getParentTraceContext()) : null,
                        stringOrNull(p.hasOrchestrationSpanID(), p.getOrchestrationSpanID()),
                        p.getTagsMap());
            }
            case EXECUTIONCOMPLETED: {
                OrchestratorService.ExecutionCompletedEvent p = proto.getExecutionCompleted();
                return new ExecutionCompletedEvent(id, ts,
                        OrchestrationRuntimeStatus.fromProtobuf(p.getOrchestrationStatus()),
                        stringOrNull(p.hasResult(), p.getResult()),
                        p.hasFailureDetails() ? new FailureDetails(p.getFailureDetails()) : null);
            }
            case EXECUTIONTERMINATED: {
                OrchestratorService.ExecutionTerminatedEvent p = proto.getExecutionTerminated();
                return new ExecutionTerminatedEvent(id, ts, stringOrNull(p.hasInput(), p.getInput()), p.getRecurse());
            }
            case TASKSCHEDULED: {
                OrchestratorService.TaskScheduledEvent p = proto.getTaskScheduled();
                return new TaskScheduledEvent(id, ts,
                        p.getName(),
                        stringOrNull(p.hasVersion(), p.getVersion()),
                        stringOrNull(p.hasInput(), p.getInput()),
                        p.hasParentTraceContext() ? toTrace(p.getParentTraceContext()) : null,
                        p.getTagsMap());
            }
            case TASKCOMPLETED: {
                OrchestratorService.TaskCompletedEvent p = proto.getTaskCompleted();
                return new TaskCompletedEvent(id, ts, p.getTaskScheduledId(), stringOrNull(p.hasResult(), p.getResult()));
            }
            case TASKFAILED: {
                OrchestratorService.TaskFailedEvent p = proto.getTaskFailed();
                return new TaskFailedEvent(id, ts, p.getTaskScheduledId(),
                        p.hasFailureDetails() ? new FailureDetails(p.getFailureDetails()) : null);
            }
            case SUBORCHESTRATIONINSTANCECREATED: {
                OrchestratorService.SubOrchestrationInstanceCreatedEvent p = proto.getSubOrchestrationInstanceCreated();
                return new SubOrchestrationInstanceCreatedEvent(id, ts,
                        p.getInstanceId(),
                        p.getName(),
                        stringOrNull(p.hasVersion(), p.getVersion()),
                        stringOrNull(p.hasInput(), p.getInput()),
                        p.hasParentTraceContext() ? toTrace(p.getParentTraceContext()) : null,
                        p.getTagsMap());
            }
            case SUBORCHESTRATIONINSTANCECOMPLETED: {
                OrchestratorService.SubOrchestrationInstanceCompletedEvent p =
                        proto.getSubOrchestrationInstanceCompleted();
                return new SubOrchestrationInstanceCompletedEvent(id, ts,
                        p.getTaskScheduledId(), stringOrNull(p.hasResult(), p.getResult()));
            }
            case SUBORCHESTRATIONINSTANCEFAILED: {
                OrchestratorService.SubOrchestrationInstanceFailedEvent p = proto.getSubOrchestrationInstanceFailed();
                return new SubOrchestrationInstanceFailedEvent(id, ts, p.getTaskScheduledId(),
                        p.hasFailureDetails() ? new FailureDetails(p.getFailureDetails()) : null);
            }
            case TIMERCREATED: {
                OrchestratorService.TimerCreatedEvent p = proto.getTimerCreated();
                return new TimerCreatedEvent(id, ts, DataConverter.getInstantFromTimestamp(p.getFireAt()));
            }
            case TIMERFIRED: {
                OrchestratorService.TimerFiredEvent p = proto.getTimerFired();
                return new TimerFiredEvent(id, ts, DataConverter.getInstantFromTimestamp(p.getFireAt()), p.getTimerId());
            }
            case ORCHESTRATORSTARTED:
                return new OrchestratorStartedEvent(id, ts);
            case ORCHESTRATORCOMPLETED:
                return new OrchestratorCompletedEvent(id, ts);
            case EVENTSENT: {
                OrchestratorService.EventSentEvent p = proto.getEventSent();
                return new EventSentEvent(id, ts, p.getInstanceId(), p.getName(),
                        stringOrNull(p.hasInput(), p.getInput()));
            }
            case EVENTRAISED: {
                OrchestratorService.EventRaisedEvent p = proto.getEventRaised();
                return new EventRaisedEvent(id, ts, p.getName(), stringOrNull(p.hasInput(), p.getInput()));
            }
            case GENERICEVENT: {
                OrchestratorService.GenericEvent p = proto.getGenericEvent();
                return new GenericEvent(id, ts, stringOrNull(p.hasData(), p.getData()));
            }
            case HISTORYSTATE: {
                OrchestratorService.HistoryStateEvent p = proto.getHistoryState();
                return new HistoryStateEvent(id, ts,
                        p.hasOrchestrationState() ? toOrchestrationState(p.getOrchestrationState()) : null);
            }
            case CONTINUEASNEW: {
                OrchestratorService.ContinueAsNewEvent p = proto.getContinueAsNew();
                return new ContinueAsNewEvent(id, ts, stringOrNull(p.hasInput(), p.getInput()));
            }
            case EXECUTIONSUSPENDED: {
                OrchestratorService.ExecutionSuspendedEvent p = proto.getExecutionSuspended();
                return new ExecutionSuspendedEvent(id, ts, stringOrNull(p.hasInput(), p.getInput()));
            }
            case EXECUTIONRESUMED: {
                OrchestratorService.ExecutionResumedEvent p = proto.getExecutionResumed();
                return new ExecutionResumedEvent(id, ts, stringOrNull(p.hasInput(), p.getInput()));
            }
            case ENTITYOPERATIONSIGNALED: {
                OrchestratorService.EntityOperationSignaledEvent p = proto.getEntityOperationSignaled();
                return new EntityOperationSignaledEvent(id, ts,
                        p.getRequestId(),
                        p.getOperation(),
                        p.hasScheduledTime() ? DataConverter.getInstantFromTimestamp(p.getScheduledTime()) : null,
                        stringOrNull(p.hasInput(), p.getInput()),
                        stringOrNull(p.hasTargetInstanceId(), p.getTargetInstanceId()));
            }
            case ENTITYOPERATIONCALLED: {
                OrchestratorService.EntityOperationCalledEvent p = proto.getEntityOperationCalled();
                return new EntityOperationCalledEvent(id, ts,
                        p.getRequestId(),
                        p.getOperation(),
                        p.hasScheduledTime() ? DataConverter.getInstantFromTimestamp(p.getScheduledTime()) : null,
                        stringOrNull(p.hasInput(), p.getInput()),
                        stringOrNull(p.hasParentInstanceId(), p.getParentInstanceId()),
                        stringOrNull(p.hasParentExecutionId(), p.getParentExecutionId()),
                        stringOrNull(p.hasTargetInstanceId(), p.getTargetInstanceId()));
            }
            case ENTITYOPERATIONCOMPLETED: {
                OrchestratorService.EntityOperationCompletedEvent p = proto.getEntityOperationCompleted();
                return new EntityOperationCompletedEvent(id, ts, p.getRequestId(),
                        stringOrNull(p.hasOutput(), p.getOutput()));
            }
            case ENTITYOPERATIONFAILED: {
                OrchestratorService.EntityOperationFailedEvent p = proto.getEntityOperationFailed();
                return new EntityOperationFailedEvent(id, ts, p.getRequestId(),
                        p.hasFailureDetails() ? new FailureDetails(p.getFailureDetails()) : null);
            }
            case ENTITYLOCKREQUESTED: {
                OrchestratorService.EntityLockRequestedEvent p = proto.getEntityLockRequested();
                return new EntityLockRequestedEvent(id, ts,
                        p.getCriticalSectionId(),
                        p.getLockSetList(),
                        p.getPosition(),
                        stringOrNull(p.hasParentInstanceId(), p.getParentInstanceId()));
            }
            case ENTITYLOCKGRANTED: {
                OrchestratorService.EntityLockGrantedEvent p = proto.getEntityLockGranted();
                return new EntityLockGrantedEvent(id, ts, p.getCriticalSectionId());
            }
            case ENTITYUNLOCKSENT: {
                OrchestratorService.EntityUnlockSentEvent p = proto.getEntityUnlockSent();
                return new EntityUnlockSentEvent(id, ts,
                        p.getCriticalSectionId(),
                        stringOrNull(p.hasParentInstanceId(), p.getParentInstanceId()),
                        stringOrNull(p.hasTargetInstanceId(), p.getTargetInstanceId()));
            }
            case EXECUTIONREWOUND: {
                OrchestratorService.ExecutionRewoundEvent p = proto.getExecutionRewound();
                return new ExecutionRewoundEvent(id, ts,
                        stringOrNull(p.hasReason(), p.getReason()),
                        stringOrNull(p.hasParentExecutionId(), p.getParentExecutionId()),
                        stringOrNull(p.hasInstanceId(), p.getInstanceId()),
                        p.hasParentTraceContext() ? toTrace(p.getParentTraceContext()) : null,
                        stringOrNull(p.hasName(), p.getName()),
                        stringOrNull(p.hasVersion(), p.getVersion()),
                        stringOrNull(p.hasInput(), p.getInput()),
                        p.hasParentInstance() ? toParentInfo(p.getParentInstance()) : null,
                        p.getTagsMap());
            }
            case EVENTTYPE_NOT_SET:
                throw new IllegalArgumentException("History event does not have an eventType set.");
            default:
                throw new UnsupportedOperationException(
                        "Deserialization of history event type " + proto.getEventTypeCase() + " is not supported.");
        }
    }

    @Nullable
    private static String stringOrNull(boolean present, com.google.protobuf.StringValue value) {
        return present ? value.getValue() : null;
    }

    private static OrchestrationInstance toInstance(OrchestratorService.OrchestrationInstance p) {
        return new OrchestrationInstance(p.getInstanceId(), stringOrNull(p.hasExecutionId(), p.getExecutionId()));
    }

    private static ParentInstanceInfo toParentInfo(OrchestratorService.ParentInstanceInfo p) {
        return new ParentInstanceInfo(
                p.getTaskScheduledId(),
                stringOrNull(p.hasName(), p.getName()),
                stringOrNull(p.hasVersion(), p.getVersion()),
                p.hasOrchestrationInstance() ? toInstance(p.getOrchestrationInstance()) : null);
    }

    private static TraceContext toTrace(OrchestratorService.TraceContext p) {
        return new TraceContext(p.getTraceParent(), stringOrNull(p.hasTraceState(), p.getTraceState()));
    }

    private static OrchestrationState toOrchestrationState(OrchestratorService.OrchestrationState p) {
        return new OrchestrationState(
                p.getInstanceId(),
                p.getName(),
                stringOrNull(p.hasVersion(), p.getVersion()),
                OrchestrationRuntimeStatus.fromProtobuf(p.getOrchestrationStatus()),
                p.hasScheduledStartTimestamp()
                        ? DataConverter.getInstantFromTimestamp(p.getScheduledStartTimestamp()) : null,
                p.hasCreatedTimestamp() ? DataConverter.getInstantFromTimestamp(p.getCreatedTimestamp()) : null,
                p.hasLastUpdatedTimestamp() ? DataConverter.getInstantFromTimestamp(p.getLastUpdatedTimestamp()) : null,
                p.hasCompletedTimestamp() ? DataConverter.getInstantFromTimestamp(p.getCompletedTimestamp()) : null,
                stringOrNull(p.hasInput(), p.getInput()),
                stringOrNull(p.hasOutput(), p.getOutput()),
                stringOrNull(p.hasCustomStatus(), p.getCustomStatus()),
                p.hasFailureDetails() ? new FailureDetails(p.getFailureDetails()) : null,
                stringOrNull(p.hasExecutionId(), p.getExecutionId()),
                stringOrNull(p.hasParentInstanceId(), p.getParentInstanceId()),
                p.getTagsMap());
    }
}
