// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.HistoryEvent;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps proto {@link HistoryEvent} instances to transport-neutral {@link OrchestrationHistoryEvent} instances.
 * <p>
 * This mapper extracts the event type from the proto oneof field and flattens the event-specific
 * message fields into a {@code Map<String, Object>} for serialization-independent consumption.
 */
final class OrchestrationHistoryEventMapper {

    private static final Map<HistoryEvent.EventTypeCase, String> EVENT_TYPE_NAMES = new HashMap<>();

    static {
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.EXECUTIONSTARTED, "ExecutionStarted");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.EXECUTIONCOMPLETED, "ExecutionCompleted");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.EXECUTIONTERMINATED, "ExecutionTerminated");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.TASKSCHEDULED, "TaskScheduled");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.TASKCOMPLETED, "TaskCompleted");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.TASKFAILED, "TaskFailed");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.SUBORCHESTRATIONINSTANCECREATED, "SubOrchestrationInstanceCreated");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.SUBORCHESTRATIONINSTANCECOMPLETED, "SubOrchestrationInstanceCompleted");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.SUBORCHESTRATIONINSTANCEFAILED, "SubOrchestrationInstanceFailed");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.TIMERCREATED, "TimerCreated");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.TIMERFIRED, "TimerFired");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.ORCHESTRATORSTARTED, "OrchestratorStarted");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.ORCHESTRATORCOMPLETED, "OrchestratorCompleted");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.EVENTSENT, "EventSent");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.EVENTRAISED, "EventRaised");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.GENERICEVENT, "GenericEvent");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.HISTORYSTATE, "HistoryState");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.CONTINUEASNEW, "ContinueAsNew");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.EXECUTIONSUSPENDED, "ExecutionSuspended");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.EXECUTIONRESUMED, "ExecutionResumed");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.ENTITYOPERATIONSIGNALED, "EntityOperationSignaled");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.ENTITYOPERATIONCALLED, "EntityOperationCalled");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.ENTITYOPERATIONCOMPLETED, "EntityOperationCompleted");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.ENTITYOPERATIONFAILED, "EntityOperationFailed");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.ENTITYLOCKREQUESTED, "EntityLockRequested");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.ENTITYLOCKGRANTED, "EntityLockGranted");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.ENTITYUNLOCKSENT, "EntityUnlockSent");
        EVENT_TYPE_NAMES.put(HistoryEvent.EventTypeCase.EXECUTIONREWOUND, "ExecutionRewound");
    }

    private OrchestrationHistoryEventMapper() {
    }

    /**
     * Converts a proto {@link HistoryEvent} to an {@link OrchestrationHistoryEvent}.
     *
     * @param proto the proto history event
     * @return the transport-neutral history event
     */
    static OrchestrationHistoryEvent fromProto(HistoryEvent proto) {
        if (proto == null) {
            throw new IllegalArgumentException("proto must not be null.");
        }

        int eventId = proto.getEventId();
        Instant timestamp = toInstant(proto.getTimestamp());

        // Determine the event type and extract its fields from the oneof
        HistoryEvent.EventTypeCase eventTypeCase = proto.getEventTypeCase();
        String eventType = EVENT_TYPE_NAMES.getOrDefault(eventTypeCase, "Unknown");
        Map<String, Object> data = extractEventData(proto, eventTypeCase);

        return new OrchestrationHistoryEvent(eventId, timestamp, eventType, data);
    }

    private static Instant toInstant(Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    private static Map<String, Object> extractEventData(
            HistoryEvent proto,
            HistoryEvent.EventTypeCase eventTypeCase) {
        Map<String, Object> data = new LinkedHashMap<>();

        // Get the specific event message from the oneof field
        Message eventMessage = getEventMessage(proto, eventTypeCase);
        if (eventMessage == null) {
            return data;
        }

        // Flatten all set fields from the event-specific message into the map
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry :
                eventMessage.getAllFields().entrySet()) {
            Descriptors.FieldDescriptor field = entry.getKey();
            Object value = entry.getValue();

            // Convert proto types to simple Java types
            data.put(field.getJsonName(), convertProtoValue(value));
        }

        return data;
    }

    private static Message getEventMessage(HistoryEvent proto, HistoryEvent.EventTypeCase eventTypeCase) {
        switch (eventTypeCase) {
            case EXECUTIONSTARTED: return proto.getExecutionStarted();
            case EXECUTIONCOMPLETED: return proto.getExecutionCompleted();
            case EXECUTIONTERMINATED: return proto.getExecutionTerminated();
            case TASKSCHEDULED: return proto.getTaskScheduled();
            case TASKCOMPLETED: return proto.getTaskCompleted();
            case TASKFAILED: return proto.getTaskFailed();
            case SUBORCHESTRATIONINSTANCECREATED: return proto.getSubOrchestrationInstanceCreated();
            case SUBORCHESTRATIONINSTANCECOMPLETED: return proto.getSubOrchestrationInstanceCompleted();
            case SUBORCHESTRATIONINSTANCEFAILED: return proto.getSubOrchestrationInstanceFailed();
            case TIMERCREATED: return proto.getTimerCreated();
            case TIMERFIRED: return proto.getTimerFired();
            case ORCHESTRATORSTARTED: return proto.getOrchestratorStarted();
            case ORCHESTRATORCOMPLETED: return proto.getOrchestratorCompleted();
            case EVENTSENT: return proto.getEventSent();
            case EVENTRAISED: return proto.getEventRaised();
            case GENERICEVENT: return proto.getGenericEvent();
            case HISTORYSTATE: return proto.getHistoryState();
            case CONTINUEASNEW: return proto.getContinueAsNew();
            case EXECUTIONSUSPENDED: return proto.getExecutionSuspended();
            case EXECUTIONRESUMED: return proto.getExecutionResumed();
            case EXECUTIONREWOUND: return proto.getExecutionRewound();
            case ENTITYOPERATIONSIGNALED: return proto.getEntityOperationSignaled();
            case ENTITYOPERATIONCALLED: return proto.getEntityOperationCalled();
            case ENTITYOPERATIONCOMPLETED: return proto.getEntityOperationCompleted();
            case ENTITYOPERATIONFAILED: return proto.getEntityOperationFailed();
            case ENTITYLOCKREQUESTED: return proto.getEntityLockRequested();
            case ENTITYLOCKGRANTED: return proto.getEntityLockGranted();
            case ENTITYUNLOCKSENT: return proto.getEntityUnlockSent();
            default: return null;
        }
    }

    private static Object convertProtoValue(Object value) {
        if (value instanceof Timestamp) {
            Timestamp ts = (Timestamp) value;
            return toInstant(ts).toString();
        }
        if (value instanceof com.google.protobuf.StringValue) {
            return ((com.google.protobuf.StringValue) value).getValue();
        }
        if (value instanceof Descriptors.EnumValueDescriptor) {
            return ((Descriptors.EnumValueDescriptor) value).getName();
        }
        if (value instanceof com.google.protobuf.ProtocolMessageEnum) {
            return ((com.google.protobuf.ProtocolMessageEnum) value).getValueDescriptor().getName();
        }
        if (value instanceof Message) {
            // Recursively flatten nested messages
            Map<String, Object> nested = new LinkedHashMap<>();
            for (Map.Entry<Descriptors.FieldDescriptor, Object> entry :
                    ((Message) value).getAllFields().entrySet()) {
                nested.put(entry.getKey().getJsonName(), convertProtoValue(entry.getValue()));
            }
            return nested;
        }
        // Primitive types (String, Integer, Boolean, etc.) pass through
        return value;
    }
}
