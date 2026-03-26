// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Internal helper for intercepting protobuf messages to resolve/externalize payload fields.
 * Shared between {@link DurableTaskGrpcWorker} and {@link OrchestrationRunner}.
 */
final class PayloadInterceptionHelper {

    private PayloadInterceptionHelper() {
    }

    /**
     * Resolves externalized payload URI tokens in an OrchestratorRequest's history events.
     */
    static OrchestratorRequest resolveOrchestratorRequestPayloads(
            OrchestratorRequest request, PayloadHelper payloadHelper) {
        List<HistoryEvent> resolvedPastEvents = resolveHistoryEvents(request.getPastEventsList(), payloadHelper);
        List<HistoryEvent> resolvedNewEvents = resolveHistoryEvents(request.getNewEventsList(), payloadHelper);

        boolean pastChanged = resolvedPastEvents != request.getPastEventsList();
        boolean newChanged = resolvedNewEvents != request.getNewEventsList();

        if (!pastChanged && !newChanged) {
            return request;
        }

        OrchestratorRequest.Builder builder = request.toBuilder();
        if (pastChanged) {
            builder.clearPastEvents().addAllPastEvents(resolvedPastEvents);
        }
        if (newChanged) {
            builder.clearNewEvents().addAllNewEvents(resolvedNewEvents);
        }
        return builder.build();
    }

    /**
     * Resolves externalized payload URI token in an ActivityRequest's input.
     */
    static ActivityRequest resolveActivityRequestPayloads(
            ActivityRequest request, PayloadHelper payloadHelper) {
        if (!request.hasInput() || request.getInput().getValue().isEmpty()) {
            return request;
        }
        String resolved = payloadHelper.maybeResolve(request.getInput().getValue());
        if (resolved.equals(request.getInput().getValue())) {
            return request;
        }
        return request.toBuilder().setInput(StringValue.of(resolved)).build();
    }

    /**
     * Externalizes large payloads in an OrchestratorResponse's actions and custom status.
     */
    static OrchestratorResponse externalizeOrchestratorResponsePayloads(
            OrchestratorResponse response, PayloadHelper payloadHelper) {
        boolean changed = false;
        OrchestratorResponse.Builder responseBuilder = response.toBuilder();

        // Externalize customStatus
        if (response.hasCustomStatus() && !response.getCustomStatus().getValue().isEmpty()) {
            String externalized = payloadHelper.maybeExternalize(response.getCustomStatus().getValue());
            if (!externalized.equals(response.getCustomStatus().getValue())) {
                responseBuilder.setCustomStatus(StringValue.of(externalized));
                changed = true;
            }
        }

        // Externalize action payloads
        List<OrchestratorAction> actions = response.getActionsList();
        List<OrchestratorAction> newActions = null;
        for (int i = 0; i < actions.size(); i++) {
            OrchestratorAction action = actions.get(i);
            OrchestratorAction externalizedAction = externalizeAction(action, payloadHelper);
            if (externalizedAction != action) {
                if (newActions == null) {
                    newActions = new ArrayList<>(actions);
                }
                newActions.set(i, externalizedAction);
            }
        }

        if (newActions != null) {
            responseBuilder.clearActions().addAllActions(newActions);
            changed = true;
        }

        return changed ? responseBuilder.build() : response;
    }

    // --- Private helpers ---

    private static List<HistoryEvent> resolveHistoryEvents(
            List<HistoryEvent> events, PayloadHelper payloadHelper) {
        List<HistoryEvent> resolved = null;
        for (int i = 0; i < events.size(); i++) {
            HistoryEvent event = events.get(i);
            HistoryEvent resolvedEvent = resolveHistoryEvent(event, payloadHelper);
            if (resolvedEvent != event) {
                if (resolved == null) {
                    resolved = new ArrayList<>(events);
                }
                resolved.set(i, resolvedEvent);
            }
        }
        return resolved != null ? resolved : events;
    }

    private static HistoryEvent resolveHistoryEvent(HistoryEvent event, PayloadHelper payloadHelper) {
        switch (event.getEventTypeCase()) {
            case EXECUTIONSTARTED:
                return resolveStringValueField(event, event.getExecutionStarted().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setExecutionStarted(
                        e.getExecutionStarted().toBuilder().setInput(v)).build());
            case EXECUTIONCOMPLETED:
                return resolveStringValueField(event, event.getExecutionCompleted().getResult(), payloadHelper,
                    (e, v) -> e.toBuilder().setExecutionCompleted(
                        e.getExecutionCompleted().toBuilder().setResult(v)).build());
            case TASKCOMPLETED:
                return resolveStringValueField(event, event.getTaskCompleted().getResult(), payloadHelper,
                    (e, v) -> e.toBuilder().setTaskCompleted(
                        e.getTaskCompleted().toBuilder().setResult(v)).build());
            case TASKSCHEDULED:
                return resolveStringValueField(event, event.getTaskScheduled().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setTaskScheduled(
                        e.getTaskScheduled().toBuilder().setInput(v)).build());
            case SUBORCHESTRATIONINSTANCECREATED:
                return resolveStringValueField(event, event.getSubOrchestrationInstanceCreated().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setSubOrchestrationInstanceCreated(
                        e.getSubOrchestrationInstanceCreated().toBuilder().setInput(v)).build());
            case SUBORCHESTRATIONINSTANCECOMPLETED:
                return resolveStringValueField(event, event.getSubOrchestrationInstanceCompleted().getResult(), payloadHelper,
                    (e, v) -> e.toBuilder().setSubOrchestrationInstanceCompleted(
                        e.getSubOrchestrationInstanceCompleted().toBuilder().setResult(v)).build());
            case EVENTRAISED:
                return resolveStringValueField(event, event.getEventRaised().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setEventRaised(
                        e.getEventRaised().toBuilder().setInput(v)).build());
            case EVENTSENT:
                return resolveStringValueField(event, event.getEventSent().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setEventSent(
                        e.getEventSent().toBuilder().setInput(v)).build());
            case GENERICEVENT:
                return resolveStringValueField(event, event.getGenericEvent().getData(), payloadHelper,
                    (e, v) -> e.toBuilder().setGenericEvent(
                        e.getGenericEvent().toBuilder().setData(v)).build());
            case CONTINUEASNEW:
                return resolveStringValueField(event, event.getContinueAsNew().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setContinueAsNew(
                        e.getContinueAsNew().toBuilder().setInput(v)).build());
            case EXECUTIONTERMINATED:
                return resolveStringValueField(event, event.getExecutionTerminated().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setExecutionTerminated(
                        e.getExecutionTerminated().toBuilder().setInput(v)).build());
            case EXECUTIONSUSPENDED:
                return resolveStringValueField(event, event.getExecutionSuspended().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setExecutionSuspended(
                        e.getExecutionSuspended().toBuilder().setInput(v)).build());
            case EXECUTIONRESUMED:
                return resolveStringValueField(event, event.getExecutionResumed().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setExecutionResumed(
                        e.getExecutionResumed().toBuilder().setInput(v)).build());
            case EXECUTIONREWOUND:
                return resolveStringValueField(event, event.getExecutionRewound().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setExecutionRewound(
                        e.getExecutionRewound().toBuilder().setInput(v)).build());
            default:
                return event;
        }
    }

    @FunctionalInterface
    interface HistoryEventUpdater {
        HistoryEvent apply(HistoryEvent event, StringValue newValue);
    }

    private static HistoryEvent resolveStringValueField(HistoryEvent event, StringValue field,
                                                         PayloadHelper payloadHelper,
                                                         HistoryEventUpdater updater) {
        if (field.getValue().isEmpty()) {
            return event;
        }
        String resolved = payloadHelper.maybeResolve(field.getValue());
        if (resolved.equals(field.getValue())) {
            return event;
        }
        return updater.apply(event, StringValue.of(resolved));
    }

    private static OrchestratorAction externalizeAction(OrchestratorAction action, PayloadHelper payloadHelper) {
        switch (action.getOrchestratorActionTypeCase()) {
            case SCHEDULETASK: {
                ScheduleTaskAction inner = action.getScheduleTask();
                if (inner.hasInput()) {
                    String ext = payloadHelper.maybeExternalize(inner.getInput().getValue());
                    if (!ext.equals(inner.getInput().getValue())) {
                        return action.toBuilder().setScheduleTask(
                            inner.toBuilder().setInput(StringValue.of(ext))).build();
                    }
                }
                return action;
            }
            case CREATESUBORCHESTRATION: {
                CreateSubOrchestrationAction inner = action.getCreateSubOrchestration();
                if (inner.hasInput()) {
                    String ext = payloadHelper.maybeExternalize(inner.getInput().getValue());
                    if (!ext.equals(inner.getInput().getValue())) {
                        return action.toBuilder().setCreateSubOrchestration(
                            inner.toBuilder().setInput(StringValue.of(ext))).build();
                    }
                }
                return action;
            }
            case COMPLETEORCHESTRATION: {
                CompleteOrchestrationAction inner = action.getCompleteOrchestration();
                CompleteOrchestrationAction.Builder innerBuilder = null;
                if (inner.hasResult()) {
                    String ext = payloadHelper.maybeExternalize(inner.getResult().getValue());
                    if (!ext.equals(inner.getResult().getValue())) {
                        innerBuilder = inner.toBuilder().setResult(StringValue.of(ext));
                    }
                }
                if (inner.hasDetails()) {
                    String ext = payloadHelper.maybeExternalize(inner.getDetails().getValue());
                    if (!ext.equals(inner.getDetails().getValue())) {
                        if (innerBuilder == null) innerBuilder = inner.toBuilder();
                        innerBuilder.setDetails(StringValue.of(ext));
                    }
                }
                // Externalize carryover events
                List<HistoryEvent> carryoverEvents = inner.getCarryoverEventsList();
                if (!carryoverEvents.isEmpty()) {
                    List<HistoryEvent> externalizedEvents = externalizeHistoryEvents(carryoverEvents, payloadHelper);
                    if (externalizedEvents != carryoverEvents) {
                        if (innerBuilder == null) innerBuilder = inner.toBuilder();
                        innerBuilder.clearCarryoverEvents().addAllCarryoverEvents(externalizedEvents);
                    }
                }
                if (innerBuilder != null) {
                    return action.toBuilder().setCompleteOrchestration(innerBuilder).build();
                }
                return action;
            }
            case TERMINATEORCHESTRATION: {
                TerminateOrchestrationAction inner = action.getTerminateOrchestration();
                if (inner.hasReason()) {
                    String ext = payloadHelper.maybeExternalize(inner.getReason().getValue());
                    if (!ext.equals(inner.getReason().getValue())) {
                        return action.toBuilder().setTerminateOrchestration(
                            inner.toBuilder().setReason(StringValue.of(ext))).build();
                    }
                }
                return action;
            }
            case SENDEVENT: {
                SendEventAction inner = action.getSendEvent();
                if (inner.hasData()) {
                    String ext = payloadHelper.maybeExternalize(inner.getData().getValue());
                    if (!ext.equals(inner.getData().getValue())) {
                        return action.toBuilder().setSendEvent(
                            inner.toBuilder().setData(StringValue.of(ext))).build();
                    }
                }
                return action;
            }
            default:
                return action;
        }
    }

    private static List<HistoryEvent> externalizeHistoryEvents(
            List<HistoryEvent> events, PayloadHelper payloadHelper) {
        List<HistoryEvent> result = null;
        for (int i = 0; i < events.size(); i++) {
            HistoryEvent event = events.get(i);
            HistoryEvent externalized = externalizeHistoryEvent(event, payloadHelper);
            if (externalized != event) {
                if (result == null) {
                    result = new ArrayList<>(events);
                }
                result.set(i, externalized);
            }
        }
        return result != null ? result : events;
    }

    private static HistoryEvent externalizeHistoryEvent(HistoryEvent event, PayloadHelper payloadHelper) {
        switch (event.getEventTypeCase()) {
            case EVENTRAISED:
                return externalizeStringValueField(event, event.getEventRaised().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setEventRaised(
                        e.getEventRaised().toBuilder().setInput(v)).build());
            case EVENTSENT:
                return externalizeStringValueField(event, event.getEventSent().getInput(), payloadHelper,
                    (e, v) -> e.toBuilder().setEventSent(
                        e.getEventSent().toBuilder().setInput(v)).build());
            case GENERICEVENT:
                return externalizeStringValueField(event, event.getGenericEvent().getData(), payloadHelper,
                    (e, v) -> e.toBuilder().setGenericEvent(
                        e.getGenericEvent().toBuilder().setData(v)).build());
            default:
                return event;
        }
    }

    private static HistoryEvent externalizeStringValueField(HistoryEvent event, StringValue field,
                                                             PayloadHelper payloadHelper,
                                                             HistoryEventUpdater updater) {
        if (field.getValue().isEmpty()) {
            return event;
        }
        String externalized = payloadHelper.maybeExternalize(field.getValue());
        if (externalized.equals(field.getValue())) {
            return event;
        }
        return updater.apply(event, StringValue.of(externalized));
    }
}
