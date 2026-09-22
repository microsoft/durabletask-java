// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.CharacterEscapes;
import com.fasterxml.jackson.core.io.SerializedString;
import com.microsoft.durabletask.FailureDetails;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import com.microsoft.durabletask.history.ContinueAsNewEvent;
import com.microsoft.durabletask.history.EntityLockGrantedEvent;
import com.microsoft.durabletask.history.EntityLockRequestedEvent;
import com.microsoft.durabletask.history.EntityOperationCalledEvent;
import com.microsoft.durabletask.history.EntityOperationCompletedEvent;
import com.microsoft.durabletask.history.EntityOperationFailedEvent;
import com.microsoft.durabletask.history.EntityOperationSignaledEvent;
import com.microsoft.durabletask.history.EntityUnlockSentEvent;
import com.microsoft.durabletask.history.EventRaisedEvent;
import com.microsoft.durabletask.history.EventSentEvent;
import com.microsoft.durabletask.history.ExecutionCompletedEvent;
import com.microsoft.durabletask.history.ExecutionResumedEvent;
import com.microsoft.durabletask.history.ExecutionStartedEvent;
import com.microsoft.durabletask.history.ExecutionSuspendedEvent;
import com.microsoft.durabletask.history.ExecutionTerminatedEvent;
import com.microsoft.durabletask.history.GenericEvent;
import com.microsoft.durabletask.history.HistoryEvent;
import com.microsoft.durabletask.history.HistoryStateEvent;
import com.microsoft.durabletask.history.OrchestrationInstance;
import com.microsoft.durabletask.history.OrchestrationState;
import com.microsoft.durabletask.history.ParentInstanceInfo;
import com.microsoft.durabletask.history.SubOrchestrationInstanceCompletedEvent;
import com.microsoft.durabletask.history.SubOrchestrationInstanceCreatedEvent;
import com.microsoft.durabletask.history.SubOrchestrationInstanceFailedEvent;
import com.microsoft.durabletask.history.TaskCompletedEvent;
import com.microsoft.durabletask.history.TaskFailedEvent;
import com.microsoft.durabletask.history.TaskScheduledEvent;
import com.microsoft.durabletask.history.TimerCreatedEvent;
import com.microsoft.durabletask.history.TimerFiredEvent;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Serializes the {@code com.microsoft.durabletask.history} domain model to the export wire format.
 * <p>
 * Each event is written as a single JSON object with a leading {@code eventType} discriminator, the type-specific
 * fields, and a trailing {@code eventId}/{@code isPlayed}/{@code timestamp}: camelCase field names, null fields
 * omitted, empty maps rendered as {@code {}}, enum values in PascalCase, timestamps as trimmed ISO-8601 with a
 * {@code Z} suffix, and strings escaped by {@link HtmlSafeJsonEscapes}.
 * <p>
 * {@link ExportFormatKind#JSONL} emits one object per line (gzip applied by the blob writer);
 * {@link ExportFormatKind#JSON} emits a single JSON array.
 * <p>
 * Entity messages use the reference SDK's legacy {@code EventSent}/{@code EventRaised} representation, including
 * the JSON-encoded request or response in the event's {@code input}.
 */
final class HistoryEventSerializer {

    private static final DateTimeFormatter DATE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private static final JsonFactory FACTORY = new JsonFactory();

    // Entity message JSON uses Newtonsoft's escaping, before the outer event applies HTML-safe escaping.
    private static final CharacterEscapes ENTITY_MESSAGE_ESCAPES = new CharacterEscapes() {
        private static final long serialVersionUID = 1L;

        @Override
        public int[] getEscapeCodesForAscii() {
            int[] escapes = CharacterEscapes.standardAsciiEscapesForJSON();
            for (int i = 0; i < 0x20; i++) {
                if (escapes[i] == CharacterEscapes.ESCAPE_STANDARD) {
                    escapes[i] = CharacterEscapes.ESCAPE_CUSTOM;
                }
            }
            return escapes;
        }

        @Override
        public SerializableString getEscapeSequence(int ch) {
            return ch < 0x20 || ch == 0x85 || ch == 0x2028 || ch == 0x2029
                    ? new SerializedString(String.format(Locale.ROOT, "\\u%04x", ch))
                    : null;
        }
    };

    private HistoryEventSerializer() {
    }

    /**
     * Serializes the history events into the format requested by {@code format}.
     * <p>
     * The entire result is materialized in memory as a single string (and later copied to a UTF-8 byte array and
     * gzipped by the blob writer). This is not a streaming serializer: peak memory scales with per-instance history
     * size multiplied by the number of instances exporting concurrently on a worker, so callers should bound history
     * size accordingly.
     *
     * @param historyEvents the ordered history events
     * @param format        the export format
     * @return the serialized content (JSONL text or JSON array text)
     * @throws IllegalArgumentException if an entity message is missing required history context or fields
     */
    static String serialize(List<HistoryEvent> historyEvents, ExportFormat format) {
        StringBuilder sb = new StringBuilder();
        boolean json = format.getKind() == ExportFormatKind.JSON;
        OrchestrationInstance currentInstance = null;
        if (json) {
            sb.append('[');
        }
        for (int i = 0; i < historyEvents.size(); i++) {
            HistoryEvent event = historyEvents.get(i);
            if (event instanceof ExecutionStartedEvent) {
                currentInstance = ((ExecutionStartedEvent) event).getOrchestrationInstance();
            }
            HistoryEvent exportEvent = isEntityEvent(event) ? toExportEvent(event, currentInstance) : event;
            String line = writeObject(coreMap(exportEvent));
            if (json) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(line);
            } else {
                sb.append(line).append('\n');
            }
        }
        if (json) {
            sb.append(']');
        }
        return sb.toString();
    }

    /**
     * Gets the blob file extension for a format.
     *
     * @param format the export format
     * @return {@code "jsonl.gz"} for JSONL (compressed) or {@code "json"} for JSON
     */
    static String fileExtension(ExportFormat format) {
        return format.getKind() == ExportFormatKind.JSON ? "json" : "jsonl.gz";
    }

    /**
     * Gets whether the format's content is gzip-compressed.
     *
     * @param format the export format
     * @return {@code true} for JSONL, {@code false} for JSON
     */
    static boolean isCompressed(ExportFormat format) {
        return format.getKind() != ExportFormatKind.JSON;
    }

    /**
     * Gets the blob content type for a format.
     *
     * @param format the export format
     * @return the content type
     */
    static String contentType(ExportFormat format) {
        return format.getKind() == ExportFormatKind.JSON ? "application/json" : "application/jsonl+gzip";
    }

    // ---- event -> ordered map ------------------------------------------------------------------

    private static Map<String, Object> coreMap(HistoryEvent event) {
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        m.put("eventType", eventType(event));

        if (event instanceof ExecutionStartedEvent) {
            ExecutionStartedEvent e = (ExecutionStartedEvent) event;
            putIfNotNull(m, "parentInstance", parentInstanceMap(e.getParentInstance()));
            putIfNotNull(m, "name", e.getName());
            putIfNotNull(m, "version", e.getVersion());
            putIfNotNull(m, "input", e.getInput());
            m.put("tags", tagsMap(e.getTags()));
            putIfNotNull(m, "scheduledStartTime", formatInstantOrNull(e.getScheduledStartTimestamp()));
            putIfNotNull(m, "orchestrationInstance", instanceMap(e.getOrchestrationInstance()));
        } else if (event instanceof ExecutionCompletedEvent) {
            ExecutionCompletedEvent e = (ExecutionCompletedEvent) event;
            putIfNotNull(m, "orchestrationStatus", statusString(e.getOrchestrationStatus()));
            putIfNotNull(m, "result", e.getResult());
            putIfNotNull(m, "failureDetails", failureMap(e.getFailureDetails()));
        } else if (event instanceof ContinueAsNewEvent) {
            // The export format models continue-as-new as an ExecutionCompleted with ContinuedAsNew status.
            ContinueAsNewEvent e = (ContinueAsNewEvent) event;
            m.put("orchestrationStatus", "ContinuedAsNew");
            putIfNotNull(m, "result", e.getInput());
        } else if (event instanceof ExecutionTerminatedEvent) {
            putIfNotNull(m, "input", ((ExecutionTerminatedEvent) event).getInput());
        } else if (event instanceof ExecutionSuspendedEvent) {
            putIfNotNull(m, "reason", ((ExecutionSuspendedEvent) event).getInput());
        } else if (event instanceof ExecutionResumedEvent) {
            putIfNotNull(m, "reason", ((ExecutionResumedEvent) event).getInput());
        } else if (event instanceof TaskScheduledEvent) {
            TaskScheduledEvent e = (TaskScheduledEvent) event;
            putIfNotNull(m, "name", e.getName());
            putIfNotNull(m, "version", e.getVersion());
            putIfNotNull(m, "input", e.getInput());
            m.put("tags", tagsMap(e.getTags()));
        } else if (event instanceof TaskCompletedEvent) {
            TaskCompletedEvent e = (TaskCompletedEvent) event;
            m.put("taskScheduledId", e.getTaskScheduledId());
            putIfNotNull(m, "result", e.getResult());
        } else if (event instanceof TaskFailedEvent) {
            TaskFailedEvent e = (TaskFailedEvent) event;
            m.put("taskScheduledId", e.getTaskScheduledId());
            putIfNotNull(m, "failureDetails", failureMap(e.getFailureDetails()));
        } else if (event instanceof SubOrchestrationInstanceCreatedEvent) {
            SubOrchestrationInstanceCreatedEvent e = (SubOrchestrationInstanceCreatedEvent) event;
            putIfNotNull(m, "name", e.getName());
            putIfNotNull(m, "version", e.getVersion());
            putIfNotNull(m, "instanceId", e.getInstanceId());
            putIfNotNull(m, "input", e.getInput());
        } else if (event instanceof SubOrchestrationInstanceCompletedEvent) {
            SubOrchestrationInstanceCompletedEvent e = (SubOrchestrationInstanceCompletedEvent) event;
            m.put("taskScheduledId", e.getTaskScheduledId());
            putIfNotNull(m, "result", e.getResult());
        } else if (event instanceof SubOrchestrationInstanceFailedEvent) {
            SubOrchestrationInstanceFailedEvent e = (SubOrchestrationInstanceFailedEvent) event;
            m.put("taskScheduledId", e.getTaskScheduledId());
            putIfNotNull(m, "failureDetails", failureMap(e.getFailureDetails()));
        } else if (event instanceof TimerCreatedEvent) {
            m.put("fireAt", formatInstant(((TimerCreatedEvent) event).getFireAt()));
        } else if (event instanceof TimerFiredEvent) {
            TimerFiredEvent e = (TimerFiredEvent) event;
            m.put("timerId", e.getTimerId());
            m.put("fireAt", formatInstant(e.getFireAt()));
        } else if (event instanceof EventSentEvent) {
            EventSentEvent e = (EventSentEvent) event;
            putIfNotNull(m, "instanceId", e.getInstanceId());
            putIfNotNull(m, "name", e.getName());
            putIfNotNull(m, "input", e.getInput());
        } else if (event instanceof EventRaisedEvent) {
            EventRaisedEvent e = (EventRaisedEvent) event;
            putIfNotNull(m, "name", e.getName());
            putIfNotNull(m, "input", e.getInput());
        } else if (event instanceof GenericEvent) {
            putIfNotNull(m, "data", ((GenericEvent) event).getData());
        } else if (event instanceof HistoryStateEvent) {
            putIfNotNull(m, "state", stateMap(((HistoryStateEvent) event).getState()));
        }
        // OrchestratorStartedEvent, OrchestratorCompletedEvent, ExecutionRewoundEvent carry no extra fields.

        // TimerFired carries eventId -1 in the export format.
        m.put("eventId", (event instanceof TimerFiredEvent) ? -1 : event.getEventId());
        m.put("isPlayed", Boolean.FALSE);
        m.put("timestamp", formatInstant(event.getTimestamp()));
        return m;
    }

    private static Map<String, Object> instanceMap(OrchestrationInstance oi) {
        if (oi == null) {
            return null;
        }
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        putIfNotNull(m, "instanceId", oi.getInstanceId());
        putIfNotNull(m, "executionId", oi.getExecutionId());
        return m;
    }

    private static Map<String, Object> parentInstanceMap(ParentInstanceInfo p) {
        if (p == null) {
            return null;
        }
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        putIfNotNull(m, "name", p.getName());
        putIfNotNull(m, "orchestrationInstance", instanceMap(p.getOrchestrationInstance()));
        // Deliberate wire-format field name (no trailing 'd'). Do not "correct" the spelling -- it would break
        // byte-for-byte compatibility with the export format.
        m.put("taskScheduleId", p.getTaskScheduledId());
        putIfNotNull(m, "version", p.getVersion());
        return m;
    }

    private static Map<String, Object> failureMap(FailureDetails f) {
        if (f == null) {
            return null;
        }
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        m.put("errorType", f.getErrorType());
        putIfNotNull(m, "errorMessage", f.getErrorMessage());
        putIfNotNull(m, "stackTrace", f.getStackTrace());
        putIfNotNull(m, "innerFailure", failureMap(f.getInnerFailure()));
        m.put("isNonRetriable", f.isNonRetriable());
        return m;
    }

    private static Map<String, Object> stateMap(OrchestrationState s) {
        if (s == null) {
            return null;
        }
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        putIfNotNull(m, "scheduledStartTime", formatInstantOrNull(s.getScheduledStartTime()));
        // The export format leaves these at their defaults (they are not populated during history retrieval).
        m.put("completedTime", "0001-01-01T00:00:00");
        m.put("compressedSize", 0);
        putIfNotNull(m, "createdTime", formatInstantOrNull(s.getCreatedTime()));
        putIfNotNull(m, "input", s.getInput());
        putIfNotNull(m, "lastUpdatedTime", formatInstantOrNull(s.getLastUpdatedTime()));
        putIfNotNull(m, "name", s.getName());
        LinkedHashMap<String, Object> oi = new LinkedHashMap<>();
        putIfNotNull(oi, "instanceId", s.getInstanceId());
        m.put("orchestrationInstance", oi);
        m.put("orchestrationStatus", "Running");
        putIfNotNull(m, "output", s.getOutput());
        m.put("size", 0);
        putIfNotNull(m, "status", s.getCustomStatus());
        m.put("tags", tagsMap(s.getTags()));
        putIfNotNull(m, "version", s.getVersion());
        return m;
    }

    private static Map<String, Object> tagsMap(Map<String, String> tags) {
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        if (tags != null) {
            m.putAll(tags);
        }
        return m;
    }

    private static String statusString(OrchestrationRuntimeStatus status) {
        if (status == null) {
            return null;
        }
        switch (status) {
            case RUNNING: return "Running";
            case COMPLETED: return "Completed";
            case CONTINUED_AS_NEW: return "ContinuedAsNew";
            case FAILED: return "Failed";
            case CANCELED: return "Canceled";
            case TERMINATED: return "Terminated";
            case PENDING: return "Pending";
            case SUSPENDED: return "Suspended";
            default: return status.name();
        }
    }

    private static String eventType(HistoryEvent event) {
        // GenericEvent keeps its suffix in the export format; every other event drops the trailing "Event".
        if (event instanceof GenericEvent) {
            return "GenericEvent";
        }
        String name = event.getClass().getSimpleName();
        return name.endsWith("Event") ? name.substring(0, name.length() - "Event".length()) : name;
    }

    private static boolean isEntityEvent(HistoryEvent event) {
        return event instanceof EntityOperationCalledEvent
                || event instanceof EntityOperationSignaledEvent
                || event instanceof EntityOperationCompletedEvent
                || event instanceof EntityOperationFailedEvent
                || event instanceof EntityLockRequestedEvent
                || event instanceof EntityLockGrantedEvent
                || event instanceof EntityUnlockSentEvent;
    }

    private static void putIfNotNull(Map<String, Object> m, String key, Object value) {
        if (value != null) {
            m.put(key, value);
        }
    }

    private static String formatInstantOrNull(Instant t) {
        return t == null ? null : formatInstant(t);
    }

    private static String formatInstant(Instant t) {
        OffsetDateTime utc = t.atOffset(ZoneOffset.UTC);
        StringBuilder sb = new StringBuilder(DATE_TIME.format(utc));
        int nanos = utc.getNano();
        if (nanos != 0) {
            // Up to seven fractional digits (100-ns ticks), trailing zeros trimmed.
            String frac = String.format(Locale.ROOT, "%09d", nanos).substring(0, 7);
            int end = frac.length();
            while (end > 0 && frac.charAt(end - 1) == '0') {
                end--;
            }
            if (end > 0) {
                sb.append('.').append(frac, 0, end);
            }
        }
        sb.append('Z');
        return sb.toString();
    }

    private static HistoryEvent toExportEvent(HistoryEvent event, OrchestrationInstance currentInstance) {
        Map<String, Object> message = new LinkedHashMap<>();
        if (event instanceof EntityOperationCalledEvent) {
            EntityOperationCalledEvent e = (EntityOperationCalledEvent) event;
            message = operationMessage(e.getOperation(), false, e.getInput(), e.getRequestId(),
                    e.getScheduledTime(), requireCurrentInstance(currentInstance));
            return entityEventSent(e, e.getTargetInstanceId(), operationEventName(e.getScheduledTime()), message);
        } else if (event instanceof EntityOperationSignaledEvent) {
            EntityOperationSignaledEvent e = (EntityOperationSignaledEvent) event;
            message = operationMessage(e.getOperation(), true, e.getInput(), e.getRequestId(),
                    e.getScheduledTime(), null);
            return entityEventSent(e, e.getTargetInstanceId(), operationEventName(e.getScheduledTime()), message);
        } else if (event instanceof EntityOperationCompletedEvent) {
            EntityOperationCompletedEvent e = (EntityOperationCompletedEvent) event;
            message.put("result", e.getOutput());
            return entityEventRaised(e, e.getRequestId(), message);
        } else if (event instanceof EntityOperationFailedEvent) {
            EntityOperationFailedEvent e = (EntityOperationFailedEvent) event;
            FailureDetails failure = e.getFailureDetails();
            if (failure == null) {
                throw new IllegalArgumentException("EntityOperationFailed history is missing failure details.");
            }
            message.put("result", failure.getErrorMessage());
            putIfNotNull(message, "exceptionType", failure.getErrorType());
            message.put("failureDetails", entityFailureMap(failure));
            return entityEventRaised(e, e.getRequestId(), message);
        } else if (event instanceof EntityLockRequestedEvent) {
            EntityLockRequestedEvent e = (EntityLockRequestedEvent) event;
            if (e.getPosition() < 0 || e.getPosition() >= e.getLockSet().size()) {
                throw new IllegalArgumentException("Entity lock position must identify a member of the lock set.");
            }
            message.put("op", null);
            message.put("id", e.getCriticalSectionId());
            message.put("parent", requireCurrentInstance(currentInstance).getInstanceId());
            List<Map<String, Object>> lockSet = new ArrayList<>();
            for (String entityId : e.getLockSet()) {
                lockSet.add(entityIdMap(entityId));
            }
            message.put("lockset", lockSet);
            if (e.getPosition() != 0) {
                message.put("pos", e.getPosition());
            }
            return entityEventSent(e, e.getLockSet().get(e.getPosition()), "op", message);
        } else if (event instanceof EntityLockGrantedEvent) {
            EntityLockGrantedEvent e = (EntityLockGrantedEvent) event;
            message.put("result", "Lock Acquisition Completed");
            return entityEventRaised(e, e.getCriticalSectionId(), message);
        } else if (event instanceof EntityUnlockSentEvent) {
            EntityUnlockSentEvent e = (EntityUnlockSentEvent) event;
            message.put("parent", requireCurrentInstance(currentInstance).getInstanceId());
            message.put("id", e.getCriticalSectionId());
            return entityEventSent(e, e.getTargetInstanceId(), "release", message);
        }
        throw new IllegalArgumentException("Unsupported entity history event: " + event.getClass().getSimpleName());
    }

    private static Map<String, Object> operationMessage(
            String operation, boolean signal, String input, String requestId,
            Instant scheduledTime, OrchestrationInstance parent) {
        Map<String, Object> message = new LinkedHashMap<>();
        message.put("op", operation);
        if (signal) {
            message.put("signal", true);
        }
        putIfNotNull(message, "input", input);
        message.put("id", requestId);
        if (parent != null) {
            message.put("parent", parent.getInstanceId());
            putIfNotNull(message, "parentExecution", parent.getExecutionId());
        }
        putIfNotNull(message, "due", formatInstantOrNull(scheduledTime));
        return message;
    }

    private static OrchestrationInstance requireCurrentInstance(OrchestrationInstance instance) {
        if (instance == null || instance.getInstanceId() == null) {
            throw new IllegalArgumentException(
                    "Entity history export requires an ExecutionStarted event with an orchestration instance.");
        }
        return instance;
    }

    private static Map<String, Object> entityIdMap(String entityId) {
        int separator = entityId == null ? -1 : entityId.indexOf('@', 1);
        if (entityId == null || !entityId.startsWith("@") || separator < 2) {
            throw new IllegalArgumentException("Invalid entity ID in exported lock set: " + entityId);
        }
        // DT Core preserves name casing and permits empty keys, unlike the SDK's EntityInstanceId.
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("name", entityId.substring(1, separator));
        result.put("key", entityId.substring(separator + 1));
        return result;
    }

    private static Map<String, Object> entityFailureMap(FailureDetails failure) {
        if (failure == null) {
            return null;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ErrorType", failure.getErrorType());
        result.put("ErrorMessage", failure.getErrorMessage());
        result.put("StackTrace", failure.getStackTrace());
        result.put("InnerFailure", entityFailureMap(failure.getInnerFailure()));
        result.put("IsNonRetriable", failure.isNonRetriable());
        result.put("Properties", failure.getProperties() == null ? Collections.emptyMap() : failure.getProperties());
        return result;
    }

    private static String operationEventName(Instant scheduledTime) {
        if (scheduledTime == null) {
            return "op";
        }
        OffsetDateTime utc = scheduledTime.atOffset(ZoneOffset.UTC);
        return "op@" + DATE_TIME.format(utc)
                + String.format(Locale.ROOT, ".%07dZ", utc.getNano() / 100);
    }

    private static HistoryEvent entityEventSent(
            HistoryEvent event, String target, String name, Map<String, Object> message) {
        if (target == null) {
            throw new IllegalArgumentException("Entity history export requires a target instance ID.");
        }
        return new EventSentEvent(event.getEventId(), event.getTimestamp(), target, name,
                writeObject(message, ENTITY_MESSAGE_ESCAPES, 0));
    }

    private static HistoryEvent entityEventRaised(HistoryEvent event, String name, Map<String, Object> message) {
        return new EventRaisedEvent(event.getEventId(), event.getTimestamp(), name,
                writeObject(message, ENTITY_MESSAGE_ESCAPES, 0));
    }

    // ---- ordered map -> JSON with the parity escaper -------------------------------------------

    private static String writeObject(Map<String, Object> map) {
        return writeObject(map, HtmlSafeJsonEscapes.INSTANCE, 0x7F);
    }

    private static String writeObject(
            Map<String, Object> map, CharacterEscapes escapes, int highestNonEscapedChar) {
        StringWriter sw = new StringWriter();
        try (JsonGenerator g = FACTORY.createGenerator(sw)) {
            g.setCharacterEscapes(escapes);
            g.setHighestNonEscapedChar(highestNonEscapedChar);
            writeMap(g, map);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return sw.toString();
    }

    private static void writeMap(JsonGenerator g, Map<?, ?> map) throws IOException {
        g.writeStartObject();
        for (Map.Entry<?, ?> e : map.entrySet()) {
            g.writeFieldName(String.valueOf(e.getKey()));
            writeValue(g, e.getValue());
        }
        g.writeEndObject();
    }

    private static void writeValue(JsonGenerator g, Object v) throws IOException {
        if (v == null) {
            g.writeNull();
        } else if (v instanceof String) {
            g.writeString((String) v);
        } else if (v instanceof Integer) {
            g.writeNumber((Integer) v);
        } else if (v instanceof Long) {
            g.writeNumber((Long) v);
        } else if (v instanceof Boolean) {
            g.writeBoolean((Boolean) v);
        } else if (v instanceof Double) {
            g.writeNumber((double) (Double) v);
        } else if (v instanceof Float) {
            g.writeNumber((float) (Float) v);
        } else if (v instanceof BigInteger) {
            g.writeNumber((BigInteger) v);
        } else if (v instanceof BigDecimal) {
            g.writeNumber((BigDecimal) v);
        } else if (v instanceof Number) {
            g.writeNumber(((Number) v).longValue());
        } else if (v instanceof Map) {
            writeMap(g, (Map<?, ?>) v);
        } else if (v instanceof Iterable) {
            g.writeStartArray();
            for (Object o : (Iterable<?>) v) {
                writeValue(g, o);
            }
            g.writeEndArray();
        } else {
            g.writeString(v.toString());
        }
    }
}
