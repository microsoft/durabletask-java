package com.microsoft.durabletask.azurefunctions;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link HttpManagementPayload}.
 */
public class HttpManagementPayloadTest {

    private static final String INSTANCE_ID = "test-instance-id";
    private static final String INSTANCE_STATUS_URL = "http://localhost:7071/runtime/webhooks/durabletask/instances/test-instance-id";
    private static final String QUERY_STRING = "taskHub=hub&connection=storage";

    private HttpManagementPayload createPayload() {
        return new HttpManagementPayload(INSTANCE_ID, INSTANCE_STATUS_URL, QUERY_STRING);
    }

    @Test
    @DisplayName("getId should return the instance ID")
    public void getId_ReturnsInstanceId() {
        HttpManagementPayload payload = createPayload();
        assertEquals(INSTANCE_ID, payload.getId());
    }

    @Test
    @DisplayName("getStatusQueryGetUri should return correct URL")
    public void getStatusQueryGetUri_ReturnsCorrectUrl() {
        HttpManagementPayload payload = createPayload();
        assertEquals(INSTANCE_STATUS_URL + "?" + QUERY_STRING, payload.getStatusQueryGetUri());
    }

    @Test
    @DisplayName("getSendEventPostUri should return correct URL")
    public void getSendEventPostUri_ReturnsCorrectUrl() {
        HttpManagementPayload payload = createPayload();
        assertEquals(INSTANCE_STATUS_URL + "/raiseEvent/{eventName}?" + QUERY_STRING, payload.getSendEventPostUri());
    }

    @Test
    @DisplayName("getTerminatePostUri should return correct URL")
    public void getTerminatePostUri_ReturnsCorrectUrl() {
        HttpManagementPayload payload = createPayload();
        assertEquals(INSTANCE_STATUS_URL + "/terminate?reason={text}&" + QUERY_STRING, payload.getTerminatePostUri());
    }

    @Test
    @DisplayName("getPurgeHistoryDeleteUri should return correct URL")
    public void getPurgeHistoryDeleteUri_ReturnsCorrectUrl() {
        HttpManagementPayload payload = createPayload();
        assertEquals(INSTANCE_STATUS_URL + "?" + QUERY_STRING, payload.getPurgeHistoryDeleteUri());
    }

    @Test
    @DisplayName("getRestartPostUri should return correct URL")
    public void getRestartPostUri_ReturnsCorrectUrl() {
        HttpManagementPayload payload = createPayload();
        assertEquals(INSTANCE_STATUS_URL + "/restart?" + QUERY_STRING, payload.getRestartPostUri());
    }

    @Test
    @DisplayName("getSuspendPostUri should return correct URL")
    public void getSuspendPostUri_ReturnsCorrectUrl() {
        HttpManagementPayload payload = createPayload();
        assertEquals(INSTANCE_STATUS_URL + "/suspend?reason={text}&" + QUERY_STRING, payload.getSuspendPostUri());
    }

    @Test
    @DisplayName("getResumePostUri should return correct URL")
    public void getResumePostUri_ReturnsCorrectUrl() {
        HttpManagementPayload payload = createPayload();
        assertEquals(INSTANCE_STATUS_URL + "/resume?reason={text}&" + QUERY_STRING, payload.getResumePostUri());
    }

    @Test
    @DisplayName("getRewindPostUri should return correct URL")
    public void getRewindPostUri_ReturnsCorrectUrl() {
        HttpManagementPayload payload = createPayload();
        assertEquals(INSTANCE_STATUS_URL + "/rewind?reason={text}&" + QUERY_STRING, payload.getRewindPostUri());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "code=synthetic%2Bkey%2F%3D&site=value%2Bone",
            "site=value%2Bone&CoDe=synthetic%2Bkey%2F%3D",
            "%63ode=synthetic%2Bkey%2F%3D&site=value%2Bone&CODE=duplicate"
    })
    void legacyQueryKeyIsMovedToRequiredHeaders(String query) {
        HttpManagementPayload payload = new HttpManagementPayload(INSTANCE_ID, INSTANCE_STATUS_URL, query);

        assertEquals("synthetic+key/=", payload.getRequiredHeaders().get("x-functions-key"));
        assertEquals(INSTANCE_STATUS_URL + "?site=value%2Bone", payload.getStatusQueryGetUri());
        for (String url : Arrays.asList(
                payload.getStatusQueryGetUri(), payload.getSendEventPostUri(), payload.getTerminatePostUri(),
                payload.getRewindPostUri(), payload.getPurgeHistoryDeleteUri(), payload.getRestartPostUri(),
                payload.getSuspendPostUri(), payload.getResumePostUri())) {
            assertTrue(url.contains("site=value%2Bone"));
            assertFalse(url.toLowerCase().contains("code="));
            assertFalse(url.contains("synthetic"));
            assertFalse(url.contains("duplicate"));
        }
    }

    @Test
    void explicitHeadersOverrideLegacyKeyAndAreDefensivelyCopied() {
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Functions-Key", "current-key");
        HttpManagementPayload payload = new HttpManagementPayload(
                INSTANCE_ID, INSTANCE_STATUS_URL, "code=old-key&" + QUERY_STRING, headers);
        headers.put("X-Functions-Key", "changed");

        assertEquals(Collections.singletonMap("x-functions-key", "current-key"), payload.getRequiredHeaders());
        assertThrows(UnsupportedOperationException.class, () -> payload.getRequiredHeaders().put("extra", "value"));
        assertEquals(INSTANCE_STATUS_URL + "?" + QUERY_STRING, payload.getStatusQueryGetUri());
    }

    @Test
    void missingOrEmptyQueryDoesNotAddNullOrTrailingSeparators() {
        for (String query : new String[] { null, "", "code=synthetic-key" }) {
            HttpManagementPayload payload = new HttpManagementPayload(INSTANCE_ID, INSTANCE_STATUS_URL, query);
            assertEquals(INSTANCE_STATUS_URL, payload.getStatusQueryGetUri());
            assertEquals(INSTANCE_STATUS_URL + "/terminate?reason={text}", payload.getTerminatePostUri());
        }
        assertTrue(createPayload().getRequiredHeaders().isEmpty());
    }
}
