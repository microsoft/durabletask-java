package com.functions;

import com.microsoft.durabletask.azurefunctions.HttpManagementPayload;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link HttpManagementPayload}.
 */
public class HttpManagementPayloadTest {

    private static final String INSTANCE_ID = "test-instance-id";
    private static final String INSTANCE_STATUS_URL = "http://localhost:7071/runtime/webhooks/durabletask/instances/test-instance-id";
    private static final String QUERY_STRING = "code=abc123";

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
}
