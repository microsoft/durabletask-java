package com.functions;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.post;
import static org.junit.jupiter.api.Assertions.*;

@Tag("e2e")
public class EndToEndTests {
    private static final String hostHealthPingPath = "/admin/host/ping";
    private static final String startOrchestrationPath = "/api/StartOrchestration";
    private static final String approvalWorkFlow = "/api/ApprovalWorkflowOrchestration";
    private static final String rewindInstance = "/api/RewindInstance";

    @Order(1)
    @Test
    public void setupHost() {
        String hostHealthPingPath = "/admin/host/ping";
        post(hostHealthPingPath).then().statusCode(200);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "StartOrchestration",
            "StartParallelOrchestration",
            "StartParallelAnyOf",
            "StartParallelCatchException"
    })
    public void generalFunctions(String functionName) throws InterruptedException {
        Set<String> continueStates = new HashSet<>();
        continueStates.add("Pending");
        continueStates.add("Running");
        String startOrchestrationPath = "/api/" + functionName;
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        boolean pass = pollingCheck(statusQueryGetUri, "Completed", continueStates, Duration.ofSeconds(20));
        assertTrue(pass);
    }

    @Test
    public void retryTest() throws InterruptedException {
        String startOrchestrationPath = "/api/RetriableOrchestration";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        boolean pass = pollingCheck(statusQueryGetUri, "Completed", null, Duration.ofSeconds(10));
        assertTrue(pass);
    }

    @Test
    public void retryTestFail() throws InterruptedException {
        String startOrchestrationPath = "/api/RetriableOrchestrationFail";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        boolean pass = pollingCheck(statusQueryGetUri, "Failed", null, Duration.ofSeconds(10));
        assertTrue(pass);
    }

    @Test
    public void retryTestSuccess() throws InterruptedException {
        String startOrchestrationPath = "/api/RetriableOrchestrationSuccess";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        boolean pass = pollingCheck(statusQueryGetUri, "Completed", null, Duration.ofSeconds(10));
        assertTrue(pass);
    }

    @Test
    public void continueAsNew() throws InterruptedException {
        String startOrchestrationPath = "api/ContinueAsNew";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        String runTimeStatus;
        //assert that the orchestration is always running.
        for (int i = 0; i < 10; i++) {
            Response statusResponse = get(statusQueryGetUri);
            runTimeStatus = statusResponse.jsonPath().get("runtimeStatus");
            assertTrue(runTimeStatus.equals("Running") || runTimeStatus.equals("Pending"));
            Thread.sleep(1000);
        }
        String terminatePostUri = jsonPath.get("terminatePostUri");
        post(terminatePostUri, "Terminated the test");
        Thread.sleep(5000);
        Response statusResponse = get(statusQueryGetUri);
        statusResponse.jsonPath().get("runtimeStatus");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void restart(boolean restartWithNewInstanceId) throws InterruptedException {
        String startOrchestrationPath = "/api/StartOrchestration";
        Set<String> continueStates = new HashSet<>();
        continueStates.add("Pending");
        continueStates.add("Running");
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        boolean completed = pollingCheck(statusQueryGetUri, "Completed", continueStates, Duration.ofSeconds(10));
        assertTrue(completed);
        Response statusResponse = get(statusQueryGetUri);
        String instanceId = statusResponse.jsonPath().get("instanceId");

        String restartPostUri = jsonPath.get("restartPostUri") + "&restartWithNewInstanceId=" + restartWithNewInstanceId;
        Response restartResponse = post(restartPostUri);
        JsonPath restartJsonPath = restartResponse.jsonPath();
        String restartStatusQueryGetUri = restartJsonPath.get("statusQueryGetUri");
        completed = pollingCheck(restartStatusQueryGetUri, "Completed", continueStates, Duration.ofSeconds(10));
        assertTrue(completed);
        Response restartStatusResponse = get(restartStatusQueryGetUri);
        String newInstanceId = restartStatusResponse.jsonPath().get("instanceId");
        if (restartWithNewInstanceId) {
            assertNotEquals(instanceId, newInstanceId);
        } else {
            assertEquals(instanceId, newInstanceId);
        }
    }

    @Test
    public void thenChain() throws InterruptedException {
        Set<String> continueStates = new HashSet<>();
        continueStates.add("Pending");
        continueStates.add("Running");
        final String expect = "AUSTIN-test";
        String startOrchestrationPath = "/api/StartOrchestrationThenChain";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        boolean completed = pollingCheck(statusQueryGetUri, "Completed", continueStates, Duration.ofSeconds(20));
        assertTrue(completed);
        String output = get(statusQueryGetUri).jsonPath().get("output");
        assertEquals(expect, output);
    }

    @Test
    public void suspendResume() throws InterruptedException {
        String startOrchestrationPath = "api/StartResumeSuspendOrchestration";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        boolean running = pollingCheck(statusQueryGetUri, "Running", null, Duration.ofSeconds(5));
        assertTrue(running);

        String suspendPostUri = jsonPath.get("suspendPostUri");
        post(suspendPostUri, "SuspendOrchestration");
        boolean suspend = pollingCheck(statusQueryGetUri, "Suspended", null, Duration.ofSeconds(5));
        assertTrue(suspend);

        String sendEventPostUri = jsonPath.get("sendEventPostUri");
        sendEventPostUri = sendEventPostUri.replace("{eventName}", "test");

        String requestBody = "{\"value\":\"Test\"}";
        RestAssured
                .given()
                .contentType(ContentType.JSON) // Set the request content type
                .body(requestBody) // Set the request body
                .post(sendEventPostUri)
                .then()
                .statusCode(202);

        boolean suspendAfterEventSent = pollingCheck(statusQueryGetUri, "Suspended", null, Duration.ofSeconds(5));
        assertTrue(suspendAfterEventSent);

        String resumePostUri = jsonPath.get("resumePostUri");
        post(resumePostUri, "ResumeOrchestration");

        boolean completed = pollingCheck(statusQueryGetUri, "Completed", null, Duration.ofSeconds(5));
        assertTrue(completed);
    }

    private boolean pollingCheck(String statusQueryGetUri,
                                 String expectedState,
                                 Set<String> continueStates,
                                 Duration timeout) throws InterruptedException {
        String runTimeStatus = null;
        Instant begin = Instant.now();
        Instant runningTime = Instant.now();
        while (Duration.between(begin, runningTime).compareTo(timeout) <= 0) {
            Response statusResponse = get(statusQueryGetUri);
            runTimeStatus = statusResponse.jsonPath().get("runtimeStatus");
            if (expectedState.equals(runTimeStatus)) {
                return true;
            }
            if (continueStates != null && !continueStates.contains(runTimeStatus)) {
                return false;
            }
            Thread.sleep(1000);
            runningTime = Instant.now();
        }
        return false;
    }

    @Order(2)
    @Test
    public void testRewindInstanceAPI() throws InterruptedException {
        Response response = post(approvalWorkFlow);
        JsonPath rewindTestJsonPath = response.jsonPath();

        // Wait for the ApprovalWorkflowOrchestration to fail
        Thread.sleep(3000);

        String instanceId = rewindTestJsonPath.get("id");
        String statusQueryGetUri = rewindTestJsonPath.get("statusQueryGetUri");
        Response statusResponse = get(statusQueryGetUri);
        String runTimeStatus = statusResponse.jsonPath().get("runtimeStatus");
        assertEquals("Failed", runTimeStatus);

        // Rewind the instance
        String rewindPostUri = rewindInstance + "?instanceId=" + instanceId;
        response = post(rewindPostUri);
        assertEquals("Failed orchestration instance is scheduled for rewind.", response.toString());

        // Wait for orchestration to rewind and complete
        Thread.sleep(3000);

        for (int i = 0; i < 5; i++) {
            statusResponse = get(statusQueryGetUri);
            runTimeStatus = statusResponse.jsonPath().get("runtimeStatus");
            if (!"Completed".equals(runTimeStatus)) {
                Thread.sleep(1000);
            } else break;
        }
        assertEquals("Completed", runTimeStatus);
    }
}

