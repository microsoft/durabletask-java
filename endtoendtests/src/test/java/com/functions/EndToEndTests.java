package com.functions;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.post;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("e2e")
public class EndToEndTests {

    @BeforeAll
    public static void setup() {
        RestAssured.baseURI = "http://localhost";
        // Use port 8080 for Docker, 7071 for local func start
        String port = System.getenv("FUNCTIONS_PORT");
        if (port != null) {
            try {
                RestAssured.port = Integer.parseInt(port);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "FUNCTIONS_PORT environment variable must be a valid integer, but was: '" + port + "'", e);
            }
        } else {
            RestAssured.port = 8080;
        }
    }

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

    @Test
    public void continueAsNewExternalEvent() throws InterruptedException {
        String startOrchestrationPath = "api/ContinueAsNewExternalEvent";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();

        // send external event, it will cause the continue-as-new.
        String sendEventPostUri = jsonPath.get("sendEventPostUri");
        sendEventPostUri = sendEventPostUri.replace("{eventName}", "event");

        // empty request body
        RestAssured
                .given()
                .contentType(ContentType.JSON) // Set the request content type
                .body("{}")
                .post(sendEventPostUri)
                .then()
                .statusCode(202);

        //wait 5 seconds for the continue-as-new to start new orchestration
        TimeUnit.SECONDS.sleep(5);

        response = post(startOrchestrationPath);
        jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        // polling 20 seconds
        // assert that the orchestration completed as expected, not enter an infinite loop
        boolean completed = pollingCheck(statusQueryGetUri, "Completed", null, Duration.ofSeconds(20));
        assertTrue(completed);
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
        final String expect = "\"AUSTIN\"-test";
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

    @Test
    public void rewindFailedOrchestration() throws InterruptedException {
        // Start the orchestration - the trigger waits for failure and calls
        // client.rewindInstance() internally before returning
        String startOrchestrationPath = "/api/StartRewindableOrchestration";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");

        // The trigger already called client.rewindInstance(), so just poll for completion
        boolean completed = pollingCheck(statusQueryGetUri, "Completed", null, Duration.ofSeconds(30));
        assertTrue(completed, "Orchestration should complete after rewind");

        // Verify the output contains the expected result
        Response statusResponse = get(statusQueryGetUri);
        String output = statusResponse.jsonPath().get("output");
        assertTrue(output.contains("rewound-success"), "Output should indicate successful rewind: " + output);
    }

    @Test
    public void rewindNonFailedOrchestration() throws InterruptedException {
        // Start a non-failing orchestration - the trigger waits for completion
        // and then calls client.rewindInstance() internally before returning
        String startOrchestrationPath = "/api/StartRewindNonFailedOrchestration";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");

        // Wait a few seconds to allow any potential state change from the rewind attempt
        Thread.sleep(5000);

        // Verify the orchestration remains in Completed state (rewind should have no effect)
        Response statusResponse = get(statusQueryGetUri);
        String status = statusResponse.jsonPath().get("runtimeStatus");
        assertEquals("Completed", status,
                "Orchestration should remain Completed after rewind attempt on a non-failed instance");
    }

    @Test
    public void rewindSubOrchestrationFailure() throws InterruptedException {
        // Start the parent orchestration - the trigger waits for the sub-orchestration
        // to fail, then calls client.rewindInstance() internally before returning
        String startOrchestrationPath = "/api/StartRewindableSubOrchestration";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");

        // The trigger already called client.rewindInstance(), so just poll for completion
        boolean completed = pollingCheck(statusQueryGetUri, "Completed", null, Duration.ofSeconds(30));
        assertTrue(completed, "Parent orchestration should complete after rewind");

        // Verify the output contains the expected result from the sub-orchestration
        Response statusResponse = get(statusQueryGetUri);
        String output = statusResponse.jsonPath().get("output");
        assertTrue(output.contains("sub-rewound-success"),
                "Output should indicate successful sub-orchestration rewind: " + output);
    }

    @Test
    public void externalEventDeserializeFail() throws InterruptedException {
        String startOrchestrationPath = "api/ExternalEventHttp";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String sendEventPostUri = jsonPath.get("sendEventPostUri");
        sendEventPostUri = sendEventPostUri.replace("{eventName}", "event");

        String requestBody = "{\"value\":\"Test\"}";
        RestAssured
            .given()
            .contentType(ContentType.JSON) // Set the request content type
            .body(requestBody) // Set the request body
            .post(sendEventPostUri)
            .then()
            .statusCode(202);
        // assert orchestration status
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        boolean completed = pollingCheck(statusQueryGetUri, "Failed", null, Duration.ofSeconds(10));
        assertTrue(completed);

        // assert exception message
        Response resp = get(statusQueryGetUri);
        String errorMessage = resp.jsonPath().get("output");
        assertTrue(errorMessage.contains("Failed to deserialize the JSON text to java.lang.String"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "DeserializeErrorHttp",
        "SubCompletedErrorHttp"
    })
    public void DeserializeFail(String functionName) throws InterruptedException {
        String startOrchestrationPath = "api/" + functionName;
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        // assert orchestration status
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        boolean completed = pollingCheck(statusQueryGetUri, "Failed", null, Duration.ofSeconds(10));
        assertTrue(completed);

        // assert exception message
        Response resp = get(statusQueryGetUri);
        String errorMessage = resp.jsonPath().get("output");
        assertTrue(errorMessage.contains("Failed to deserialize the JSON text"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "default",
        "",
        "0.9",
        "1.0"
    })
    public void VersionedOrchestrationTests(String version) throws InterruptedException {
        Response response = post("api/StartVersionedOrchestration?version=" + version);
        String statusQueryGetUri = null;
        try {

            JsonPath jsonPath = response.jsonPath();
            // assert orchestration status
            statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        } catch (Exception e) {
            fail("Failed to parse response: " + response.asString());
        }
        boolean completed = pollingCheck(statusQueryGetUri, "Completed", null, Duration.ofSeconds(10));
        assertTrue(completed);

        // assert exception message
        Response resp = get(statusQueryGetUri);
        String output = resp.jsonPath().get("output");
        if (version.equals("default")) {
            assertTrue(output.contains("Version: '1.0'"), "Expected default version (1.0), got: " + output);
        } else {
            assertTrue(output.contains(String.format("Version: '%s'", version)), "Expected version (" + version + "), got: " + output);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "default",
        "",
        "0.9",
        "1.0"
    })
    public void VersionedSubOrchestrationTests(String version) throws InterruptedException {
        Response response = post("api/StartVersionedSubOrchestration?version=" + version);
        String statusQueryGetUri = null;
        try {

            JsonPath jsonPath = response.jsonPath();
            // assert orchestration status
            statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        } catch (Exception e) {
            fail("Failed to parse response: " + response.asString());
        }
        boolean completed = pollingCheck(statusQueryGetUri, "Completed", null, Duration.ofSeconds(10));
        assertTrue(completed);

        // assert exception message
        Response resp = get(statusQueryGetUri);
        String output = resp.jsonPath().get("output");
        if (version.equals("default")) {
            assertTrue(output.contains(String.format("Version: '%s'", "1.0")), "Expected default version (1.0), got: " + output);
        } else {
            assertTrue(output.contains(String.format("Version: '%s'", version)), "Expected version (" + version + "), got: " + output);
        }
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
}