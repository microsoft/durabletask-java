package com.functions;

import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.post;
import static org.junit.jupiter.api.Assertions.*;

@Tag("e2e")
public class EndToEndTests {

    @Order(1)
    @Test
    public void setupHost() {
        String hostHealthPingPath = "/admin/host/ping";
        post(hostHealthPingPath).then().statusCode(200);
    }

    @Test
    public void basicChain() throws InterruptedException {
        String startOrchestrationPath = "/api/StartOrchestration";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        String runTimeStatus = waitForCompletion(statusQueryGetUri);
        assertEquals("Completed", runTimeStatus);
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
            assertEquals("Running", runTimeStatus);
            Thread.sleep(1000);
        }
        String terminatePostUri = jsonPath.get("terminatePostUri");
        post(terminatePostUri, "Terminated the test");
        Thread.sleep(1000);
        Response statusResponse = get(statusQueryGetUri);
        runTimeStatus = statusResponse.jsonPath().get("runtimeStatus");
        assertEquals("Terminated", runTimeStatus);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void restart(boolean restartWithNewInstanceId) throws InterruptedException {
        String startOrchestrationPath = "/api/StartOrchestration";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        String runTimeStatus = waitForCompletion(statusQueryGetUri);
        assertEquals("Completed", runTimeStatus);
        Response statusResponse = get(statusQueryGetUri);
        String instanceId = statusResponse.jsonPath().get("instanceId");

        String restartPostUri = jsonPath.get("restartPostUri") + "&restartWithNewInstanceId=" + restartWithNewInstanceId;
        Response restartResponse = post(restartPostUri);
        JsonPath restartJsonPath = restartResponse.jsonPath();
        String restartStatusQueryGetUri = restartJsonPath.get("statusQueryGetUri");
        String restartRuntimeStatus = waitForCompletion(restartStatusQueryGetUri);
        assertEquals("Completed", restartRuntimeStatus);
        Response restartStatusResponse = get(restartStatusQueryGetUri);
        String newInstanceId = restartStatusResponse.jsonPath().get("instanceId");
        if (restartWithNewInstanceId) {
            assertNotEquals(instanceId, newInstanceId);
        } else {
            assertEquals(instanceId, newInstanceId);
        }
    }

    private String waitForCompletion(String statusQueryGetUri) throws InterruptedException {
        String runTimeStatus = null;
        for (int i = 0; i < 15; i++) {
            Response statusResponse = get(statusQueryGetUri);
            runTimeStatus = statusResponse.jsonPath().get("runtimeStatus");
            if (!"Completed".equals(runTimeStatus)) {
                Thread.sleep(1000);
            } else break;
        }
        return runTimeStatus;
    }

    @Test
    public void thenChain() throws InterruptedException {
        final String expect = "AUSTIN-test";
        String startOrchestrationPath = "/api/StartOrchestrationThenChain";
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        String runTimeStatus = null;
        String output = null;
        for (int i = 0; i < 15; i++) {
            Response statusResponse = get(statusQueryGetUri);
            runTimeStatus = statusResponse.jsonPath().get("runtimeStatus");
            output = statusResponse.jsonPath().get("output");
            if (!"Completed".equals(runTimeStatus)) {
                Thread.sleep(1000);
            } else break;
        }
        assertEquals("Completed", runTimeStatus);
        assertEquals(expect, output);
    }
}