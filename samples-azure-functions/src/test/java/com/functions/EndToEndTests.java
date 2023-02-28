package com.functions;

import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.post;
import static org.junit.jupiter.api.Assertions.*;

@Tag("e2e")
public class EndToEndTests {
    private static final String hostHealthPingPath = "/admin/host/ping";
    private static final String startOrchestrationPath = "/api/StartOrchestration";

    @Order(1)
    @Test
    public void setupHost() {
        post(hostHealthPingPath).then().statusCode(200);
    }

    @Test
    public void basicChain() throws InterruptedException {
        Response response = post(startOrchestrationPath);
        JsonPath jsonPath = response.jsonPath();
        String statusQueryGetUri = jsonPath.get("statusQueryGetUri");
        String runTimeStatus = null;
        for (int i = 0; i < 15; i++) {
            Response statusResponse = get(statusQueryGetUri);
            runTimeStatus = statusResponse.jsonPath().get("runtimeStatus");
            if (!"Completed".equals(runTimeStatus)) {
                Thread.sleep(1000);
            } else break;
        }
        assertEquals("Completed", runTimeStatus);
    }
}
