// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azurefunctions;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DurableClientContextTest {
    private final Gson gson = new Gson();

    @ParameterizedTest
    // The host owns key filtering; these query spellings verify opaque SDK pass-through.
    @ValueSource(strings = {
            "taskHub=hub&connection=storage&code=synthetic%2Bkey",
            "CoDe=synthetic%2Bkey&site=example",
            "%63ode=synthetic%2Bkey&site=example&CODE=duplicate",
            "taskHub=hub&connection=storage",
            ""
    })
    void bindingQueryPassesThroughToPayloadAndLocation(String query) {
        JsonObject binding = new JsonObject();
        binding.addProperty("rpcBaseUrl", "http://127.0.0.1:12345");
        binding.addProperty("requiredQueryStringParameters", query);
        assertCheckStatusResponse(binding, query.isEmpty() ? "" : "?" + query);
    }

    @Test
    void bindingQueriesRemainScopedToEachContext() {
        DurableClientContext first = this.gson.fromJson(
                "{\"requiredQueryStringParameters\":\"code=first\"}", DurableClientContext.class);
        DurableClientContext second = this.gson.fromJson(
                "{\"requiredQueryStringParameters\":\"\"}", DurableClientContext.class);
        HttpRequestMessage<?> request = request();

        assertTrue(first.createHttpManagementPayload(request, "instance").getStatusQueryGetUri().endsWith("?code=first"));
        assertEquals("https://example.com/runtime/webhooks/durabletask/instances/instance",
                second.createHttpManagementPayload(request, "instance").getStatusQueryGetUri());
    }

    private void assertCheckStatusResponse(JsonObject binding, String query) {
        DurableClientContext context = this.gson.fromJson(binding, DurableClientContext.class);
        HttpRequestMessage<?> request = request();
        HttpResponseMessage.Builder builder = mock(HttpResponseMessage.Builder.class, RETURNS_SELF);
        HttpResponseMessage response = mock(HttpResponseMessage.class);
        when(request.createResponseBuilder(HttpStatus.ACCEPTED)).thenReturn(builder);
        when(builder.build()).thenReturn(response);

        assertSame(response, context.createCheckStatusResponse(request, "instance"));

        ArgumentCaptor<Object> body = ArgumentCaptor.forClass(Object.class);
        verify(builder).body(body.capture());
        HttpManagementPayload payload = (HttpManagementPayload) body.getValue();
        String expectedUrl = "https://example.com/runtime/webhooks/durabletask/instances/instance" + query;
        assertEquals(expectedUrl, payload.getStatusQueryGetUri());
        verify(builder).header("Location", expectedUrl);
        JsonObject json = this.gson.toJsonTree(payload).getAsJsonObject();
        assertTrue(json.entrySet().stream().allMatch(entry -> entry.getValue().isJsonPrimitive()
                && entry.getValue().getAsJsonPrimitive().isString()));
        assertFalse(this.gson.toJson(payload).contains("starter-key"));
    }

    private static HttpRequestMessage<?> request() {
        HttpRequestMessage<?> request = mock(HttpRequestMessage.class);
        when(request.getUri()).thenReturn(URI.create("https://example.com/api/start?code=starter-key"));
        return request;
    }
}
