// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions;

import com.microsoft.durabletask.Task;
import com.microsoft.durabletask.TaskOptions;
import com.microsoft.durabletask.TaskOrchestrationContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.microsoft.durabletask.RetryPolicy;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link DurableHttp}.
 */
@ExtendWith(MockitoExtension.class)
class DurableHttpTest {

    @Mock
    private TaskOrchestrationContext ctx;

    @Mock
    private Task<DurableHttpResponse> mockTask;

    // ---- Null validation tests ----

    @Test
    @DisplayName("callHttp(ctx, request): null ctx throws IllegalArgumentException")
    void callHttp_nullCtx_throws() {
        DurableHttpRequest request = new DurableHttpRequest("GET", URI.create("https://example.com"));
        assertThrows(IllegalArgumentException.class, () -> DurableHttp.callHttp(null, request));
    }

    @Test
    @DisplayName("callHttp(ctx, request): null request throws IllegalArgumentException")
    void callHttp_nullRequest_throws() {
        assertThrows(IllegalArgumentException.class, () -> DurableHttp.callHttp(ctx, (DurableHttpRequest) null));
    }

    @Test
    @DisplayName("callHttp(ctx, request, options): null ctx throws IllegalArgumentException")
    void callHttp_withOptions_nullCtx_throws() {
        DurableHttpRequest request = new DurableHttpRequest("GET", URI.create("https://example.com"));
        assertThrows(IllegalArgumentException.class, () -> DurableHttp.callHttp(null, request, null));
    }

    @Test
    @DisplayName("callHttp(ctx, request, options): null request throws IllegalArgumentException")
    void callHttp_withOptions_nullRequest_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> DurableHttp.callHttp(ctx, (DurableHttpRequest) null, null));
    }

    // ---- Delegation tests ----

    @Test
    @DisplayName("callHttp(ctx, request): delegates to callActivity with BuiltIn::HttpActivity")
    void callHttp_delegatesToCallActivity() {
        DurableHttpRequest request = new DurableHttpRequest("GET", URI.create("https://example.com"));
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), eq(request), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, request);

        assertSame(mockTask, result);
        verify(ctx).callActivity("BuiltIn::HttpActivity", request, DurableHttpResponse.class);
    }

    @Test
    @DisplayName("callHttp(ctx, request, options): delegates with TaskOptions when options is non-null")
    void callHttp_withOptions_delegatesWithTaskOptions() {
        DurableHttpRequest request = new DurableHttpRequest("POST", URI.create("https://example.com/api"));
        TaskOptions options = new TaskOptions(new RetryPolicy(3, Duration.ofSeconds(1)));
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), eq(request), eq(options), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, request, options);

        assertSame(mockTask, result);
        verify(ctx).callActivity("BuiltIn::HttpActivity", request, options, DurableHttpResponse.class);
    }

    @Test
    @DisplayName("callHttp(ctx, request, null options): delegates without TaskOptions")
    void callHttp_withNullOptions_delegatesWithoutTaskOptions() {
        DurableHttpRequest request = new DurableHttpRequest("GET", URI.create("https://example.com"));
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), eq(request), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, request, null);

        assertSame(mockTask, result);
        verify(ctx).callActivity("BuiltIn::HttpActivity", request, DurableHttpResponse.class);
        verify(ctx, never()).callActivity(anyString(), any(), any(TaskOptions.class), any());
    }

    // ---- Convenience method tests ----

    @Test
    @DisplayName("callHttp(ctx, method, uri): creates DurableHttpRequest and delegates")
    void callHttp_convenience_methodUri() {
        ArgumentCaptor<DurableHttpRequest> captor = ArgumentCaptor.forClass(DurableHttpRequest.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        URI uri = URI.create("https://httpbin.org/get");
        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, "GET", uri);

        assertSame(mockTask, result);
        DurableHttpRequest captured = captor.getValue();
        assertEquals("GET", captured.getMethod());
        assertEquals(uri, captured.getUri());
        assertNull(captured.getHeaders());
        assertNull(captured.getContent());
    }

    @Test
    @DisplayName("callHttp(ctx, method, uri, headers, content): creates DurableHttpRequest and delegates")
    void callHttp_convenience_headersContent() {
        ArgumentCaptor<DurableHttpRequest> captor = ArgumentCaptor.forClass(DurableHttpRequest.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        URI uri = URI.create("https://httpbin.org/post");
        Map<String, String> headers = Collections.singletonMap("Content-Type", "application/json");
        String content = "{\"key\":\"value\"}";

        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, "POST", uri, headers, content);

        assertSame(mockTask, result);
        DurableHttpRequest captured = captor.getValue();
        assertEquals("POST", captured.getMethod());
        assertEquals(uri, captured.getUri());
        assertEquals(headers, captured.getHeaders());
        assertEquals(content, captured.getContent());
    }

    @Test
    @DisplayName("callHttp(ctx, method, uri, null, null): null headers and content are allowed")
    void callHttp_convenience_nullHeadersContent() {
        ArgumentCaptor<DurableHttpRequest> captor = ArgumentCaptor.forClass(DurableHttpRequest.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        Task<DurableHttpResponse> result = DurableHttp.callHttp(
                ctx, "DELETE", URI.create("https://example.com/resource"), null, null);

        assertSame(mockTask, result);
        DurableHttpRequest captured = captor.getValue();
        assertEquals("DELETE", captured.getMethod());
        assertNull(captured.getHeaders());
        assertNull(captured.getContent());
    }

    // ---- Activity name tests ----

    @Test
    @DisplayName("BUILT_IN_HTTP_ACTIVITY_NAME is 'BuiltIn::HttpActivity'")
    void builtInActivityName() {
        assertEquals("BuiltIn::HttpActivity", DurableHttp.BUILT_IN_HTTP_ACTIVITY_NAME);
    }
}
