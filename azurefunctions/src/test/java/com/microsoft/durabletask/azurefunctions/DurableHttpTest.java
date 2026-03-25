// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions;

import com.microsoft.durabletask.DurableHttp;
import com.microsoft.durabletask.DurableHttpRequest;
import com.microsoft.durabletask.DurableHttpResponse;
import com.microsoft.durabletask.ManagedIdentityOptions;
import com.microsoft.durabletask.ManagedIdentityTokenSource;
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
import java.util.List;
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

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("callHttp(ctx, request): delegates to callActivity with wrapped list input")
    void callHttp_delegatesToCallActivity() {
        DurableHttpRequest request = new DurableHttpRequest("GET", URI.create("https://example.com"));
        ArgumentCaptor<List<DurableHttpRequest>> captor = ArgumentCaptor.forClass(List.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, request);

        assertSame(mockTask, result);
        List<DurableHttpRequest> captured = captor.getValue();
        assertEquals(1, captured.size());
        assertSame(request, captured.get(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("callHttp(ctx, request, options): delegates with TaskOptions when options is non-null")
    void callHttp_withOptions_delegatesWithTaskOptions() {
        DurableHttpRequest request = new DurableHttpRequest("POST", URI.create("https://example.com/api"));
        TaskOptions options = new TaskOptions(new RetryPolicy(3, Duration.ofSeconds(1)));
        ArgumentCaptor<List<DurableHttpRequest>> captor = ArgumentCaptor.forClass(List.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(options), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, request, options);

        assertSame(mockTask, result);
        List<DurableHttpRequest> captured = captor.getValue();
        assertEquals(1, captured.size());
        assertSame(request, captured.get(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("callHttp(ctx, request, null options): delegates without TaskOptions")
    void callHttp_withNullOptions_delegatesWithoutTaskOptions() {
        DurableHttpRequest request = new DurableHttpRequest("GET", URI.create("https://example.com"));
        ArgumentCaptor<List<DurableHttpRequest>> captor = ArgumentCaptor.forClass(List.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, request, null);

        assertSame(mockTask, result);
        assertEquals(1, captor.getValue().size());
        assertSame(request, captor.getValue().get(0));
        verify(ctx, never()).callActivity(anyString(), any(), any(TaskOptions.class), any());
    }

    // ---- Convenience method tests ----

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("callHttp(ctx, method, uri): creates DurableHttpRequest and delegates")
    void callHttp_convenience_methodUri() {
        ArgumentCaptor<List<DurableHttpRequest>> captor = ArgumentCaptor.forClass(List.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        URI uri = URI.create("https://httpbin.org/get");
        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, "GET", uri);

        assertSame(mockTask, result);
        DurableHttpRequest captured = captor.getValue().get(0);
        assertEquals("GET", captured.getMethod());
        assertEquals(uri, captured.getUri());
        assertNull(captured.getHeaders());
        assertNull(captured.getContent());
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("callHttp(ctx, method, uri, headers, content): creates DurableHttpRequest and delegates")
    void callHttp_convenience_headersContent() {
        ArgumentCaptor<List<DurableHttpRequest>> captor = ArgumentCaptor.forClass(List.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        URI uri = URI.create("https://httpbin.org/post");
        Map<String, String> headers = Collections.singletonMap("Content-Type", "application/json");
        String content = "{\"key\":\"value\"}";

        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, "POST", uri, headers, content);

        assertSame(mockTask, result);
        DurableHttpRequest captured = captor.getValue().get(0);
        assertEquals("POST", captured.getMethod());
        assertEquals(uri, captured.getUri());
        assertEquals(headers, captured.getHeaders());
        assertEquals(content, captured.getContent());
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("callHttp(ctx, method, uri, null, null): null headers and content are allowed")
    void callHttp_convenience_nullHeadersContent() {
        ArgumentCaptor<List<DurableHttpRequest>> captor = ArgumentCaptor.forClass(List.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        Task<DurableHttpResponse> result = DurableHttp.callHttp(
                ctx, "DELETE", URI.create("https://example.com/resource"), null, null);

        assertSame(mockTask, result);
        DurableHttpRequest captured = captor.getValue().get(0);
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

    // ---- TokenSource / Managed Identity delegation tests ----

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("callHttp: request with ManagedIdentityTokenSource passes token source to activity")
    void callHttp_withManagedIdentityTokenSource_passesThrough() {
        ManagedIdentityTokenSource tokenSource = new ManagedIdentityTokenSource(
                "https://management.core.windows.net/.default");
        DurableHttpRequest request = new DurableHttpRequest("GET",
                URI.create("https://example.com"), null, null, tokenSource);

        ArgumentCaptor<List<DurableHttpRequest>> captor = ArgumentCaptor.forClass(List.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, request);

        assertSame(mockTask, result);
        DurableHttpRequest captured = captor.getValue().get(0);
        assertNotNull(captured.getTokenSource());
        assertInstanceOf(ManagedIdentityTokenSource.class, captured.getTokenSource());
        ManagedIdentityTokenSource capturedToken = (ManagedIdentityTokenSource) captured.getTokenSource();
        assertEquals("https://management.core.windows.net/.default", capturedToken.getResource());
        assertEquals("AzureManagedIdentity", capturedToken.getKind());
        assertNull(capturedToken.getOptions());
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("callHttp: request with ManagedIdentityTokenSource and options passes both through")
    void callHttp_withManagedIdentityAndOptions_passesThrough() {
        ManagedIdentityOptions options = new ManagedIdentityOptions(
                URI.create("https://login.microsoftonline.com/"), "tenant_id");
        ManagedIdentityTokenSource tokenSource = new ManagedIdentityTokenSource(
                "https://graph.microsoft.com/.default", options);
        DurableHttpRequest request = new DurableHttpRequest("GET",
                URI.create("https://example.com"), null, null, tokenSource);

        ArgumentCaptor<List<DurableHttpRequest>> captor = ArgumentCaptor.forClass(List.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, request);

        assertSame(mockTask, result);
        DurableHttpRequest captured = captor.getValue().get(0);
        assertNotNull(captured.getTokenSource());
        ManagedIdentityTokenSource capturedToken = (ManagedIdentityTokenSource) captured.getTokenSource();
        assertEquals("https://graph.microsoft.com/.default", capturedToken.getResource());
        assertNotNull(capturedToken.getOptions());
        assertEquals(URI.create("https://login.microsoftonline.com/"),
                capturedToken.getOptions().getAuthorityHost());
        assertEquals("tenant_id", capturedToken.getOptions().getTenantId());
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("callHttp: request with ManagedIdentityTokenSource + TaskOptions passes both through")
    void callHttp_withManagedIdentityAndTaskOptions_passesThrough() {
        ManagedIdentityTokenSource tokenSource = new ManagedIdentityTokenSource(
                "https://vault.azure.net/.default");
        DurableHttpRequest request = new DurableHttpRequest("GET",
                URI.create("https://myvault.vault.azure.net/secrets/mysecret"),
                null, null, tokenSource);
        TaskOptions taskOptions = new TaskOptions(new RetryPolicy(3, Duration.ofSeconds(1)));

        ArgumentCaptor<List<DurableHttpRequest>> captor = ArgumentCaptor.forClass(List.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(),
                eq(taskOptions), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        Task<DurableHttpResponse> result = DurableHttp.callHttp(ctx, request, taskOptions);

        assertSame(mockTask, result);
        DurableHttpRequest captured = captor.getValue().get(0);
        assertNotNull(captured.getTokenSource());
        assertInstanceOf(ManagedIdentityTokenSource.class, captured.getTokenSource());
        assertEquals("https://vault.azure.net/.default",
                ((ManagedIdentityTokenSource) captured.getTokenSource()).getResource());
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("callHttp: request without tokenSource passes null tokenSource")
    void callHttp_withoutTokenSource_passesNullTokenSource() {
        DurableHttpRequest request = new DurableHttpRequest("GET", URI.create("https://example.com"));

        ArgumentCaptor<List<DurableHttpRequest>> captor = ArgumentCaptor.forClass(List.class);
        when(ctx.callActivity(eq("BuiltIn::HttpActivity"), captor.capture(), eq(DurableHttpResponse.class)))
                .thenReturn(mockTask);

        DurableHttp.callHttp(ctx, request);

        assertNull(captor.getValue().get(0).getTokenSource());
    }
}
