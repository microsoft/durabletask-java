// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions.internal.middleware;

import com.microsoft.azure.functions.internal.spi.middleware.MiddlewareChain;
import com.microsoft.azure.functions.internal.spi.middleware.MiddlewareContext;
import com.microsoft.durabletask.ExceptionPropertiesProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ActivityMiddleware}, covering exception reshaping, pass-through behavior,
 * and the cross-class-loader SPI discovery that guards against the worker-thread regression.
 */
public class ActivityMiddlewareTest {

    private static final String ACTIVITY_TRIGGER = "DurableActivityTrigger";

    /** Auto-cleaned temp directory root for SPI class loader fixtures. */
    @TempDir
    Path tempDir;

    /** A MiddlewareChain whose {@code doNext} throws the supplied exception. */
    private static MiddlewareChain throwingChain(Exception toThrow) {
        return context -> {
            throw toThrow;
        };
    }

    /** A test exception representing a user's activity failure. */
    private static final class BusinessException extends Exception {
        private static final long serialVersionUID = 1L;

        BusinessException(String message) {
            super(message);
        }
    }

    private MiddlewareContext activityContext() {
        MiddlewareContext context = mock(MiddlewareContext.class);
        when(context.getParameterName(anyString())).thenReturn("input");
        return context;
    }

    // --- Reflection bridges to ActivityMiddleware's private test seams ---
    // The seams are private (they are not part of the middleware's API), so tests reach them via
    // reflection rather than widening visibility.

    private static void setProviderSupplier(Supplier<ExceptionPropertiesProvider> supplier) {
        invokeStatic("setProviderSupplierForTesting", new Class<?>[] {Supplier.class}, supplier);
    }

    private static void resetProviderCache() {
        invokeStatic("resetProviderCacheForTesting", new Class<?>[] {});
    }

    private static ExceptionPropertiesProvider discoverProvider(ClassLoader[] candidates) {
        return (ExceptionPropertiesProvider) invokeStatic(
                "discoverProvider", new Class<?>[] {ClassLoader[].class}, (Object) candidates);
    }

    private static Object invokeStatic(String name, Class<?>[] paramTypes, Object... args) {
        try {
            Method method = ActivityMiddleware.class.getDeclaredMethod(name, paramTypes);
            method.setAccessible(true);
            return method.invoke(null, args);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to invoke ActivityMiddleware." + name, e);
        }
    }

    @BeforeEach
    void resetBefore() {
        resetProviderCache();
    }

    @AfterEach
    void resetAfter() {
        resetProviderCache();
    }

    @Test
    @DisplayName("Reshapes a failing activity into structured TaskFailureDetails JSON when the "
            + "provider yields properties")
    void reshapesFailureWhenProviderYieldsProperties() {
        setProviderSupplier(() -> exception -> {
            Map<String, Object> properties = new LinkedHashMap<>();
            properties.put("code", "E123");
            properties.put("count", 7);
            return properties;
        });

        BusinessException original = new BusinessException("boom");
        ActivityMiddleware middleware = new ActivityMiddleware();

        Exception thrown = assertThrows(Exception.class,
                () -> middleware.invoke(activityContext(), throwingChain(original)));

        // The original exception is replaced by a structured-failure carrier whose message is JSON.
        assertNotSameInstance(original, thrown);
        String message = thrown.getMessage();
        assertNotNull(message);
        assertTrue(message.startsWith("{"), "message should be a JSON object, was: " + message);
        assertTrue(message.contains("\"errorType\":\"" + BusinessException.class.getName() + "\""),
                message);
        assertTrue(message.contains("\"errorMessage\":\"boom\""), message);
        assertTrue(message.contains("\"code\":\"E123\""), message);
        assertTrue(message.contains("\"count\":7"), message);
    }

    @Test
    @DisplayName("Rethrows the original exception unchanged when the provider returns no properties")
    void rethrowsOriginalWhenProviderReturnsEmpty() {
        setProviderSupplier(
                () -> exception -> Collections.emptyMap());

        BusinessException original = new BusinessException("boom");
        ActivityMiddleware middleware = new ActivityMiddleware();

        Exception thrown = assertThrows(Exception.class,
                () -> middleware.invoke(activityContext(), throwingChain(original)));

        assertSame(original, thrown);
    }

    @Test
    @DisplayName("Rethrows the original exception unchanged when the provider returns null")
    void rethrowsOriginalWhenProviderReturnsNull() {
        setProviderSupplier(() -> exception -> null);

        BusinessException original = new BusinessException("boom");
        ActivityMiddleware middleware = new ActivityMiddleware();

        Exception thrown = assertThrows(Exception.class,
                () -> middleware.invoke(activityContext(), throwingChain(original)));

        assertSame(original, thrown);
    }

    @Test
    @DisplayName("Rethrows the original exception unchanged when no provider is registered")
    void rethrowsOriginalWhenNoProvider() {
        setProviderSupplier(() -> null);

        BusinessException original = new BusinessException("boom");
        ActivityMiddleware middleware = new ActivityMiddleware();

        Exception thrown = assertThrows(Exception.class,
                () -> middleware.invoke(activityContext(), throwingChain(original)));

        assertSame(original, thrown);
    }

    @Test
    @DisplayName("Rethrows the original exception unchanged when the provider itself throws")
    void rethrowsOriginalWhenProviderThrows() {
        setProviderSupplier(() -> exception -> {
            throw new IllegalStateException("provider is broken");
        });

        BusinessException original = new BusinessException("boom");
        ActivityMiddleware middleware = new ActivityMiddleware();

        Exception thrown = assertThrows(Exception.class,
                () -> middleware.invoke(activityContext(), throwingChain(original)));

        assertSame(original, thrown);
    }

    @Test
    @DisplayName("Does not invoke the provider for non-activity triggers")
    void passesThroughNonActivityTrigger() throws Exception {
        AtomicInteger providerCalls = new AtomicInteger();
        setProviderSupplier(() -> exception -> {
            providerCalls.incrementAndGet();
            return Collections.singletonMap("k", "v");
        });

        MiddlewareContext context = mock(MiddlewareContext.class);
        when(context.getParameterName(anyString())).thenReturn(null); // not an activity
        MiddlewareChain chain = mock(MiddlewareChain.class);
        ActivityMiddleware middleware = new ActivityMiddleware();

        middleware.invoke(context, chain);

        verify(chain, times(1)).doNext(context);
        assertEquals(0, providerCalls.get(), "provider must not be consulted for non-activities");
    }

    @Test
    @DisplayName("Reshapes nested causes into a nested innerFailure payload")
    void reshapesNestedCauses() {
        setProviderSupplier(() -> exception -> {
            // Attach a property only to the outer exception so we can assert nesting shape.
            if ("outer".equals(exception.getMessage())) {
                return Collections.singletonMap("layer", "outer");
            }
            return null;
        });

        BusinessException cause = new BusinessException("inner");
        Exception outer = new RuntimeException("outer", cause);
        ActivityMiddleware middleware = new ActivityMiddleware();

        Exception thrown = assertThrows(Exception.class,
                () -> middleware.invoke(activityContext(), throwingChain(outer)));

        String message = thrown.getMessage();
        assertNotNull(message);
        assertTrue(message.contains("\"innerFailure\":{"), message);
        assertTrue(message.contains("\"errorType\":\"" + BusinessException.class.getName() + "\""),
                message);
        assertTrue(message.contains("\"layer\":\"outer\""), message);
    }

    // --- Cross-class-loader SPI discovery (the worker-thread regression guard) ---

    @Test
    @DisplayName("discoverProvider returns null when no candidate class loader exposes the SPI file")
    void discoverProviderReturnsNullWhenNoServiceFileVisible() {
        ClassLoader blind = getClass().getClassLoader();
        assertNull(discoverProvider(new ClassLoader[] {blind}));
    }

    @Test
    @DisplayName("discoverProvider falls back past a provider-blind class loader to one that "
            + "exposes the SPI file")
    void discoverProviderFallsBackToClassLoaderThatSeesServiceFile() throws Exception {
        ClassLoader blind = getClass().getClassLoader();
        URLClassLoader appLike = newClassLoaderExposingProvider(blind);
        try {
            // The first (blind) class loader mirrors the Azure Functions worker thread's context
            // class loader, which cannot see the app's META-INF/services registration. Discovery
            // must not stop there; it must fall back to the class loader that does.
            ExceptionPropertiesProvider provider =
                    discoverProvider(new ClassLoader[] {blind, appLike});

            assertNotNull(provider, "provider should be discovered via the fallback class loader");
            assertInstanceOf(ExceptionPropertiesProvider.class, provider);
            assertEquals(TestExceptionPropertiesProvider.class.getName(),
                    provider.getClass().getName());
        } finally {
            appLike.close();
        }
    }

    @Test
    @DisplayName("discoverProvider skips null and duplicate candidate class loaders")
    void discoverProviderSkipsNullAndDuplicateCandidates() throws Exception {
        ClassLoader blind = getClass().getClassLoader();
        URLClassLoader appLike = newClassLoaderExposingProvider(blind);
        try {
            ExceptionPropertiesProvider provider = discoverProvider(
                    new ClassLoader[] {null, blind, blind, appLike, appLike});
            assertNotNull(provider);
            assertEquals(TestExceptionPropertiesProvider.class.getName(),
                    provider.getClass().getName());
        } finally {
            appLike.close();
        }
    }

    /**
     * Builds a URLClassLoader that exposes a {@code META-INF/services} registration for
     * {@link TestExceptionPropertiesProvider}. The provider class itself is loaded via the parent
     * (so it resolves to the same {@link ExceptionPropertiesProvider} type), while the service file
     * is served from this loader's own URL root — mirroring how an app jar carries its SPI file.
     */
    private URLClassLoader newClassLoaderExposingProvider(ClassLoader parent) throws IOException {
        Path root = Files.createTempDirectory(tempDir, "amw-spi-");
        Path servicesDir = root.resolve("META-INF").resolve("services");
        Files.createDirectories(servicesDir);
        Path serviceFile = servicesDir.resolve(ExceptionPropertiesProvider.class.getName());
        Files.write(serviceFile,
                (TestExceptionPropertiesProvider.class.getName() + System.lineSeparator())
                        .getBytes(StandardCharsets.UTF_8));

        URL rootUrl = root.toUri().toURL();
        return new URLClassLoader(new URL[] {rootUrl}, parent);
    }

    private static void assertNotSameInstance(Object unexpected, Object actual) {
        assertFalse(unexpected == actual,
                "expected a different instance than the original exception");
    }
}
