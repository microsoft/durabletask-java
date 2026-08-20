/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.durabletask.azurefunctions.internal.middleware;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.microsoft.azure.functions.internal.spi.middleware.Middleware;
import com.microsoft.azure.functions.internal.spi.middleware.MiddlewareChain;
import com.microsoft.azure.functions.internal.spi.middleware.MiddlewareContext;
import com.microsoft.durabletask.ExceptionPropertiesProvider;
import com.microsoft.durabletask.FailureDetails;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Durable Function Activity Middleware.
 *
 * <p>When an activity function throws, this middleware gives a registered
 * {@link ExceptionPropertiesProvider} the chance to attach custom properties to the failure or any
 * exception in its causal chain. If the provider returns any properties, the exception is reshaped into a serialized
 * {@code TaskFailureDetails} JSON payload (matching the protobuf JSON shape) so the Durable Task
 * host extension can surface the structured properties on {@code FailureDetails.Properties}.
 * <p>If no provider is registered, or it yields no properties for the thrown exception, the original
 * exception is re-thrown untouched.
 *
 * <p>The provider is discovered via {@link ServiceLoader} (SPI): an application registers its
 * implementation in {@code META-INF/services/com.microsoft.durabletask.ExceptionPropertiesProvider}.
 */
public class ActivityMiddleware implements Middleware {

    private static final String ACTIVITY_TRIGGER = "DurableActivityTrigger";
    private static final JsonFormat.Printer FAILURE_DETAILS_JSON_PRINTER =
            JsonFormat.printer().omittingInsignificantWhitespace();
    private static final Logger LOGGER = Logger.getLogger(ActivityMiddleware.class.getName());

    private static final Object PROVIDER_LOCK = new Object();
    private static volatile boolean providerLoaded = false;
    private static ExceptionPropertiesProvider cachedProvider;

    // Test-only override. When non-null, this supplier replaces SPI discovery so tests can inject a
    // provider (or {@code null}) without registering a real one. Set/cleared via reflection.
    private static Supplier<ExceptionPropertiesProvider> providerSupplierOverride;

    /**
     * Runs the activity and, if it fails and a provider supplies custom properties, replaces the
     * failure with a structured {@code TaskFailureDetails} JSON payload; otherwise the original
     * exception is rethrown unchanged. Non-activity invocations pass straight through.
     */
    @Override
    public void invoke(MiddlewareContext context, MiddlewareChain chain) throws Exception {
        String parameterName = context.getParameterName(ACTIVITY_TRIGGER);
        if (parameterName == null) {
            chain.doNext(context);
            return;
        }

        try {
            chain.doNext(context);
        } catch (Exception e) {
            ExceptionPropertiesProvider provider = getProvider();
            if (provider == null) {
                throw e;
            }

            FailureDetails failureDetails = FailureDetails.fromException(unwrap(e), provider);
            if (!hasCustomProperties(failureDetails)) {
                // No custom properties for this failure chain - preserve the original behavior.
                throw e;
            }

            try {
                throw new StructuredActivityFailure(
                        FAILURE_DETAILS_JSON_PRINTER.print(failureDetails.toProto()));
            } catch (InvalidProtocolBufferException serializationException) {
                LOGGER.log(Level.WARNING,
                        "Failed to serialize structured failure details; rethrowing the original exception.",
                        serializationException);
                throw e;
            }
        }
    }

    /**
     * Lazily resolves and caches the {@link ExceptionPropertiesProvider}, using the test override
     * when present and otherwise discovering it via SPI. The result (including {@code null}) is
     * cached for the lifetime of the worker.
     */
    private static ExceptionPropertiesProvider getProvider() {
        if (!providerLoaded) {
            synchronized (PROVIDER_LOCK) {
                if (!providerLoaded) {
                    cachedProvider = providerSupplierOverride != null
                            ? providerSupplierOverride.get()
                            : discoverProvider();
                    providerLoaded = true;
                }
            }
        }
        return cachedProvider;
    }

    /**
     * Discovers the app-registered {@link ExceptionPropertiesProvider} via SPI, trying the thread
     * context, middleware, and interface class loaders in turn (the worker thread's context loader
     * may not see the app's {@code META-INF/services} registration).
     */
    private static ExceptionPropertiesProvider discoverProvider() {
        return discoverProvider(new ClassLoader[] {
                Thread.currentThread().getContextClassLoader(),
                ActivityMiddleware.class.getClassLoader(),
                ExceptionPropertiesProvider.class.getClassLoader(),
        });
    }

    /**
     * Returns the first {@link ExceptionPropertiesProvider} found by {@link ServiceLoader} across
     * the given class loaders (nulls and duplicates skipped), or {@code null} if none is found.
     * This is the seam that guards against the worker-thread class loader regression.
     */
    private static ExceptionPropertiesProvider discoverProvider(ClassLoader[] candidates) {
        ClassLoader previous = null;
        for (ClassLoader classLoader : candidates) {
            if (classLoader == null || classLoader == previous) {
                continue;
            }
            previous = classLoader;
            try {
                ServiceLoader<ExceptionPropertiesProvider> loader =
                        ServiceLoader.load(ExceptionPropertiesProvider.class, classLoader);
                Iterator<ExceptionPropertiesProvider> iterator = loader.iterator();
                if (iterator.hasNext()) {
                    return iterator.next();
                }
            } catch (Throwable t) {
                // Discovery failures must not break activity execution; the feature is opt-in.
                LOGGER.log(Level.WARNING,
                        "Failed to load ExceptionPropertiesProvider via ServiceLoader using " + classLoader,
                        t);
            }
        }
        return null;
    }

    /**
     * Test-only. Overrides SPI discovery with the given supplier ({@code null} simulates "no
     * provider registered") and clears the cache so the next lookup re-runs. Invoked via reflection.
     */
    private static void setProviderSupplierForTesting(Supplier<ExceptionPropertiesProvider> supplier) {
        synchronized (PROVIDER_LOCK) {
            providerSupplierOverride = supplier;
            providerLoaded = false;
            cachedProvider = null;
        }
    }

    /**
     * Test-only. Restores real SPI discovery and clears the cached provider so tests do not leak
     * state into one another (the provider is cached in a static field). Invoked via reflection.
     */
    private static void resetProviderCacheForTesting() {
        synchronized (PROVIDER_LOCK) {
            providerSupplierOverride = null;
            providerLoaded = false;
            cachedProvider = null;
        }
    }

    /**
     * Unwraps reflective {@link InvocationTargetException} layers to reach the user exception that
     * actually caused the activity to fail.
     */
    private static Throwable unwrap(Throwable e) {
        Throwable current = e;
        while (current instanceof InvocationTargetException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static boolean hasCustomProperties(FailureDetails failureDetails) {
        for (FailureDetails current = failureDetails;
                current != null;
                current = current.getInnerFailure()) {
            Map<String, Object> properties = current.getProperties();
            if (properties != null && !properties.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Internal exception whose message carries the serialized {@code TaskFailureDetails} JSON
     * payload. It intentionally has no cause so the Java worker reports its message verbatim.
     */
    private static final class StructuredActivityFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        StructuredActivityFailure(String message) {
            super(message, null, false, false);
        }
    }
}
