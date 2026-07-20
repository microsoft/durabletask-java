/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.durabletask.azurefunctions.internal.middleware;

import com.microsoft.azure.functions.internal.spi.middleware.Middleware;
import com.microsoft.azure.functions.internal.spi.middleware.MiddlewareChain;
import com.microsoft.azure.functions.internal.spi.middleware.MiddlewareContext;
import com.microsoft.durabletask.ExceptionPropertiesProvider;

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
 * {@link ExceptionPropertiesProvider} the chance to attach custom properties to the failure. If the
 * provider returns any properties, the exception is reshaped into a serialized
 * {@code TaskFailureDetails} JSON payload (matching the protobuf JSON shape) so the Durable Task
 * host extension can surface the structured properties on {@code FailureDetails.Properties}. This
 * mirrors the {@code durable-functions} JavaScript SDK's activity handler wrapper.
 *
 * <p>If no provider is registered, or it yields no properties for the thrown exception, the original
 * exception is re-thrown untouched so the legacy failure behavior is preserved.
 *
 * <p>The provider is discovered via {@link ServiceLoader} (SPI): an application registers its
 * implementation in {@code META-INF/services/com.microsoft.durabletask.ExceptionPropertiesProvider}.
 *
 * <p>This class is internal and is hence not for public use. Its APIs are unstable and can change
 * at any time.
 */
public class ActivityMiddleware implements Middleware {

    private static final String ACTIVITY_TRIGGER = "DurableActivityTrigger";
    private static final int MAX_INNER_FAILURE_DEPTH = 10;
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

            Throwable userException = unwrap(e);
            Map<String, Object> properties = safeGetProperties(provider, userException);
            if (properties == null || properties.isEmpty()) {
                // No custom properties for this failure - preserve the original behavior.
                throw e;
            }

            throw new StructuredActivityFailure(buildFailureDetailsJson(userException, properties, provider));
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

    /**
     * Invokes the provider defensively, returning {@code null} if the failure is not an
     * {@link Exception} or the provider itself throws, so a misbehaving provider never masks the
     * original failure.
     */
    private static Map<String, Object> safeGetProperties(
            ExceptionPropertiesProvider provider,
            Throwable exception) {
        if (!(exception instanceof Exception)) {
            return null;
        }
        try {
            return provider.getExceptionProperties((Exception) exception);
        } catch (Exception providerException) {
            // Don't let a misbehaving provider mask the original failure.
            LOGGER.log(Level.WARNING,
                    "ExceptionPropertiesProvider threw while extracting properties; ignoring provider output.",
                    providerException);
            return null;
        }
    }

    /**
     * Builds the single-line JSON payload that mirrors the protobuf {@code TaskFailureDetails} shape
     * consumed by the Durable Task host extension.
     */
    private static String buildFailureDetailsJson(
            Throwable exception,
            Map<String, Object> properties,
            ExceptionPropertiesProvider provider) {
        StringBuilder sb = new StringBuilder(256);
        appendFailure(sb, exception, properties, provider, 0);
        return sb.toString();
    }

    /**
     * Recursively appends one failure level (error type/message/stack trace, any custom properties,
     * and the cause as a nested {@code innerFailure}) to the JSON buffer.
     */
    private static void appendFailure(
            StringBuilder sb,
            Throwable exception,
            Map<String, Object> properties,
            ExceptionPropertiesProvider provider,
            int depth) {
        sb.append('{');
        sb.append("\"errorType\":");
        appendString(sb, exception.getClass().getName());
        sb.append(",\"errorMessage\":");
        appendString(sb, exception.getMessage() != null ? exception.getMessage() : "");
        sb.append(",\"stackTrace\":");
        appendString(sb, getFullStackTrace(exception));
        sb.append(",\"isNonRetriable\":false");

        if (properties != null && !properties.isEmpty()) {
            sb.append(",\"properties\":");
            appendValue(sb, properties);
        }

        Throwable cause = exception.getCause();
        if (cause != null && cause != exception && depth < MAX_INNER_FAILURE_DEPTH) {
            sb.append(",\"innerFailure\":");
            appendFailure(sb, cause, safeGetProperties(provider, cause), provider, depth + 1);
        }

        sb.append('}');
    }

    /**
     * Serializes a single property value as JSON, handling strings, booleans, numbers (non-finite
     * doubles fall back to strings), maps, iterables, and arrays; anything else is written as its
     * {@code toString()}.
     */
    @SuppressWarnings("unchecked")
    private static void appendValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String) {
            appendString(sb, (String) value);
        } else if (value instanceof Boolean) {
            sb.append(((Boolean) value) ? "true" : "false");
        } else if (value instanceof Double || value instanceof Float) {
            double d = ((Number) value).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                appendString(sb, value.toString());
            } else {
                sb.append(value.toString());
            }
        } else if (value instanceof Number) {
            sb.append(value.toString());
        } else if (value instanceof Map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                appendString(sb, String.valueOf(entry.getKey()));
                sb.append(':');
                appendValue(sb, entry.getValue());
            }
            sb.append('}');
        } else if (value instanceof Iterable) {
            sb.append('[');
            boolean first = true;
            for (Object item : (Iterable<Object>) value) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                appendValue(sb, item);
            }
            sb.append(']');
        } else if (value instanceof Object[]) {
            sb.append('[');
            Object[] array = (Object[]) value;
            for (int i = 0; i < array.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                appendValue(sb, array[i]);
            }
            sb.append(']');
        } else {
            appendString(sb, value.toString());
        }
    }

    /** Appends {@code value} as a JSON string literal, escaping quotes, backslashes, and control characters. */
    private static void appendString(StringBuilder sb, String value) {
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                    break;
            }
        }
        sb.append('"');
    }

    /** Formats the throwable's stack trace as newline-separated {@code \tat ...} frames. */
    private static String getFullStackTrace(Throwable e) {
        StackTraceElement[] elements = e.getStackTrace();
        StringBuilder sb = new StringBuilder(elements.length * 64);
        for (StackTraceElement element : elements) {
            sb.append("\tat ").append(element.toString()).append(System.lineSeparator());
        }
        return sb.toString();
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
