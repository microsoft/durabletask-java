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

    // Visible for testing only. When non-null, this supplier replaces SPI discovery so unit tests
    // can exercise the reshaping and pass-through behavior without registering a real provider.
    private static Supplier<ExceptionPropertiesProvider> providerSupplierOverride;

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

            throw new StructuredActivityFailure(buildFailureDetailsJson(userException, provider));
        }
    }

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

    private static ExceptionPropertiesProvider discoverProvider() {
        // The provider is registered via SPI in the function app's jar. Depending on how the
        // Azure Functions Java worker dispatches invocations, the thread context class loader may
        // be the worker's class loader (which cannot see the app's META-INF/services registration)
        // rather than the app class loader. Try several candidate class loaders and use the first
        // one that yields a provider. The class loader that loaded this middleware is bundled with
        // the app (durabletask-azure-functions is an app dependency), so it can see the app's SPI
        // registration and is the most reliable fallback.
        return discoverProvider(new ClassLoader[] {
                Thread.currentThread().getContextClassLoader(),
                ActivityMiddleware.class.getClassLoader(),
                ExceptionPropertiesProvider.class.getClassLoader(),
        });
    }

    // Visible for testing. Iterates the candidate class loaders in order and returns the first
    // provider discovered via SPI, skipping nulls and duplicates. This is the seam that guards
    // against the worker-thread class loader regression: discovery must not stop at the (possibly
    // provider-blind) thread context class loader.
    static ExceptionPropertiesProvider discoverProvider(ClassLoader[] candidates) {
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
                LOGGER.warning("Failed to load ExceptionPropertiesProvider via ServiceLoader using "
                        + classLoader + ": " + t);
            }
        }
        return null;
    }

    // Visible for testing. Overrides SPI discovery with the given supplier (may be {@code null} to
    // simulate "no provider registered") and clears the cached provider so the next lookup re-runs.
    static void setProviderSupplierForTesting(Supplier<ExceptionPropertiesProvider> supplier) {
        synchronized (PROVIDER_LOCK) {
            providerSupplierOverride = supplier;
            providerLoaded = false;
            cachedProvider = null;
        }
    }

    // Visible for testing. Restores real SPI discovery and clears any cached provider so tests do
    // not leak state into one another (the provider is cached in a static field).
    static void resetProviderCacheForTesting() {
        synchronized (PROVIDER_LOCK) {
            providerSupplierOverride = null;
            providerLoaded = false;
            cachedProvider = null;
        }
    }

    private static Throwable unwrap(Throwable e) {
        Throwable current = e;
        while (current instanceof InvocationTargetException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

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
            LOGGER.warning("ExceptionPropertiesProvider threw while extracting properties: " + providerException);
            return null;
        }
    }

    // Builds the single-line JSON payload that mirrors the protobuf TaskFailureDetails shape
    // consumed by the Durable Task host extension.
    private static String buildFailureDetailsJson(Throwable exception, ExceptionPropertiesProvider provider) {
        StringBuilder sb = new StringBuilder(256);
        appendFailure(sb, exception, provider, 0);
        return sb.toString();
    }

    private static void appendFailure(
            StringBuilder sb,
            Throwable exception,
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

        Map<String, Object> properties = safeGetProperties(provider, exception);
        if (properties != null && !properties.isEmpty()) {
            sb.append(",\"properties\":");
            appendValue(sb, properties);
        }

        Throwable cause = exception.getCause();
        if (cause != null && cause != exception && depth < MAX_INNER_FAILURE_DEPTH) {
            sb.append(",\"innerFailure\":");
            appendFailure(sb, cause, provider, depth + 1);
        }

        sb.append('}');
    }

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
