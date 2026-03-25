// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nonnull;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;

/**
 * Creates type-safe proxies for interacting with durable entities from orchestrations.
 * <p>
 * A typed entity proxy is a JDK dynamic proxy that implements a user-defined interface.
 * Method calls on the proxy are translated into entity operations:
 * <ul>
 *   <li>{@code void} methods become fire-and-forget signals via
 *       {@link TaskOrchestrationContext#signalEntity}</li>
 *   <li>Methods returning {@link Task}{@code <V>} become two-way calls via
 *       {@link TaskOrchestrationContext#callEntity}</li>
 * </ul>
 * <p>
 * The method name is used as the entity operation name (case-insensitive matching on the
 * entity side). Methods must accept 0 or 1 parameters; the single parameter is passed as
 * the operation input.
 *
 * <p>Example:
 * <pre>{@code
 * // Define entity operations as an interface
 * public interface ICounter {
 *     void add(int amount);            // fire-and-forget signal
 *     void reset();                    // fire-and-forget signal
 *     Task<Integer> get();             // two-way call returning a result
 * }
 *
 * // Use in an orchestration
 * ICounter counter = ctx.createEntityProxy(entityId, ICounter.class);
 * counter.add(5);
 * counter.reset();
 * int value = counter.get().await();
 * }</pre>
 *
 * @see TaskOrchestrationContext#createEntityProxy(EntityInstanceId, Class)
 * @see TaskOrchestrationEntityFeature#createProxy(EntityInstanceId, Class)
 */
public final class EntityProxy {

    private EntityProxy() {
        // Utility class — not instantiable
    }

    /**
     * Creates a typed entity proxy for the given entity instance.
     *
     * @param ctx            the orchestration context (used to send signals and calls)
     * @param entityId       the target entity's instance ID
     * @param proxyInterface the interface whose methods map to entity operations
     * @param <T>            the proxy interface type
     * @return a proxy instance that implements {@code proxyInterface}
     * @throws IllegalArgumentException if {@code proxyInterface} is not an interface
     */
    @SuppressWarnings("unchecked")
    public static <T> T create(
            @Nonnull TaskOrchestrationContext ctx,
            @Nonnull EntityInstanceId entityId,
            @Nonnull Class<T> proxyInterface) {
        if (ctx == null) {
            throw new IllegalArgumentException("ctx must not be null");
        }
        if (entityId == null) {
            throw new IllegalArgumentException("entityId must not be null");
        }
        if (proxyInterface == null) {
            throw new IllegalArgumentException("proxyInterface must not be null");
        }
        if (!proxyInterface.isInterface()) {
            throw new IllegalArgumentException(
                    "proxyInterface must be an interface, got: " + proxyInterface.getName());
        }

        return (T) Proxy.newProxyInstance(
                proxyInterface.getClassLoader(),
                new Class<?>[]{ proxyInterface },
                new EntityInvocationHandler(ctx, entityId));
    }

    /**
     * Invocation handler that translates interface method calls into entity operations.
     */
    private static final class EntityInvocationHandler implements InvocationHandler {
        private final TaskOrchestrationContext ctx;
        private final EntityInstanceId entityId;

        EntityInvocationHandler(TaskOrchestrationContext ctx, EntityInstanceId entityId) {
            this.ctx = ctx;
            this.entityId = entityId;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // Handle java.lang.Object methods
            if (method.getDeclaringClass() == Object.class) {
                switch (method.getName()) {
                    case "toString":
                        return "EntityProxy[" + entityId + "]";
                    case "hashCode":
                        return entityId.hashCode();
                    case "equals":
                        if (args == null || args.length == 0) {
                            return false;
                        }
                        if (args[0] == proxy) {
                            return true;
                        }
                        if (args[0] == null || !Proxy.isProxyClass(args[0].getClass())) {
                            return false;
                        }
                        InvocationHandler otherHandler = Proxy.getInvocationHandler(args[0]);
                        if (otherHandler instanceof EntityInvocationHandler) {
                            return entityId.equals(((EntityInvocationHandler) otherHandler).entityId);
                        }
                        return false;
                    default:
                        return method.invoke(this, args);
                }
            }

            String operationName = method.getName();

            if (args != null && args.length > 1) {
                throw new UnsupportedOperationException(
                        "Entity proxy methods must have 0 or 1 parameters. " +
                        "Method '" + operationName + "' has " + args.length + " parameters. " +
                        "Use a single wrapper object to pass multiple values.");
            }

            Object input = (args != null && args.length == 1) ? args[0] : null;

            Class<?> returnType = method.getReturnType();

            if (returnType == void.class) {
                // Fire-and-forget signal
                ctx.signalEntity(entityId, operationName, input);
                return null;
            } else if (Task.class.isAssignableFrom(returnType)) {
                // Two-way entity call — extract the Task<V> type parameter
                Class<?> resultType = extractTaskTypeParameter(method);
                return ctx.callEntity(entityId, operationName, input, resultType);
            } else {
                throw new UnsupportedOperationException(
                        "Entity proxy methods must return void (for signals) or Task<V> (for calls). " +
                        "Method '" + operationName + "' returns " + returnType.getName() + ".");
            }
        }

        /**
         * Extracts the generic type parameter from a method returning {@code Task<V>}.
         * Falls back to {@code Void.class} if the type cannot be determined.
         */
        private static Class<?> extractTaskTypeParameter(Method method) {
            Type genericReturnType = method.getGenericReturnType();
            if (genericReturnType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) genericReturnType;
                Type[] typeArgs = pt.getActualTypeArguments();
                if (typeArgs.length > 0) {
                    return getRawClass(typeArgs[0]);
                }
            }
            return Void.class;
        }

        /**
         * Resolves a {@link Type} to its raw {@link Class}, handling parameterized types
         * and wildcard types.
         */
        private static Class<?> getRawClass(Type type) {
            if (type instanceof Class) {
                return (Class<?>) type;
            } else if (type instanceof ParameterizedType) {
                return getRawClass(((ParameterizedType) type).getRawType());
            } else if (type instanceof java.lang.reflect.WildcardType) {
                java.lang.reflect.WildcardType wt = (java.lang.reflect.WildcardType) type;
                Type[] upperBounds = wt.getUpperBounds();
                if (upperBounds.length > 0) {
                    return getRawClass(upperBounds[0]);
                }
            }
            return Object.class;
        }
    }
}
