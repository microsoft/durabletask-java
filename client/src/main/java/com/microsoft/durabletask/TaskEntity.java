// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for durable entities that provides automatic reflection-based operation dispatch.
 * <p>
 * Subclasses define operations as public methods. When an operation is received, {@code TaskEntity}
 * resolves it by:
 * <ol>
 *   <li><b>Reflection dispatch on {@code this}</b>: Look for a public method on the entity class
 *       whose name matches the operation name (case-insensitive).</li>
 *   <li><b>State dispatch</b>: If no method is found on the entity, look for a matching public
 *       method on the {@code TState} object.</li>
 *   <li><b>Implicit delete</b>: If the operation name is "delete" and no explicit method exists,
 *       delete the entity state.</li>
 *   <li>If none of the above match, throw {@link UnsupportedOperationException}.</li>
 * </ol>
 * <p>
 * Methods may accept 0, 1, or 2 parameters. Supported parameter types are the operation input type
 * and {@link TaskEntityContext}. The method may return the operation result or be {@code void}.
 *
 * <p>Example:
 * <pre>{@code
 * public class CounterEntity extends TaskEntity<Integer> {
 *     public void add(int amount) { this.state += amount; }
 *     public void reset() { this.state = 0; }
 *     public int get() { return this.state; }
 *
 *     protected Integer initializeState(TaskEntityOperation operation) {
 *         return 0;
 *     }
 * }
 * }</pre>
 *
 * @param <TState> the type of the entity's state
 */
public abstract class TaskEntity<TState> implements ITaskEntity {

    /**
     * The current state of the entity. Subclasses may read and write this field directly
     * in their operation methods.
     */
    protected TState state;

    // Cache for resolved methods, keyed by (class, operationName).
    // Uses Optional<Method> so that "not found" results are also cached.
    private static final Map<String, Optional<Method>> methodCache = new ConcurrentHashMap<>();

    /**
     * Creates a new {@code TaskEntity} instance.
     */
    protected TaskEntity() {
    }

    /**
     * Called to initialize the entity state when no prior state exists.
     * <p>
     * The default implementation attempts to create a new instance of {@code TState} using
     * its no-arg constructor via reflection. Override this method to provide custom initialization.
     *
     * @param operation the operation that triggered state initialization
     * @return the initial state value
     */
    @SuppressWarnings("unchecked")
    protected TState initializeState(TaskEntityOperation operation) {
        Class<TState> stateType = getStateType();
        if (stateType == null) {
            return null;
        }
        try {
            return stateType.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize entity state of type " + stateType.getName() +
                    ". Override initializeState() to provide custom initialization.", e);
        }
    }

    /**
     * Gets the runtime class of the state type parameter. Override this method in concrete
     * entity classes to provide the state type class, which is needed for deserialization.
     *
     * @return the state type class, or {@code null} if not applicable
     */
    @Nullable
    protected abstract Class<TState> getStateType();

    @Override
    public Object runAsync(TaskEntityOperation operation) throws Exception {
        // Step 1: Load or initialize state
        Class<TState> stateType = getStateType();
        if (stateType != null) {
            this.state = operation.getState().getState(stateType);
            if (this.state == null) {
                this.state = initializeState(operation);
            }
        }

        Object result;
        try {
            // Step 2: Try reflection dispatch on this entity class
            Method method = findMethod(this.getClass(), operation.getName());
            if (method != null) {
                result = invokeMethod(method, this, operation);
            } else if (this.state != null) {
                // Step 3: Try state dispatch
                Method stateMethod = findMethod(this.state.getClass(), operation.getName());
                if (stateMethod != null) {
                    result = invokeMethod(stateMethod, this.state, operation);
                } else {
                    // Step 4: Implicit delete
                    result = handleImplicitOperations(operation);
                }
            } else {
                // Step 4: Implicit delete (no state loaded)
                result = handleImplicitOperations(operation);
            }
        } finally {
            // Step 5: Save state back regardless of dispatch path
            if (stateType != null) {
                operation.getState().setState(this.state);
            }
        }

        return result;
    }

    private Object handleImplicitOperations(TaskEntityOperation operation) {
        if ("delete".equalsIgnoreCase(operation.getName())) {
            operation.getState().deleteState();
            this.state = null;
            return null;
        }
        throw new UnsupportedOperationException(
                "Entity '" + this.getClass().getSimpleName() + "' does not support operation '" +
                operation.getName() + "'.");
    }

    /**
     * Finds a public method on the target class matching the operation name (case-insensitive).
     * Methods inherited from {@code Object} and from {@code TaskEntity} itself are excluded.
     */
    @Nullable
    private static Method findMethod(Class<?> targetClass, String operationName) {
        String cacheKey = targetClass.getName() + "#" + operationName.toLowerCase();
        return methodCache.computeIfAbsent(cacheKey, k -> {
            for (Method m : targetClass.getMethods()) {
                // Skip methods from Object
                if (m.getDeclaringClass() == Object.class) {
                    continue;
                }
                // Skip methods from TaskEntity base class itself
                if (m.getDeclaringClass() == TaskEntity.class) {
                    continue;
                }
                // Skip methods from ITaskEntity interface
                if (m.getDeclaringClass() == ITaskEntity.class) {
                    continue;
                }
                if (m.getName().equalsIgnoreCase(operationName)) {
                    return Optional.of(m);
                }
            }
            return Optional.empty();
        }).orElse(null);
    }

    /**
     * Invokes the resolved method with appropriate parameter binding.
     * <p>
     * Supports 0–2 parameters among:
     * <ul>
     *   <li>The operation input (deserialized to the parameter type)</li>
     *   <li>{@link TaskEntityContext}</li>
     * </ul>
     */
    private static Object invokeMethod(Method method, Object target, TaskEntityOperation operation)
            throws Exception {
        Class<?>[] paramTypes = method.getParameterTypes();
        Object[] args = new Object[paramTypes.length];

        for (int i = 0; i < paramTypes.length; i++) {
            if (TaskEntityContext.class.isAssignableFrom(paramTypes[i])) {
                args[i] = operation.getContext();
            } else if (TaskEntityOperation.class.isAssignableFrom(paramTypes[i])) {
                args[i] = operation;
            } else {
                // Assume this is the input parameter
                args[i] = operation.getInput(paramTypes[i]);
            }
        }

        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            // Unwrap the target exception
            Throwable cause = e.getTargetException();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }
}
