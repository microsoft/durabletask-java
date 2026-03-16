// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    /**
     * The current entity context, providing access to entity metadata such as the entity ID
     * and the ability to signal other entities.
     * <p>
     * This property is automatically set before each operation dispatch and is available for use
     * in operation methods. This mirrors the .NET SDK's {@code TaskEntity.Context} property.
     */
    protected TaskEntityContext context;

    /**
     * Controls whether operations can be dispatched to methods on the state object.
     * When {@code true}, if no matching method is found on the entity class itself,
     * the framework will look for a matching method on the state object.
     * When {@code false} (the default), only methods on the entity class are considered.
     * <p>
     * This matches the .NET SDK default where {@code AllowStateDispatch} is {@code false}.
     */
    private boolean allowStateDispatch = false;

    // Cache for resolved methods, keyed by (class, operationName).
    // Uses Optional<Method> so that "not found" results are also cached.
    private static final Map<String, Optional<Method>> methodCache = new ConcurrentHashMap<>();

    /**
     * Creates a new {@code TaskEntity} instance.
     */
    protected TaskEntity() {
    }

    /**
     * Gets whether state dispatch is allowed.
     *
     * @return {@code true} if operations can be dispatched to state object methods
     */
    protected boolean getAllowStateDispatch() {
        return this.allowStateDispatch;
    }

    /**
     * Sets whether operations can be dispatched to methods on the state object.
     * <p>
     * When {@code true}, if no matching method is found on the entity class itself,
     * the framework will look for a matching method on the state object.
     * When {@code false} (the default), only methods on the entity class are considered.
     *
     * @param allowStateDispatch {@code true} to allow state dispatch, {@code false} to disable
     */
    protected void setAllowStateDispatch(boolean allowStateDispatch) {
        this.allowStateDispatch = allowStateDispatch;
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
        // Set the context before dispatch so subclass methods can access it
        this.context = operation.getContext();

        // Step 1: Load or initialize state
        Class<TState> stateType = getStateType();
        if (stateType != null) {
            this.state = operation.getState().getState(stateType);
            if (this.state == null) {
                this.state = initializeState(operation);
            }
        }

        Object result;

        // Step 2: Try reflection dispatch on this entity class
        Method method = findMethod(this.getClass(), operation.getName());
        if (method != null) {
            result = invokeMethod(method, this, operation);
        } else if (this.allowStateDispatch && this.state != null) {
            // Step 3: Try state dispatch (only if allowStateDispatch is true)
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

        // Step 5: Save state back only on success (the executor handles rollback on failure)
        if (stateType != null) {
            operation.getState().setState(this.state);
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

    // region Re-entrant self-dispatch

    /**
     * Dispatches an operation to this entity instance synchronously (re-entrant self-call).
     * <p>
     * This allows one entity operation to invoke another operation on the same entity,
     * reusing the same state and context. The dispatched operation executes inline — state
     * mutations are visible immediately to the caller.
     * <p>
     * The operation is resolved using the same case-insensitive reflection dispatch as external
     * operations: entity class methods are tried first, then state object methods (if
     * {@linkplain #setAllowStateDispatch state dispatch} is enabled), then implicit operations.
     *
     * <p>Example:
     * <pre>{@code
     * public class BankAccount extends TaskEntity<BankAccountState> {
     *     public void deposit(int amount) {
     *         this.state.balance += amount;
     *     }
     *
     *     public void depositWithBonus(int amount) {
     *         dispatch("deposit", amount);          // re-entrant call
     *         dispatch("deposit", amount / 10);     // 10% bonus
     *     }
     * }
     * }</pre>
     *
     * @param operationName the name of the operation to dispatch (case-insensitive)
     * @return the operation result, or {@code null} for void operations
     * @throws IllegalStateException        if called outside of entity execution
     * @throws UnsupportedOperationException if no matching operation is found
     */
    protected Object dispatch(String operationName) {
        return dispatch(operationName, null);
    }

    /**
     * Dispatches an operation with input to this entity instance synchronously (re-entrant self-call).
     *
     * @param operationName the name of the operation to dispatch (case-insensitive)
     * @param input         the input value to pass to the operation, or {@code null}
     * @return the operation result, or {@code null} for void operations
     * @throws IllegalStateException        if called outside of entity execution
     * @throws UnsupportedOperationException if no matching operation is found
     * @see #dispatch(String)
     */
    protected Object dispatch(String operationName, Object input) {
        if (this.context == null) {
            throw new IllegalStateException(
                    "dispatch() can only be called during entity operation execution.");
        }

        Method method = findMethod(this.getClass(), operationName);
        Object target = this;

        if (method == null && this.allowStateDispatch && this.state != null) {
            method = findMethod(this.state.getClass(), operationName);
            target = this.state;
        }

        if (method == null) {
            if ("delete".equalsIgnoreCase(operationName)) {
                this.state = null;
                return null;
            }
            throw new UnsupportedOperationException(
                    "Entity '" + this.getClass().getSimpleName() +
                    "' does not support operation '" + operationName + "'.");
        }

        return invokeMethodDirect(method, target, input, this.context);
    }

    /**
     * Dispatches an operation with input and a typed return value (re-entrant self-call).
     *
     * @param operationName the name of the operation to dispatch (case-insensitive)
     * @param input         the input value to pass to the operation, or {@code null}
     * @param returnType    the expected return type
     * @param <V>           the return type
     * @return the operation result cast to {@code V}
     * @throws IllegalStateException        if called outside of entity execution
     * @throws UnsupportedOperationException if no matching operation is found
     * @throws ClassCastException           if the result cannot be cast to {@code returnType}
     * @see #dispatch(String, Object)
     */
    @SuppressWarnings("unchecked")
    protected <V> V dispatch(String operationName, Object input, Class<V> returnType) {
        Object result = dispatch(operationName, input);
        if (result == null) {
            return null;
        }
        // Primitive types (int.class, etc.) cannot use Class.cast() with boxed values,
        // so we let the unchecked cast handle primitive-to-wrapper conversion.
        if (returnType.isPrimitive()) {
            return (V) result;
        }
        return returnType.cast(result);
    }

    /**
     * Invokes a method with a direct (already-deserialized) input value, without requiring
     * a {@link TaskEntityOperation}. Used by {@link #dispatch} for re-entrant self-calls.
     */
    private static Object invokeMethodDirect(
            Method method, Object target, Object input, TaskEntityContext context) {
        Class<?>[] paramTypes = method.getParameterTypes();
        Object[] args = new Object[paramTypes.length];

        for (int i = 0; i < paramTypes.length; i++) {
            if (TaskEntityContext.class.isAssignableFrom(paramTypes[i])) {
                args[i] = context;
            } else if (TaskEntityOperation.class.isAssignableFrom(paramTypes[i])) {
                throw new UnsupportedOperationException(
                        "Cannot dispatch to method '" + method.getName() +
                        "' that accepts TaskEntityOperation. Use a simpler signature for " +
                        "operations that support re-entrant dispatch.");
            } else {
                args[i] = input;
            }
        }

        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getTargetException();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    // endregion

    /**
     * Finds a public method on the target class matching the operation name (case-insensitive).
     * Methods inherited from {@code Object} and from {@code TaskEntity} itself are excluded.
     */
    @Nullable
    private static Method findMethod(Class<?> targetClass, String operationName) {
        String cacheKey = targetClass.getName() + "#" + operationName.toLowerCase();
        return methodCache.computeIfAbsent(cacheKey, k -> {
            List<Method> matches = new ArrayList<>();
            for (Method m : targetClass.getMethods()) {
                // Skip static methods — only instance methods should be dispatchable
                if (Modifier.isStatic(m.getModifiers())) {
                    continue;
                }
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
                // Skip methods from JDK packages to prevent unintended state dispatch
                // (e.g., Integer.intValue(), String.length()) when state type is a JDK class
                String declaringPackage = m.getDeclaringClass().getPackageName();
                if (declaringPackage.startsWith("java.") || declaringPackage.startsWith("javax.")) {
                    continue;
                }
                if (m.getName().equalsIgnoreCase(operationName)) {
                    matches.add(m);
                }
            }
            if (matches.size() > 1) {
                throw new IllegalStateException(
                        "Ambiguous match: multiple methods named '" + operationName + "' found on " +
                        targetClass.getName() + ". Entity operation methods must have unique names.");
            }
            return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
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
