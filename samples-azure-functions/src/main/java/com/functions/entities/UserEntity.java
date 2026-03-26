// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.durabletask.AbstractTaskEntity;
import com.microsoft.durabletask.TaskEntityContext;
import com.microsoft.durabletask.TaskEntityOperation;

/**
 * Entity that demonstrates complex state management and interaction with orchestrations.
 * <p>
 * This mirrors the .NET {@code UserEntity} from {@code User.cs} and shows:
 * <ul>
 *   <li>Setting and updating complex state</li>
 *   <li>Using {@link TaskEntityContext} to schedule orchestrations from within an entity</li>
 *   <li>Custom {@link #initializeState} for types without a no-arg constructor pattern</li>
 *   <li>Implicit "delete" handling from the {@link AbstractTaskEntity} base class</li>
 * </ul>
 */
public class UserEntity extends AbstractTaskEntity<UserState> {

    /**
     * Sets the user state to the given value.
     */
    public void set(UserState user) {
        this.state = user;
    }

    /**
     * Partially updates the user state. Only non-null fields are applied.
     */
    public void update(UserUpdate update) {
        String newName = update.getName() != null ? update.getName() : this.state.getName();
        int newAge = update.getAge() != null ? update.getAge() : this.state.getAge();
        this.state = new UserState(newName, newAge);
    }

    /**
     * Starts a greeting orchestration for this user.
     * <p>
     * Demonstrates binding {@link TaskEntityContext} as a method parameter.
     * The SDK automatically injects the context when dispatching to this method.
     * This is equivalent to .NET's {@code Greet(TaskEntityContext context, string? message = null)}.
     * <p>
     * When using {@code TaskEntity<T>}, the context is also accessible via {@code this.context},
     * but parameter binding works for both entity-based and state-dispatch modes.
     *
     * @param context the entity context, automatically injected by the dispatch engine
     * @param message optional custom greeting message (may be null)
     */
    public void greet(TaskEntityContext context, String message) {
        if (this.state.getName() == null) {
            throw new IllegalStateException("User has not been initialized.");
        }

        // Get access to TaskEntityContext by adding it as a parameter. Can be with or without
        // an input parameter. Order does not matter.
        GreetingInput input = new GreetingInput(
                this.state.getName(), this.state.getAge(), message);
        context.startNewOrchestration("GreetingOrchestration", input);
    }

    @Override
    protected UserState initializeState(TaskEntityOperation operation) {
        // UserState doesn't need special initialization, but this shows
        // how to customize default state (mirroring .NET's new User(null!, -1))
        return new UserState(null, -1);
    }

    @Override
    protected Class<UserState> getStateType() {
        return UserState.class;
    }

    /**
     * Input payload for the GreetingOrchestration.
     */
    public static class GreetingInput {
        public String name;
        public int age;
        public String customMessage;

        public GreetingInput() {
        }

        public GreetingInput(String name, int age, String customMessage) {
            this.name = name;
            this.age = age;
            this.customMessage = customMessage;
        }
    }
}
