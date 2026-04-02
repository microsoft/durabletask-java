// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

/**
 * State class for the {@link UserEntity}.
 * <p>
 * This mirrors the .NET {@code User} record from {@code User.cs}.
 */
public class UserState {
    private String name;
    private int age;

    public UserState() {
    }

    public UserState(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "UserState{name='" + name + "', age=" + age + "}";
    }
}
