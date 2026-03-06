// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

/**
 * Partial update DTO for the User entity.
 * <p>
 * Fields that are {@code null} are not updated. This mirrors the .NET {@code UserUpdate} record.
 */
public class UserUpdate {
    private String name;
    private Integer age;

    public UserUpdate() {
    }

    public UserUpdate(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
