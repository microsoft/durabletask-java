// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Immutable identifier for a durable entity instance, consisting of a name and key pair.
 * <p>
 * The name typically corresponds to the entity class/type name, and the key identifies the specific
 * entity instance (e.g., a user ID or account number).
 */
public final class EntityInstanceId implements Comparable<EntityInstanceId> {
    private final String name;
    private final String key;

    /**
     * Creates a new {@code EntityInstanceId} with the specified name and key.
     *
     * @param name the entity name (type), must not be null
     * @param key  the entity key (instance identifier), must not be null
     * @throws IllegalArgumentException if name or key is null or empty
     */
    public EntityInstanceId(@Nonnull String name, @Nonnull String key) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Entity name must not be null or empty.");
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Entity key must not be null or empty.");
        }
        this.name = name;
        this.key = key;
    }

    /**
     * Gets the entity name (type).
     *
     * @return the entity name
     */
    @Nonnull
    public String getName() {
        return this.name;
    }

    /**
     * Gets the entity key (instance identifier).
     *
     * @return the entity key
     */
    @Nonnull
    public String getKey() {
        return this.key;
    }

    /**
     * Parses an {@code EntityInstanceId} from its string representation.
     * <p>
     * The expected format is {@code @{name}@{key}}, matching the .NET {@code EntityId.ToString()} format.
     *
     * @param value the string to parse
     * @return the parsed {@code EntityInstanceId}
     * @throws IllegalArgumentException if the string is not in the expected format
     */
    @Nonnull
    public static EntityInstanceId fromString(@Nonnull String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Value must not be null or empty.");
        }
        if (!value.startsWith("@")) {
            throw new IllegalArgumentException(
                    "Invalid EntityInstanceId format. Expected '@{name}@{key}', got: " + value);
        }
        int secondAt = value.indexOf('@', 1);
        if (secondAt < 0) {
            throw new IllegalArgumentException(
                    "Invalid EntityInstanceId format. Expected '@{name}@{key}', got: " + value);
        }
        String name = value.substring(1, secondAt);
        String key = value.substring(secondAt + 1);
        return new EntityInstanceId(name, key);
    }

    /**
     * Returns the string representation in the format {@code @{name}@{key}}.
     *
     * @return the string representation of this entity instance ID
     */
    @Override
    public String toString() {
        return "@" + this.name + "@" + this.key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EntityInstanceId that = (EntityInstanceId) o;
        return this.name.equals(that.name) && this.key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.key);
    }

    @Override
    public int compareTo(@Nonnull EntityInstanceId other) {
        int nameCompare = this.name.compareTo(other.name);
        if (nameCompare != 0) {
            return nameCompare;
        }
        return this.key.compareTo(other.key);
    }
}
