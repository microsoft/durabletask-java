// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An auto-paginating iterable over entity query results with typed state access.
 * <p>
 * This class wraps an {@link EntityQueryPageable} and yields {@link TypedEntityMetadata} items
 * with eagerly deserialized state, mirroring the .NET SDK's {@code AsyncPageable<EntityMetadata<TState>>}
 * returned by {@code GetAllEntitiesAsync<T>()}.
 * <p>
 * Use {@link DurableEntityClient#getAllEntities(EntityQuery, Class)} to obtain an instance.
 *
 * <h3>Example:</h3>
 * <pre>{@code
 * EntityQuery query = new EntityQuery().setInstanceIdStartsWith("counter");
 * for (TypedEntityMetadata<Integer> entity : client.getEntities().getAllEntities(query, Integer.class)) {
 *     Integer state = entity.getState();
 *     System.out.println("Counter value: " + state);
 * }
 * }</pre>
 *
 * @param <T> the entity state type
 */
public final class TypedEntityQueryPageable<T> implements Iterable<TypedEntityMetadata<T>> {
    private final EntityQueryPageable inner;
    private final Class<T> stateType;

    /**
     * Creates a new {@code TypedEntityQueryPageable}.
     *
     * @param inner     the underlying pageable that fetches raw entity metadata
     * @param stateType the class to deserialize each entity's state into
     */
    TypedEntityQueryPageable(EntityQueryPageable inner, Class<T> stateType) {
        this.inner = inner;
        this.stateType = stateType;
    }

    /**
     * Returns an iterator over individual {@link TypedEntityMetadata} items with eagerly
     * deserialized state, automatically fetching subsequent pages as needed.
     *
     * @return an iterator over all matching entities with typed state
     */
    @Override
    public Iterator<TypedEntityMetadata<T>> iterator() {
        return new TypedEntityItemIterator(inner.iterator());
    }

    private class TypedEntityItemIterator implements Iterator<TypedEntityMetadata<T>> {
        private final Iterator<EntityMetadata> delegate;

        TypedEntityItemIterator(Iterator<EntityMetadata> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public TypedEntityMetadata<T> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return new TypedEntityMetadata<>(delegate.next(), stateType);
        }
    }
}
