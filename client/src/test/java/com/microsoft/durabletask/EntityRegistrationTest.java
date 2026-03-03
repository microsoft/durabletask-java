// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for entity registration overloads on {@link DurableTaskGrpcWorkerBuilder}.
 */
public class EntityRegistrationTest {

    // region Test entity classes

    /**
     * A simple entity with a public no-arg constructor.
     */
    static class TestEntity implements ITaskEntity {
        @Override
        public Object runAsync(TaskEntityOperation operation) {
            return null;
        }
    }

    /**
     * An entity that does NOT have a public no-arg constructor.
     */
    static class NoDefaultConstructorEntity implements ITaskEntity {
        private final String value;

        public NoDefaultConstructorEntity(String value) {
            this.value = value;
        }

        @Override
        public Object runAsync(TaskEntityOperation operation) {
            return null;
        }
    }

    // endregion

    // region addEntity(Class)

    @Test
    void addEntity_class_derivesNameFromSimpleName() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addEntity(TestEntity.class);

        // The name should be derived from the simple class name, lowercased
        assertTrue(builder.entityFactories.containsKey("testentity"));
    }

    @Test
    void addEntity_class_nullClass_throws() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class, () -> {
            builder.addEntity((Class<? extends ITaskEntity>) null);
        });
    }

    @Test
    void addEntity_class_factoryCreatesInstance() throws Exception {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addEntity(TestEntity.class);

        TaskEntityFactory factory = builder.entityFactories.get("testentity");
        assertNotNull(factory);

        ITaskEntity entity = factory.create();
        assertNotNull(entity);
        assertInstanceOf(TestEntity.class, entity);
    }

    @Test
    void addEntity_class_factoryCreatesNewInstanceEachTime() throws Exception {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addEntity(TestEntity.class);

        TaskEntityFactory factory = builder.entityFactories.get("testentity");
        ITaskEntity entity1 = factory.create();
        ITaskEntity entity2 = factory.create();

        assertNotSame(entity1, entity2, "Factory should create a new instance each time");
    }

    // endregion

    // region addEntity(String, Class)

    @Test
    void addEntity_nameAndClass_usesProvidedName() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addEntity("MyCustomName", TestEntity.class);

        assertTrue(builder.entityFactories.containsKey("mycustomname"));
    }

    @Test
    void addEntity_nameAndClass_nullClass_throws() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class, () -> {
            builder.addEntity("name", (Class<? extends ITaskEntity>) null);
        });
    }

    @Test
    void addEntity_nameAndClass_noDefaultConstructor_throwsOnCreate() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addEntity("myEntity", NoDefaultConstructorEntity.class);

        TaskEntityFactory factory = builder.entityFactories.get("myentity");
        assertNotNull(factory);

        // Should throw at creation time, not registration time
        assertThrows(RuntimeException.class, factory::create);
    }

    @Test
    void addEntity_nameAndClass_duplicateName_throws() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        builder.addEntity("dup", TestEntity.class);

        assertThrows(IllegalArgumentException.class, () -> {
            builder.addEntity("dup", TestEntity.class);
        });
    }

    // endregion

    // region addEntity(ITaskEntity) — singleton

    @Test
    void addEntity_singleton_derivesNameFromClass() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        TestEntity instance = new TestEntity();
        builder.addEntity(instance);

        assertTrue(builder.entityFactories.containsKey("testentity"));
    }

    @Test
    void addEntity_singleton_returnsSameInstance() throws Exception {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        TestEntity instance = new TestEntity();
        builder.addEntity(instance);

        TaskEntityFactory factory = builder.entityFactories.get("testentity");
        ITaskEntity created1 = factory.create();
        ITaskEntity created2 = factory.create();

        assertSame(instance, created1, "Singleton registration should return the same instance");
        assertSame(instance, created2, "Singleton registration should return the same instance");
    }

    @Test
    void addEntity_singleton_null_throws() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class, () -> {
            builder.addEntity((ITaskEntity) null);
        });
    }

    // endregion

    // region addEntity(String, ITaskEntity) — named singleton

    @Test
    void addEntity_namedSingleton_usesProvidedName() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        TestEntity instance = new TestEntity();
        builder.addEntity("customName", instance);

        assertTrue(builder.entityFactories.containsKey("customname"));
    }

    @Test
    void addEntity_namedSingleton_returnsSameInstance() throws Exception {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        TestEntity instance = new TestEntity();
        builder.addEntity("myEntity", instance);

        TaskEntityFactory factory = builder.entityFactories.get("myentity");
        assertSame(instance, factory.create());
    }

    @Test
    void addEntity_namedSingleton_null_throws() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class, () -> {
            builder.addEntity("name", (ITaskEntity) null);
        });
    }

    // endregion

    // region Chaining

    @Test
    void addEntity_returnsBuilderForChaining() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        DurableTaskGrpcWorkerBuilder result = builder
                .addEntity(TestEntity.class)
                .addEntity("custom", new TestEntity());

        assertSame(builder, result);
        assertEquals(2, builder.entityFactories.size());
    }

    // endregion
}
