// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Sample demonstrating advanced entity programming models:
 * <ul>
 *   <li><b>{@link ITaskEntity} low-level interface</b>: Implementing the raw entity interface with
 *       manual switch-based operation dispatch instead of reflection-based {@link TaskEntity}.</li>
 *   <li><b>State dispatch</b>: Using {@link TaskEntity} with {@code allowStateDispatch=true}
 *       (default) so operations can be dispatched to methods on the state POJO itself.</li>
 *   <li><b>POJO entity state</b>: Using a rich class as the entity state instead of a primitive.</li>
 *   <li><b>Implicit delete</b>: Sending a "delete" operation to an entity that has no explicit
 *       delete method — {@link TaskEntity} handles this automatically by clearing the state.</li>
 * </ul>
 * <p>
 * Usage: Run this sample with a Durable Task sidecar listening on localhost:4001.
 * <pre>
 *   docker run -d -p 4001:4001 durabletask-sidecar
 *   ./gradlew :samples:run -PmainClass=io.durabletask.samples.LowLevelEntitySample
 * </pre>
 */
final class LowLevelEntitySample {

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        DurableTaskGrpcWorker worker = new DurableTaskGrpcWorkerBuilder()
                // Demo 1: ITaskEntity — manual dispatch
                .addEntity("KeyValue", KeyValueEntity::new)
                // Demo 2: TaskEntity with POJO state and state dispatch
                .addEntity("ShoppingCart", CartEntity::new)
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "LowLevelDemo"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            EntityInstanceId kvId = new EntityInstanceId("KeyValue", "config");

                            // Use the low-level ITaskEntity entity
                            ctx.callEntity(kvId, "set", new KeyValuePair("color", "blue")).await();
                            ctx.callEntity(kvId, "set", new KeyValuePair("size", "large")).await();
                            String color = ctx.callEntity(kvId, "get", "color", String.class).await();

                            ctx.complete("color=" + color);
                        };
                    }
                })
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "StateDispatchDemo"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            EntityInstanceId cartId = new EntityInstanceId("ShoppingCart", "cart-1");

                            // Operations dispatched to methods on the CartState POJO
                            ctx.signalEntity(cartId, "addItem", "Widget");
                            ctx.signalEntity(cartId, "addItem", "Gadget");
                            ctx.signalEntity(cartId, "addItem", "Widget");

                            int count = ctx.callEntity(cartId, "getItemCount", Integer.class).await();
                            ctx.complete("Cart has " + count + " items");
                        };
                    }
                })
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "ImplicitDeleteDemo"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            EntityInstanceId cartId = new EntityInstanceId("ShoppingCart", "cart-delete");

                            // Add an item
                            ctx.signalEntity(cartId, "addItem", "TempItem");

                            // Read state to confirm it exists
                            int count = ctx.callEntity(cartId, "getItemCount", Integer.class).await();

                            // Implicit delete — no explicit "delete" method on CartEntity or CartState,
                            // but TaskEntity handles it automatically by clearing the entity state
                            ctx.callEntity(cartId, "delete").await();

                            ctx.complete("Had " + count + " item(s), then deleted");
                        };
                    }
                })
                .build();

        worker.start();
        System.out.println("Worker started. KeyValue and ShoppingCart entities registered.");

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();

        // --- Demo 1: Low-level ITaskEntity ---
        System.out.println("\n--- Demo 1: ITaskEntity with manual dispatch ---");
        String id1 = client.scheduleNewOrchestrationInstance("LowLevelDemo");
        OrchestrationMetadata result1 = client.waitForInstanceCompletion(
                id1, Duration.ofSeconds(30), true);
        System.out.printf("Result: %s%n", result1.readOutputAs(String.class));

        EntityMetadata kvMeta = client.getEntities().getEntityMetadata(
                new EntityInstanceId("KeyValue", "config"), true);
        if (kvMeta != null) {
            System.out.printf("KeyValue entity state: %s%n", kvMeta.readStateAs(Object.class));
        }

        // --- Demo 2: State dispatch (operations dispatched to CartState methods) ---
        System.out.println("\n--- Demo 2: State dispatch with POJO state ---");
        String id2 = client.scheduleNewOrchestrationInstance("StateDispatchDemo");
        OrchestrationMetadata result2 = client.waitForInstanceCompletion(
                id2, Duration.ofSeconds(30), true);
        System.out.printf("Result: %s%n", result2.readOutputAs(String.class));

        // --- Demo 3: Implicit delete ---
        System.out.println("\n--- Demo 3: Implicit delete ---");
        String id3 = client.scheduleNewOrchestrationInstance("ImplicitDeleteDemo");
        OrchestrationMetadata result3 = client.waitForInstanceCompletion(
                id3, Duration.ofSeconds(30), true);
        System.out.printf("Result: %s%n", result3.readOutputAs(String.class));

        // Verify entity was deleted
        EntityMetadata deletedMeta = client.getEntities().getEntityMetadata(
                new EntityInstanceId("ShoppingCart", "cart-delete"), true);
        System.out.printf("Entity after delete: %s%n", deletedMeta == null ? "null (deleted)" : "still exists");

        worker.stop();
    }

    // ---- Data classes ----

    /**
     * A simple key-value pair used as input for the KeyValue entity.
     */
    public static class KeyValuePair {
        public String key;
        public String value;

        public KeyValuePair() {} // for deserialization
        public KeyValuePair(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    // ---- Low-level ITaskEntity implementation ----

    /**
     * A key-value store entity implemented directly with {@link ITaskEntity}.
     * <p>
     * This demonstrates manual switch-based operation dispatch without the reflection-based
     * {@link TaskEntity} base class. This gives full control over how operations are routed.
     */
    public static class KeyValueEntity implements ITaskEntity {
        @Override
        public Object runAsync(TaskEntityOperation operation) throws Exception {
            // Load current state (a Map stored as JSON)
            @SuppressWarnings("unchecked")
            java.util.Map<String, String> store = operation.getState().getState(java.util.Map.class);
            if (store == null) {
                store = new java.util.HashMap<>();
            }

            Object result = null;

            // Manual switch-based dispatch
            switch (operation.getName().toLowerCase()) {
                case "set":
                    KeyValuePair kvp = operation.getInput(KeyValuePair.class);
                    store.put(kvp.key, kvp.value);
                    break;
                case "get":
                    String key = operation.getInput(String.class);
                    result = store.get(key);
                    break;
                case "remove":
                    String removeKey = operation.getInput(String.class);
                    result = store.remove(removeKey);
                    break;
                case "getall":
                    result = new java.util.HashMap<>(store);
                    break;
                case "delete":
                    operation.getState().deleteState();
                    return null;
                default:
                    throw new UnsupportedOperationException(
                            "KeyValue entity does not support operation: " + operation.getName());
            }

            // Save state back
            operation.getState().setState(store);
            return result;
        }
    }

    // ---- State dispatch entity implementation ----

    /**
     * Shopping cart state POJO whose public methods serve as entity operations
     * via state dispatch.
     * <p>
     * When {@link CartEntity} receives an operation like "addItem", and no matching method
     * exists on {@code CartEntity} itself, the framework dispatches the operation to
     * this state class's {@code addItem} method.
     */
    public static class CartState {
        public java.util.List<String> items = new java.util.ArrayList<>();

        /**
         * Adds an item to the cart. Called via state dispatch.
         */
        public void addItem(String item) {
            items.add(item);
        }

        /**
         * Returns the number of items in the cart. Called via state dispatch.
         */
        public int getItemCount() {
            return items.size();
        }
    }

    /**
     * Cart entity that delegates operations to methods on the {@link CartState} POJO.
     * <p>
     * This entity has no operation methods of its own — all operations (addItem, getItemCount)
     * are dispatched to the state object. This is enabled by default via
     * {@code allowStateDispatch = true}.
     */
    public static class CartEntity extends TaskEntity<CartState> {
        // No operation methods here — they are on CartState

        @Override
        protected CartState initializeState(TaskEntityOperation operation) {
            return new CartState();
        }

        @Override
        protected Class<CartState> getStateType() {
            return CartState.class;
        }
    }
}
