// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * Sample demonstrating the typed entity proxy feature.
 * <p>
 * A typed entity proxy lets you define entity operations as a Java interface. Method calls
 * on the proxy are automatically translated to entity operations:
 * <ul>
 *   <li>{@code void} methods → fire-and-forget signals</li>
 *   <li>{@code Task<V>} methods → two-way calls that return a result</li>
 * </ul>
 * <p>
 * This provides a type-safe, IDE-friendly alternative to passing string operation names
 * and achieves parity with the .NET SDK's typed entity proxy feature.
 * <p>
 * Usage: Run this sample with a Durable Task sidecar listening on localhost:4001.
 * <pre>
 *   docker run -d -p 4001:4001 durabletask-sidecar
 *   ./gradlew :samples:run -PmainClass=io.durabletask.samples.TypedEntityProxySample
 * </pre>
 */
final class TypedEntityProxySample {

    // ---- Step 1: Define entity operations as an interface ----

    /**
     * Typed interface for the shopping cart entity.
     * <p>
     * {@code void} methods become fire-and-forget signals.
     * {@code Task<V>} methods become two-way calls that return a result.
     */
    public interface IShoppingCart {
        void addItem(String item);
        void removeItem(String item);
        void clear();
        Task<String[]> getItems();
        Task<Integer> getItemCount();
    }

    // ---- Step 2: Implement the entity as usual ----

    /**
     * Shopping cart entity that stores a list of items.
     */
    public static class ShoppingCartEntity extends AbstractTaskEntity<java.util.List<String>> {

        public void addItem(String item) {
            this.state.add(item);
        }

        public void removeItem(String item) {
            this.state.remove(item);
        }

        public void clear() {
            this.state.clear();
        }

        public String[] getItems() {
            return this.state.toArray(new String[0]);
        }

        public int getItemCount() {
            return this.state.size();
        }

        @Override
        @SuppressWarnings("unchecked")
        protected java.util.List<String> initializeState(TaskEntityOperation operation) {
            return new java.util.ArrayList<>();
        }

        @Override
        @SuppressWarnings("unchecked")
        protected Class<java.util.List<String>> getStateType() {
            return (Class<java.util.List<String>>) (Class<?>) java.util.List.class;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        DurableTaskGrpcWorker worker = new DurableTaskGrpcWorkerBuilder()
                .addEntity("ShoppingCart", ShoppingCartEntity::new)
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "ShoppingWorkflow"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            EntityInstanceId cartId = new EntityInstanceId("ShoppingCart", "cart-1");

                            // ---- Step 3: Create a typed proxy ----
                            IShoppingCart cart = ctx.createEntityProxy(cartId, IShoppingCart.class);

                            // Void methods are fire-and-forget signals — no .await() needed
                            cart.addItem("Laptop");
                            cart.addItem("Mouse");
                            cart.addItem("Keyboard");
                            cart.removeItem("Mouse");

                            // Task<V> methods are two-way calls — use .await() to get the result
                            ctx.createTimer(Duration.ofSeconds(3)).await();
                            int count = cart.getItemCount().await();
                            String[] items = cart.getItems().await();

                            ctx.complete(String.format(
                                    "Cart has %d items: %s",
                                    count,
                                    String.join(", ", items)));
                        };
                    }
                })
                .build();

        worker.start();
        System.out.println("Worker started. ShoppingCart entity registered.");

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();

        String instanceId = client.scheduleNewOrchestrationInstance("ShoppingWorkflow");
        System.out.printf("Started orchestration: %s%n", instanceId);

        OrchestrationMetadata result = client.waitForInstanceCompletion(
                instanceId, Duration.ofSeconds(30), true);
        System.out.printf("Result: %s%n", result.readOutputAs(String.class));

        worker.stop();
    }
}
