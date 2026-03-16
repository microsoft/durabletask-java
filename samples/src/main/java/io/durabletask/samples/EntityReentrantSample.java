// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Sample demonstrating entity re-entrancy via the {@code dispatch()} method.
 * <p>
 * Entity re-entrancy allows one operation to call another operation on the same entity
 * synchronously, reusing the same state and context. This is useful for composing operations
 * from simpler building blocks without duplicating logic.
 * <p>
 * The {@code dispatch()} method resolves operations using the same case-insensitive reflection
 * dispatch as external operations, so any named operation — including state-dispatched methods
 * and implicit "delete" — can be called re-entrantly.
 * <p>
 * Usage: Run this sample with a Durable Task sidecar listening on localhost:4001.
 * <pre>
 *   docker run -d -p 4001:4001 durabletask-sidecar
 *   ./gradlew :samples:run -PmainClass=io.durabletask.samples.EntityReentrantSample
 * </pre>
 */
final class EntityReentrantSample {

    /**
     * A rewards-enabled bank account entity.
     * <p>
     * Demonstrates re-entrancy: the {@code depositWithRewards} operation uses
     * {@code dispatch("deposit", ...)} to compose the main deposit and a bonus
     * deposit into a single atomic operation.
     */
    public static class RewardsAccountEntity extends TaskEntity<Double> {

        /**
         * Simple deposit — adds the amount to the balance.
         */
        public double deposit(double amount) {
            if (amount <= 0) {
                throw new IllegalArgumentException("Deposit amount must be positive.");
            }
            this.state += amount;
            return this.state;
        }

        /**
         * Deposit with a rewards bonus — re-entrantly calls deposit() twice.
         * <p>
         * For every deposit, a 5% bonus is added automatically.
         * Both the main deposit and the bonus are applied atomically within
         * a single entity operation execution.
         */
        public double depositWithRewards(double amount) {
            // Re-entrant: calls deposit() on this same entity instance
            dispatch("deposit", amount);
            // Add 5% bonus
            dispatch("deposit", amount * 0.05);
            return this.state;
        }

        /**
         * Withdraw — subtracts the amount from the balance.
         */
        public double withdraw(double amount) {
            if (amount <= 0) {
                throw new IllegalArgumentException("Withdrawal amount must be positive.");
            }
            if (amount > this.state) {
                throw new IllegalStateException(
                        String.format("Insufficient funds. Balance: %.2f, Requested: %.2f", this.state, amount));
            }
            this.state -= amount;
            return this.state;
        }

        /**
         * Get the current balance.
         */
        public double getBalance() {
            return this.state;
        }

        /**
         * Close the account — uses dispatch("delete") to leverage the implicit
         * delete operation, which clears entity state entirely.
         */
        public String closeAccount() {
            double finalBalance = this.state;
            dispatch("delete"); // re-entrant call to implicit delete
            return String.format("Account closed. Final balance was: %.2f", finalBalance);
        }

        @Override
        protected Double initializeState(TaskEntityOperation operation) {
            return 0.0;
        }

        @Override
        protected Class<Double> getStateType() {
            return Double.class;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        DurableTaskGrpcWorker worker = new DurableTaskGrpcWorkerBuilder()
                .addEntity("RewardsAccount", RewardsAccountEntity::new)
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "RewardsDemo"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            EntityInstanceId accountId = new EntityInstanceId("RewardsAccount", "demo-account");

                            // Deposit $1000 with 5% rewards bonus → $1050
                            ctx.signalEntity(accountId, "depositWithRewards", 1000.0);

                            ctx.createTimer(Duration.ofSeconds(3)).await();

                            // Check balance
                            double balance = ctx.callEntity(accountId, "getBalance", Double.class).await();

                            ctx.complete(String.format(
                                    "After depositing $1000.00 with rewards: balance = $%.2f (includes 5%% bonus)",
                                    balance));
                        };
                    }
                })
                .build();

        worker.start();
        System.out.println("Worker started. RewardsAccount entity registered.");

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();

        String instanceId = client.scheduleNewOrchestrationInstance("RewardsDemo");
        System.out.printf("Started orchestration: %s%n", instanceId);

        OrchestrationMetadata result = client.waitForInstanceCompletion(
                instanceId, Duration.ofSeconds(30), true);
        System.out.printf("Result: %s%n", result.readOutputAs(String.class));

        worker.stop();
    }
}
