// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * Sample demonstrating bank account entities with an atomic transfer orchestration.
 * <p>
 * This sample shows two key features:
 * <ul>
 *   <li><b>Entity operations</b>: deposit, withdraw, and get on a bank account entity</li>
 *   <li><b>Entity locking</b>: using {@code lockEntities} for atomic multi-entity operations</li>
 * </ul>
 * <p>
 * The transfer orchestration locks both the source and destination accounts, then performs
 * a withdraw and deposit atomically. This prevents concurrent operations from seeing
 * inconsistent state.
 * <p>
 * Usage: Run this sample with a Durable Task sidecar listening on localhost:4001.
 * <pre>
 *   docker run -d -p 4001:4001 durabletask-sidecar
 *   ./gradlew :samples:run -PmainClass=io.durabletask.samples.BankAccountSample
 * </pre>
 */
final class BankAccountSample {

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        // Build the worker with bank account entity and transfer orchestration
        DurableTaskGrpcWorker worker = new DurableTaskGrpcWorkerBuilder()
                .addEntity("BankAccount", BankAccountEntity::new)
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "SetupAccounts"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            EntityInstanceId accountA = new EntityInstanceId("BankAccount", "account-A");
                            EntityInstanceId accountB = new EntityInstanceId("BankAccount", "account-B");

                            // Initialize both accounts with a deposit
                            ctx.signalEntity(accountA, "deposit", 1000.0);
                            ctx.signalEntity(accountB, "deposit", 500.0);

                            ctx.complete("Accounts initialized");
                        };
                    }
                })
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "TransferFunds"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            TransferRequest request = ctx.getInput(TransferRequest.class);
                            EntityInstanceId source = new EntityInstanceId("BankAccount", request.sourceAccount);
                            EntityInstanceId dest = new EntityInstanceId("BankAccount", request.destAccount);

                            // Lock both accounts to ensure atomic transfer
                            try (AutoCloseable lock = ctx.lockEntities(Arrays.asList(source, dest)).await()) {
                                // Withdraw from source
                                double sourceBalance = ctx.callEntity(
                                        source, "withdraw", request.amount, Double.class).await();

                                // Deposit to destination
                                double destBalance = ctx.callEntity(
                                        dest, "deposit", request.amount, Double.class).await();

                                String result = String.format(
                                        "Transferred %.2f from %s (balance: %.2f) to %s (balance: %.2f)",
                                        request.amount, request.sourceAccount, sourceBalance,
                                        request.destAccount, destBalance);
                                ctx.complete(result);
                            } catch (Exception e) {
                                ctx.complete("Transfer failed: " + e.getMessage());
                            }
                        };
                    }
                })
                .build();

        worker.start();
        System.out.println("Worker started. BankAccount entity and TransferFunds orchestration registered.");

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();

        // Step 1: Initialize accounts
        String setupId = client.scheduleNewOrchestrationInstance("SetupAccounts");
        System.out.printf("Setting up accounts (instance: %s)...%n", setupId);
        client.waitForInstanceCompletion(setupId, Duration.ofSeconds(30), true);
        System.out.println("Accounts initialized: A=$1000, B=$500");

        // Step 2: Transfer $250 from account A to account B
        TransferRequest transfer = new TransferRequest("account-A", "account-B", 250.0);
        String transferId = client.scheduleNewOrchestrationInstance(
                "TransferFunds",
                new NewOrchestrationInstanceOptions().setInput(transfer));
        System.out.printf("Transferring funds (instance: %s)...%n", transferId);

        OrchestrationMetadata result = client.waitForInstanceCompletion(
                transferId, Duration.ofSeconds(30), true);
        System.out.printf("Transfer result: %s%n", result.readOutputAs(String.class));

        // Step 3: Query final account balances
        EntityMetadata accountA = client.getEntityMetadata(
                new EntityInstanceId("BankAccount", "account-A"), true);
        EntityMetadata accountB = client.getEntityMetadata(
                new EntityInstanceId("BankAccount", "account-B"), true);

        if (accountA != null) {
            System.out.printf("Account A balance: $%.2f%n", accountA.readStateAs(Double.class));
        }
        if (accountB != null) {
            System.out.printf("Account B balance: $%.2f%n", accountB.readStateAs(Double.class));
        }

        worker.stop();
    }

    /**
     * A bank account entity that stores a balance and supports deposit, withdraw, and get operations.
     */
    public static class BankAccountEntity extends TaskEntity<Double> {

        public double deposit(double amount) {
            if (amount <= 0) {
                throw new IllegalArgumentException("Deposit amount must be positive.");
            }
            this.state += amount;
            return this.state;
        }

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

        public double get() {
            return this.state;
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

    /**
     * Represents a request to transfer funds between two accounts.
     */
    public static class TransferRequest {
        public String sourceAccount;
        public String destAccount;
        public double amount;

        // Required for deserialization
        public TransferRequest() {}

        public TransferRequest(String sourceAccount, String destAccount, double amount) {
            this.sourceAccount = sourceAccount;
            this.destAccount = destAccount;
            this.amount = amount;
        }
    }
}
