// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableEntityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;
import com.microsoft.durabletask.interruption.ContinueAsNewInterruption;
import com.microsoft.durabletask.interruption.OrchestratorBlockedException;

import java.util.Arrays;
import java.util.Optional;

/**
 * Azure Functions for the BankAccount entity sample.
 * <p>
 * Demonstrates:
 * <ul>
 *   <li><b>Entity locking (critical section)</b>: The {@code TransferFunds} orchestration uses
 *       {@code ctx.lockEntities()} to lock both the source and destination accounts before
 *       performing an atomic transfer.</li>
 *   <li><b>Call entity from orchestration</b>: The transfer orchestration calls entity operations
 *       and reads their return values.</li>
 * </ul>
 * <p>
 * This mirrors the standalone {@code BankAccountSample} adapted for Azure Functions.
 * <p>
 * See {@code bankaccounts.http} for example HTTP requests.
 *
 * <p>APIs:
 * <ul>
 *   <li>Deposit: POST /api/bankaccounts/{id}/deposit/{amount}</li>
 *   <li>Withdraw: POST /api/bankaccounts/{id}/withdraw/{amount}</li>
 *   <li>Get balance: GET /api/bankaccounts/{id}</li>
 *   <li>Transfer: POST /api/bankaccounts/transfer?from={id}&amp;to={id}&amp;amount={amount}</li>
 * </ul>
 */
public class BankAccountFunctions {

    // ─── Entity trigger function ───

    @FunctionName("BankAccount")
    public String bankAccountEntity(
            @DurableEntityTrigger(name = "req") String req) {
        return EntityRunner.loadAndRun(req, BankAccountEntity::new);
    }

    // ─── Orchestration ───

    /**
     * Orchestration that performs an atomic fund transfer between two bank account entities.
     * <p>
     * Uses {@code ctx.lockEntities()} to acquire exclusive locks on both accounts before
     * performing the withdraw and deposit. This ensures no concurrent operations can modify
     * either account during the transfer.
     */
    @FunctionName("TransferFunds")
    public String transferFunds(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        TransferRequest request = ctx.getInput(TransferRequest.class);
        EntityInstanceId source = new EntityInstanceId("BankAccount", request.sourceAccount);
        EntityInstanceId dest = new EntityInstanceId("BankAccount", request.destAccount);

        // Lock both accounts to ensure atomic transfer (critical section)
        try (AutoCloseable lock = ctx.lockEntities(Arrays.asList(source, dest)).await()) {
            // Withdraw from source
            double sourceBalance = ctx.callEntity(
                    source, "withdraw", request.amount, Double.class).await();

            // Deposit to destination
            double destBalance = ctx.callEntity(
                    dest, "deposit", request.amount, Double.class).await();

            return String.format(
                    "Transferred %.2f from %s (balance: %.2f) to %s (balance: %.2f)",
                    request.amount, request.sourceAccount, sourceBalance,
                    request.destAccount, destBalance);
        } catch (OrchestratorBlockedException | ContinueAsNewInterruption e) {
            // These are framework control-flow signals and must never be swallowed
            throw e;
        } catch (Exception e) {
            return "Transfer failed: " + e.getMessage();
        }
    }

    // ─── HTTP API functions ───

    /**
     * POST /api/bankaccounts/{id}/deposit/{amount}
     * <p>
     * Signals the bank account entity to deposit the given amount.
     */
    @FunctionName("BankAccount_Deposit")
    public HttpResponseMessage deposit(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    route = "bankaccounts/{id}/deposit/{amount}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id,
            @BindingName("amount") double amount) {
        EntityInstanceId entityId = new EntityInstanceId("BankAccount", id);
        DurableTaskClient client = durableContext.getClient();
        client.getEntities().signalEntity(entityId, "deposit", amount);
        return request.createResponseBuilder(HttpStatus.ACCEPTED).build();
    }

    /**
     * POST /api/bankaccounts/{id}/withdraw/{amount}
     * <p>
     * Signals the bank account entity to withdraw the given amount.
     */
    @FunctionName("BankAccount_Withdraw")
    public HttpResponseMessage withdraw(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    route = "bankaccounts/{id}/withdraw/{amount}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id,
            @BindingName("amount") double amount) {
        EntityInstanceId entityId = new EntityInstanceId("BankAccount", id);
        DurableTaskClient client = durableContext.getClient();
        client.getEntities().signalEntity(entityId, "withdraw", amount);
        return request.createResponseBuilder(HttpStatus.ACCEPTED).build();
    }

    /**
     * GET /api/bankaccounts/{id}
     * <p>
     * Gets the current balance of the bank account entity.
     */
    @FunctionName("BankAccount_Get")
    public HttpResponseMessage getBalance(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET},
                    route = "bankaccounts/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            @BindingName("id") String id) {
        EntityInstanceId entityId = new EntityInstanceId("BankAccount", id);
        DurableTaskClient client = durableContext.getClient();

        TypedEntityMetadata<Double> entity = client.getEntities().getEntityMetadata(entityId, Double.class);
        if (entity == null) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND).build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(new JacksonDataConverter().serialize(entity))
                .build();
    }

    /**
     * POST /api/bankaccounts/transfer?from={id}&amp;to={id}&amp;amount={amount}
     * <p>
     * Starts a TransferFunds orchestration that atomically transfers funds between two accounts
     * using entity locking.
     */
    @FunctionName("BankAccount_Transfer")
    public HttpResponseMessage transfer(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST},
                    route = "bankaccounts/transfer",
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        String from = trimOrNull(request.getQueryParameters().get("from"));
        String to = trimOrNull(request.getQueryParameters().get("to"));
        String amountStr = trimOrNull(request.getQueryParameters().get("amount"));

        if (from == null || to == null || amountStr == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Query parameters 'from', 'to', and 'amount' are required.")
                    .build();
        }

        double amount;
        try {
            amount = Double.parseDouble(amountStr);
        } catch (NumberFormatException e) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Amount must be a valid number.")
                    .build();
        }

        DurableTaskClient client = durableContext.getClient();
        TransferRequest payload = new TransferRequest(from, to, amount);
        String instanceId = client.scheduleNewOrchestrationInstance("TransferFunds", payload);
        context.getLogger().info("Started TransferFunds orchestration: " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    // ─── Helpers ───

    private static String trimOrNull(String value) {
        if (value == null) return null;
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    // ─── Payload classes ───

    /**
     * Represents a request to transfer funds between two bank accounts.
     */
    public static class TransferRequest {
        public String sourceAccount;
        public String destAccount;
        public double amount;

        public TransferRequest() {
        }

        public TransferRequest(String sourceAccount, String destAccount, double amount) {
            this.sourceAccount = sourceAccount;
            this.destAccount = destAccount;
            this.amount = amount;
        }
    }
}
