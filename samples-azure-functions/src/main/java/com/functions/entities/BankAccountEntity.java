// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions.entities;

import com.microsoft.durabletask.AbstractTaskEntity;
import com.microsoft.durabletask.TaskEntityOperation;

/**
 * A bank account entity that stores a balance and supports deposit, withdraw, and get operations.
 * <p>
 * Used by {@link BankAccountFunctions} to demonstrate entity locking (critical sections)
 * for atomic multi-entity operations such as fund transfers.
 * <p>
 * This mirrors the standalone {@code BankAccountSample.BankAccountEntity}.
 */
public class BankAccountEntity extends AbstractTaskEntity<Double> {

    /**
     * Adds the given amount to the account balance and returns the new balance.
     */
    public double deposit(double amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("Deposit amount must be positive.");
        }
        this.state += amount;
        return this.state;
    }

    /**
     * Withdraws the given amount from the account balance and returns the new balance.
     *
     * @throws IllegalStateException if the account has insufficient funds
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
     * Returns the current account balance.
     */
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
