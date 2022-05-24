// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.models;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class Task<V> {
    final CompletableFuture<V> future;

    public Task(CompletableFuture<V> future) {
        this.future = future;
    }

    public boolean isDone() {
        return this.future.isDone();
    }
    
    public boolean isCancelled() { 
        return this.future.isCancelled();
    }

    public abstract V await() throws TaskFailedException, OrchestratorBlockedEvent;
    public abstract Task<Void> thenRun(Runnable fn);
    public abstract Task<Void> thenAccept(Consumer<? super V> fn);
    public abstract <R> Task<R> thenApply(Function<? super V, ? extends R> fn);
    public abstract <R> Task<R> thenCompose(Function<? super V, ? extends Task<R>> task);
}