// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import java.util.Objects;
import java.util.ResourceBundle;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Filter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A {@link Logger} wrapper that suppresses log emission while an orchestration is replaying.
 *
 * <p>The {@code client} module targets Java 8, but a few of the overrides below wrap
 * {@link Logger} methods that were introduced in Java 9:
 * <ul>
 *   <li>{@link #log(Level, Throwable, Supplier)}</li>
 *   <li>{@link #logp(Level, String, String, Throwable, Supplier)}</li>
 *   <li>{@link #logrb(Level, String, String, ResourceBundle, String, Throwable)}</li>
 * </ul>
 * These overrides make the wrapper replay-safe when it runs on a Java 9+ runtime. On a Java 8
 * runtime the class still loads and every Java 8 {@link Logger} method remains replay-safe; the
 * Java 9+ overrides are simply unreachable, because a Java 8-compiled caller cannot resolve those
 * signatures through a {@link Logger}-typed reference and the JDK never routes into them
 * internally.
 */
final class ReplaySafeLogger extends Logger {
    private final Logger delegate;
    private final BooleanSupplier isReplaying;

    ReplaySafeLogger(Logger delegate, BooleanSupplier isReplaying) {
        super(Objects.requireNonNull(delegate, "delegate").getName(), null);
        this.delegate = delegate;
        this.isReplaying = Objects.requireNonNull(isReplaying, "isReplaying");
    }

    @Override
    public boolean isLoggable(Level level) {
        return this.delegate.isLoggable(level);
    }

    @Override
    public void log(LogRecord record) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.log(record);
        }
    }

    @Override
    public void log(Level level, String message) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.log(level, message);
        }
    }

    @Override
    public void log(Level level, Supplier<String> messageSupplier) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.log(level, messageSupplier);
        }
    }

    @Override
    public void log(Level level, String message, Object parameter) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.log(level, message, parameter);
        }
    }

    @Override
    public void log(Level level, String message, Object[] parameters) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.log(level, message, parameters);
        }
    }

    @Override
    public void log(Level level, String message, Throwable thrown) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.log(level, message, thrown);
        }
    }

    @Override
    public void log(Level level, Throwable thrown, Supplier<String> messageSupplier) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.log(level, thrown, messageSupplier);
        }
    }

    @Override
    public void logp(
            Level level,
            String sourceClass,
            String sourceMethod,
            String message) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logp(level, sourceClass, sourceMethod, message);
        }
    }

    @Override
    public void logp(
            Level level,
            String sourceClass,
            String sourceMethod,
            Supplier<String> messageSupplier) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logp(level, sourceClass, sourceMethod, messageSupplier);
        }
    }

    @Override
    public void logp(
            Level level,
            String sourceClass,
            String sourceMethod,
            String message,
            Object parameter) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logp(level, sourceClass, sourceMethod, message, parameter);
        }
    }

    @Override
    public void logp(
            Level level,
            String sourceClass,
            String sourceMethod,
            String message,
            Object[] parameters) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logp(level, sourceClass, sourceMethod, message, parameters);
        }
    }

    @Override
    public void logp(
            Level level,
            String sourceClass,
            String sourceMethod,
            String message,
            Throwable thrown) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logp(level, sourceClass, sourceMethod, message, thrown);
        }
    }

    @Override
    public void logp(
            Level level,
            String sourceClass,
            String sourceMethod,
            Throwable thrown,
            Supplier<String> messageSupplier) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logp(level, sourceClass, sourceMethod, thrown, messageSupplier);
        }
    }

    @Override
    public void logrb(
            Level level,
            String sourceClass,
            String sourceMethod,
            String bundleName,
            String message) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logrb(level, sourceClass, sourceMethod, bundleName, message);
        }
    }

    @Override
    public void logrb(
            Level level,
            String sourceClass,
            String sourceMethod,
            String bundleName,
            String message,
            Object parameter) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logrb(level, sourceClass, sourceMethod, bundleName, message, parameter);
        }
    }

    @Override
    public void logrb(
            Level level,
            String sourceClass,
            String sourceMethod,
            String bundleName,
            String message,
            Object[] parameters) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logrb(level, sourceClass, sourceMethod, bundleName, message, parameters);
        }
    }

    @Override
    public void logrb(
            Level level,
            String sourceClass,
            String sourceMethod,
            ResourceBundle bundle,
            String message,
            Object... parameters) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logrb(level, sourceClass, sourceMethod, bundle, message, parameters);
        }
    }

    @Override
    public void logrb(
            Level level,
            String sourceClass,
            String sourceMethod,
            String bundleName,
            String message,
            Throwable thrown) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logrb(level, sourceClass, sourceMethod, bundleName, message, thrown);
        }
    }

    @Override
    public void logrb(
            Level level,
            String sourceClass,
            String sourceMethod,
            ResourceBundle bundle,
            String message,
            Throwable thrown) {
        if (!this.isReplaying.getAsBoolean()) {
            this.delegate.logrb(level, sourceClass, sourceMethod, bundle, message, thrown);
        }
    }

    @Override
    public String getName() {
        return this.delegate.getName();
    }

    @Override
    public ResourceBundle getResourceBundle() {
        return this.delegate.getResourceBundle();
    }

    @Override
    public String getResourceBundleName() {
        return this.delegate.getResourceBundleName();
    }

    @Override
    public void setResourceBundle(ResourceBundle bundle) {
        this.delegate.setResourceBundle(bundle);
    }

    @Override
    public Filter getFilter() {
        return this.delegate.getFilter();
    }

    @Override
    public void setFilter(Filter filter) {
        this.delegate.setFilter(filter);
    }

    @Override
    public Level getLevel() {
        return this.delegate.getLevel();
    }

    @Override
    public void setLevel(Level level) {
        this.delegate.setLevel(level);
    }

    @Override
    public Handler[] getHandlers() {
        return this.delegate.getHandlers();
    }

    @Override
    public void addHandler(Handler handler) {
        this.delegate.addHandler(handler);
    }

    @Override
    public void removeHandler(Handler handler) {
        this.delegate.removeHandler(handler);
    }

    @Override
    public Logger getParent() {
        return this.delegate.getParent();
    }

    @Override
    public void setParent(Logger parent) {
        this.delegate.setParent(parent);
    }

    @Override
    public boolean getUseParentHandlers() {
        return this.delegate.getUseParentHandlers();
    }

    @Override
    public void setUseParentHandlers(boolean useParentHandlers) {
        this.delegate.setUseParentHandlers(useParentHandlers);
    }
}
