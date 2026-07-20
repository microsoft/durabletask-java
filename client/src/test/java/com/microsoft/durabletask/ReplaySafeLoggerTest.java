// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ListResourceBundle;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Filter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ReplaySafeLoggerTest {
    private static final ResourceBundle TEST_BUNDLE = new ListResourceBundle() {
        @Override
        protected Object[][] getContents() {
            return new Object[][]{{"message", "localized message"}};
        }

        @Override
        public String getBaseBundleName() {
            return "test.bundle";
        }
    };

    @Test
    void eagerMethodFamilies_areSuppressedDuringReplayAndEmittedWhenLive() {
        AtomicBoolean replaying = new AtomicBoolean(true);
        RecordingHandler handler = new RecordingHandler();
        Logger logger = newReplaySafeLogger(replaying, handler);
        IllegalStateException failure = new IllegalStateException("failure");
        List<Consumer<Logger>> logCalls = Arrays.asList(
                value -> value.severe("severe"),
                value -> value.warning("warning"),
                value -> value.info("info"),
                value -> value.config("config"),
                value -> value.fine("fine"),
                value -> value.finer("finer"),
                value -> value.finest("finest"),
                value -> value.log(Level.INFO, "log"),
                value -> value.log(Level.INFO, "log {0}", "argument"),
                value -> value.log(Level.INFO, "log {0}", new Object[]{"argument"}),
                value -> value.log(Level.INFO, "log", failure),
                value -> value.logp(Level.INFO, "Source", "method", "logp"),
                value -> value.logp(Level.INFO, "Source", "method", "logp {0}", "argument"),
                value -> value.logp(Level.INFO, "Source", "method", "logp {0}", new Object[]{"argument"}),
                value -> value.logp(Level.INFO, "Source", "method", "logp", failure),
                value -> value.logrb(Level.INFO, "Source", "method", (String) null, "logrb"),
                value -> value.logrb(Level.INFO, "Source", "method", (String) null, "logrb {0}", "argument"),
                value -> value.logrb(
                    Level.INFO,
                    "Source",
                    "method",
                    (String) null,
                    "logrb {0}",
                    new Object[]{"argument"}),
                value -> value.logrb(Level.INFO, "Source", "method", (String) null, "logrb", failure),
                value -> value.logrb(Level.INFO, "Source", "method", TEST_BUNDLE, "message", "argument"),
                value -> value.logrb(Level.INFO, TEST_BUNDLE, "message", "argument"),
                value -> value.logrb(Level.INFO, "Source", "method", TEST_BUNDLE, "message", failure),
                value -> value.logrb(Level.INFO, TEST_BUNDLE, "message", failure),
                value -> value.entering("Source", "method"),
                value -> value.entering("Source", "method", "argument"),
                value -> value.entering("Source", "method", new Object[]{"argument"}),
                value -> value.exiting("Source", "method"),
                value -> value.exiting("Source", "method", "result"),
                value -> value.throwing("Source", "method", failure));

        logCalls.forEach(call -> call.accept(logger));
        assertTrue(handler.records.isEmpty());

        replaying.set(false);
        logCalls.forEach(call -> call.accept(logger));
        assertEquals(logCalls.size(), handler.records.size());
    }

    @Test
    void supplierMethods_doNotEvaluateDuringReplayAndEvaluateOnceWhenLive() {
        AtomicBoolean replaying = new AtomicBoolean(true);
        AtomicInteger evaluations = new AtomicInteger();
        RecordingHandler handler = new RecordingHandler();
        Logger logger = newReplaySafeLogger(replaying, handler);
        IllegalStateException failure = new IllegalStateException("failure");
        Supplier<String> supplier = () -> {
            evaluations.incrementAndGet();
            return "message";
        };
        List<Consumer<Logger>> logCalls = Arrays.asList(
                value -> value.log(Level.INFO, supplier),
                value -> value.log(Level.INFO, failure, supplier),
                value -> value.logp(Level.INFO, "Source", "method", supplier),
                value -> value.logp(Level.INFO, "Source", "method", failure, supplier),
                value -> value.info(supplier));

        logCalls.forEach(call -> call.accept(logger));
        assertEquals(0, evaluations.get());
        assertTrue(handler.records.isEmpty());

        replaying.set(false);
        logCalls.forEach(call -> call.accept(logger));
        assertEquals(logCalls.size(), evaluations.get());
        assertEquals(logCalls.size(), handler.records.size());
    }

    @Test
    void loggerTracksReplayStateTransitions() {
        AtomicBoolean replaying = new AtomicBoolean(true);
        RecordingHandler handler = new RecordingHandler();
        Logger logger = newReplaySafeLogger(replaying, handler);

        logger.info("replay");
        replaying.set(false);
        logger.info("live");
        replaying.set(true);
        logger.info("replay again");

        assertEquals(1, handler.records.size());
        assertEquals("live", handler.records.get(0).getMessage());
    }

    @Test
    void isLoggableAlwaysDelegates() {
        AtomicBoolean replaying = new AtomicBoolean(true);
        TestLogger delegate = new TestLogger("delegate");
        delegate.setLevel(Level.WARNING);
        Logger logger = new ReplaySafeLogger(delegate, replaying::get);

        assertFalse(logger.isLoggable(Level.INFO));
        assertTrue(logger.isLoggable(Level.WARNING));

        replaying.set(false);
        assertFalse(logger.isLoggable(Level.INFO));
        assertTrue(logger.isLoggable(Level.WARNING));
    }

    @Test
    void liveRecordsUseDelegateFilterAndHandler() {
        AtomicBoolean replaying = new AtomicBoolean(true);
        AtomicBoolean accepted = new AtomicBoolean(false);
        AtomicInteger filterCalls = new AtomicInteger();
        RecordingHandler handler = new RecordingHandler();
        TestLogger delegate = configuredLogger(handler);
        delegate.setFilter(record -> {
            filterCalls.incrementAndGet();
            return accepted.get();
        });
        Logger logger = new ReplaySafeLogger(delegate, replaying::get);

        logger.info("replay");
        assertEquals(0, filterCalls.get());

        replaying.set(false);
        logger.info("filtered");
        assertEquals(1, filterCalls.get());
        assertTrue(handler.records.isEmpty());

        accepted.set(true);
        logger.info("accepted");
        assertEquals(2, filterCalls.get());
        assertEquals(1, handler.records.size());
        assertEquals("accepted", handler.records.get(0).getMessage());
    }

    @Test
    void explicitLogRecordMetadataIsPreserved() {
        RecordingHandler handler = new RecordingHandler();
        Logger logger = new ReplaySafeLogger(configuredLogger(handler), () -> false);
        IllegalStateException failure = new IllegalStateException("failure");
        LogRecord record = new LogRecord(Level.WARNING, "message");
        record.setLoggerName("category");
        record.setParameters(new Object[]{"argument"});
        record.setThrown(failure);
        record.setSourceClassName("CustomerOrchestrator");
        record.setSourceMethodName("run");
        record.setResourceBundle(TEST_BUNDLE);
        record.setResourceBundleName(TEST_BUNDLE.getBaseBundleName());

        logger.log(record);

        assertEquals(1, handler.records.size());
        LogRecord actual = handler.records.get(0);
        assertSame(record, actual);
        assertEquals(Level.WARNING, actual.getLevel());
        assertEquals("message", actual.getMessage());
        assertEquals("category", actual.getLoggerName());
        assertArrayEquals(new Object[]{"argument"}, actual.getParameters());
        assertSame(failure, actual.getThrown());
        assertEquals("CustomerOrchestrator", actual.getSourceClassName());
        assertEquals("run", actual.getSourceMethodName());
        assertSame(TEST_BUNDLE, actual.getResourceBundle());
        assertEquals(TEST_BUNDLE.getBaseBundleName(), actual.getResourceBundleName());
    }

    @Test
    void explicitSourceAndDirectResourceBundleArePreserved() {
        RecordingHandler handler = new RecordingHandler();
        TestLogger delegate = configuredLogger(handler);
        delegate.setResourceBundle(TEST_BUNDLE);
        Logger logger = new ReplaySafeLogger(delegate, () -> false);

        logger.logp(Level.INFO, "CustomerOrchestrator", "run", "message");

        assertEquals(1, handler.records.size());
        LogRecord record = handler.records.get(0);
        assertEquals("CustomerOrchestrator", record.getSourceClassName());
        assertEquals("run", record.getSourceMethodName());
        assertSame(TEST_BUNDLE, record.getResourceBundle());
        assertEquals(TEST_BUNDLE.getBaseBundleName(), record.getResourceBundleName());
    }

    @Test
    void parentInheritedResourceBundleIsPreserved() {
        RecordingHandler handler = new RecordingHandler();
        TestLogger parent = new TestLogger("parent");
        parent.setResourceBundle(TEST_BUNDLE);
        TestLogger delegate = configuredLogger(handler);
        delegate.setParent(parent);
        Logger logger = new ReplaySafeLogger(delegate, () -> false);

        logger.info("message");

        assertEquals(1, handler.records.size());
        LogRecord record = handler.records.get(0);
        assertSame(TEST_BUNDLE, record.getResourceBundle());
        assertEquals(TEST_BUNDLE.getBaseBundleName(), record.getResourceBundleName());
    }

    @Test
    void configurationMethodsOperateOnDelegate() {
        TestLogger delegate = new TestLogger("delegate");
        TestLogger parent = new TestLogger("parent");
        Logger logger = new ReplaySafeLogger(delegate, () -> false);
        Filter filter = record -> true;
        RecordingHandler handler = new RecordingHandler();

        logger.setResourceBundle(TEST_BUNDLE);
        logger.setFilter(filter);
        logger.setLevel(Level.FINE);
        logger.addHandler(handler);
        logger.setParent(parent);
        logger.setUseParentHandlers(false);

        assertEquals("delegate", logger.getName());
        assertSame(TEST_BUNDLE, delegate.getResourceBundle());
        assertSame(TEST_BUNDLE, logger.getResourceBundle());
        assertEquals(TEST_BUNDLE.getBaseBundleName(), logger.getResourceBundleName());
        assertSame(filter, delegate.getFilter());
        assertSame(filter, logger.getFilter());
        assertEquals(Level.FINE, delegate.getLevel());
        assertEquals(Level.FINE, logger.getLevel());
        assertArrayEquals(new Handler[]{handler}, delegate.getHandlers());
        assertArrayEquals(new Handler[]{handler}, logger.getHandlers());
        assertNotSame(logger.getHandlers(), logger.getHandlers());
        assertSame(parent, delegate.getParent());
        assertSame(parent, logger.getParent());
        assertFalse(delegate.getUseParentHandlers());
        assertFalse(logger.getUseParentHandlers());

        logger.removeHandler(handler);
        assertEquals(0, delegate.getHandlers().length);
        assertEquals(0, handler.closeCalls);
    }

    @Test
    void constructorRejectsNullDependencies() {
        NullPointerException delegateException = assertThrows(
                NullPointerException.class,
                () -> new ReplaySafeLogger(null, () -> false));
        NullPointerException replayException = assertThrows(
                NullPointerException.class,
                () -> new ReplaySafeLogger(new TestLogger("delegate"), null));

        assertEquals("delegate", delegateException.getMessage());
        assertEquals("isReplaying", replayException.getMessage());
    }

    @Test
    void allPublicLoggerMethodsAreClassified() {
        Set<String> expectedInheritedEmissionMethods = new HashSet<>(Arrays.asList(
                signature("logrb", Level.class, ResourceBundle.class, String.class, Object[].class),
                signature("logrb", Level.class, ResourceBundle.class, String.class, Throwable.class),
                signature("entering", String.class, String.class),
                signature("entering", String.class, String.class, Object.class),
                signature("entering", String.class, String.class, Object[].class),
                signature("exiting", String.class, String.class),
                signature("exiting", String.class, String.class, Object.class),
                signature("throwing", String.class, String.class, Throwable.class),
                signature("severe", String.class),
                signature("warning", String.class),
                signature("info", String.class),
                signature("config", String.class),
                signature("fine", String.class),
                signature("finer", String.class),
                signature("finest", String.class),
                signature("severe", Supplier.class),
                signature("warning", Supplier.class),
                signature("info", Supplier.class),
                signature("config", Supplier.class),
                signature("fine", Supplier.class),
                signature("finer", Supplier.class),
                signature("finest", Supplier.class)));

        Set<String> actualInheritedMethods = Arrays.stream(Logger.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !Modifier.isFinal(method.getModifiers()))
                .filter(method -> !isOverridden(method))
                .map(ReplaySafeLoggerTest::signature)
                .collect(Collectors.toSet());

        assertEquals(expectedInheritedEmissionMethods, actualInheritedMethods);
    }

    private static Logger newReplaySafeLogger(AtomicBoolean replaying, RecordingHandler handler) {
        return new ReplaySafeLogger(configuredLogger(handler), replaying::get);
    }

    private static TestLogger configuredLogger(RecordingHandler handler) {
        TestLogger logger = new TestLogger("delegate");
        logger.setLevel(Level.ALL);
        logger.setUseParentHandlers(false);
        logger.addHandler(handler);
        return logger;
    }

    private static boolean isOverridden(Method method) {
        try {
            ReplaySafeLogger.class.getDeclaredMethod(method.getName(), method.getParameterTypes());
            return true;
        } catch (NoSuchMethodException ignored) {
            return false;
        }
    }

    private static String signature(Method method) {
        return signature(method.getName(), method.getParameterTypes());
    }

    private static String signature(String name, Class<?>... parameterTypes) {
        return name + Arrays.stream(parameterTypes)
                .map(Class::getName)
                .collect(Collectors.joining(",", "(", ")"));
    }

    private static final class TestLogger extends Logger {
        TestLogger(String name) {
            super(name, null);
        }
    }

    private static final class RecordingHandler extends Handler {
        private final List<LogRecord> records = new ArrayList<>();
        private int closeCalls;

        @Override
        public void publish(LogRecord record) {
            this.records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
            this.closeCalls++;
        }
    }
}