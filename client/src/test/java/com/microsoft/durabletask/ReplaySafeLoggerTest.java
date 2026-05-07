// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.event.Level;
import org.slf4j.spi.LoggingEventBuilder;
import org.slf4j.spi.NOPLoggingEventBuilder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReplaySafeLoggerTest {

    @Mock
    private TaskOrchestrationContext context;

    @Mock
    private Logger inner;

    // -----------------------------------------------------------------------
    // Constructor validation
    // -----------------------------------------------------------------------

    @Test
    void constructor_throwsOnNullContext() {
        assertThrows(IllegalArgumentException.class, () -> new ReplaySafeLogger(null, inner));
    }

    @Test
    void constructor_throwsOnNullLogger() {
        assertThrows(IllegalArgumentException.class, () -> new ReplaySafeLogger(context, null));
    }

    // -----------------------------------------------------------------------
    // getName — always passes through
    // -----------------------------------------------------------------------

    @Test
    void getName_returnsInnerLoggerName() {
        when(inner.getName()).thenReturn("com.example.MyOrchestrator");
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);
        assertEquals("com.example.MyOrchestrator", logger.getName());
    }

    // -----------------------------------------------------------------------
    // isXxxEnabled — always passes through regardless of replay state
    // -----------------------------------------------------------------------

    @Test
    void isInfoEnabled_passesThroughDuringReplay() {
        when(inner.isInfoEnabled()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);
        assertTrue(logger.isInfoEnabled());
    }

    @Test
    void isDebugEnabled_passesThroughWhenNotReplaying() {
        when(inner.isDebugEnabled()).thenReturn(false);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);
        assertFalse(logger.isDebugEnabled());
    }

    @Test
    void isWarnEnabled_withMarker_passesThroughDuringReplay() {
        Marker marker = MarkerFactory.getMarker("TEST");
        when(inner.isWarnEnabled(marker)).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);
        assertTrue(logger.isWarnEnabled(marker));
    }

    @Test
    void isTraceEnabled_passesThroughDuringReplay() {
        when(inner.isTraceEnabled()).thenReturn(false);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);
        assertFalse(logger.isTraceEnabled());
    }

    @Test
    void isErrorEnabled_passesThroughDuringReplay() {
        when(inner.isErrorEnabled()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);
        assertTrue(logger.isErrorEnabled());
    }

    // -----------------------------------------------------------------------
    // Suppresses logs during replay — one representative per level
    // -----------------------------------------------------------------------

    @Test
    void info_suppressedDuringReplay() {
        when(context.getIsReplaying()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.info("test message");
        verify(inner, never()).info(anyString());
    }

    @Test
    void debug_suppressedDuringReplay() {
        when(context.getIsReplaying()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.debug("test message");
        verify(inner, never()).debug(anyString());
    }

    @Test
    void warn_suppressedDuringReplay() {
        when(context.getIsReplaying()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.warn("test message");
        verify(inner, never()).warn(anyString());
    }

    @Test
    void error_suppressedDuringReplay() {
        when(context.getIsReplaying()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.error("test message");
        verify(inner, never()).error(anyString());
    }

    @Test
    void trace_suppressedDuringReplay() {
        when(context.getIsReplaying()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.trace("test message");
        verify(inner, never()).trace(anyString());
    }

    // -----------------------------------------------------------------------
    // Forwards logs when NOT replaying — one representative per level
    // -----------------------------------------------------------------------

    @Test
    void info_forwardedWhenNotReplaying() {
        when(context.getIsReplaying()).thenReturn(false);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.info("hello");
        verify(inner).info("hello");
    }

    @Test
    void debug_forwardedWhenNotReplaying() {
        when(context.getIsReplaying()).thenReturn(false);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.debug("hello");
        verify(inner).debug("hello");
    }

    @Test
    void warn_forwardedWhenNotReplaying() {
        when(context.getIsReplaying()).thenReturn(false);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.warn("hello");
        verify(inner).warn("hello");
    }

    @Test
    void error_forwardedWhenNotReplaying() {
        when(context.getIsReplaying()).thenReturn(false);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.error("hello");
        verify(inner).error("hello");
    }

    @Test
    void trace_forwardedWhenNotReplaying() {
        when(context.getIsReplaying()).thenReturn(false);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.trace("hello");
        verify(inner).trace("hello");
    }

    // -----------------------------------------------------------------------
    // Format-string overloads
    // -----------------------------------------------------------------------

    @Test
    void info_formatOneArg_forwardedWhenNotReplaying() {
        when(context.getIsReplaying()).thenReturn(false);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.info("count={}", 42);
        verify(inner).info("count={}", (Object) 42);
    }

    @Test
    void info_formatTwoArgs_suppressedDuringReplay() {
        when(context.getIsReplaying()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.info("{} + {}", 1, 2);
        verify(inner, never()).info(anyString(), any(), any());
    }

    @Test
    void warn_formatVarargs_forwardedWhenNotReplaying() {
        when(context.getIsReplaying()).thenReturn(false);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.warn("{} {} {}", "a", "b", "c");
        verify(inner).warn("{} {} {}", "a", "b", "c");
    }

    // -----------------------------------------------------------------------
    // Throwable overload
    // -----------------------------------------------------------------------

    @Test
    void error_withThrowable_forwardedWhenNotReplaying() {
        when(context.getIsReplaying()).thenReturn(false);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);
        Exception ex = new RuntimeException("boom");

        logger.error("failed", ex);
        verify(inner).error("failed", ex);
    }

    @Test
    void error_withThrowable_suppressedDuringReplay() {
        when(context.getIsReplaying()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);
        Exception ex = new RuntimeException("boom");

        logger.error("failed", ex);
        verify(inner, never()).error(anyString(), any(Throwable.class));
    }

    // -----------------------------------------------------------------------
    // Marker overloads
    // -----------------------------------------------------------------------

    @Test
    void info_withMarker_forwardedWhenNotReplaying() {
        Marker marker = MarkerFactory.getMarker("IMPORTANT");
        when(context.getIsReplaying()).thenReturn(false);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.info(marker, "hello");
        verify(inner).info(marker, "hello");
    }

    @Test
    void info_withMarker_suppressedDuringReplay() {
        Marker marker = MarkerFactory.getMarker("IMPORTANT");
        when(context.getIsReplaying()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.info(marker, "hello");
        verify(inner, never()).info(any(Marker.class), anyString());
    }

    // -----------------------------------------------------------------------
    // Dynamic replay state changes
    // -----------------------------------------------------------------------

    @Test
    void respectsChangesInReplayState() {
        when(context.getIsReplaying()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        logger.info("during replay");
        verify(inner, never()).info("during replay");

        // Transition to not replaying
        when(context.getIsReplaying()).thenReturn(false);

        logger.info("after replay");
        verify(inner).info("after replay");
    }

    // -----------------------------------------------------------------------
    // SLF4J 2.x fluent API — makeLoggingEventBuilder
    // -----------------------------------------------------------------------

    @Test
    void makeLoggingEventBuilder_returnsNOPDuringReplay() {
        when(context.getIsReplaying()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        LoggingEventBuilder builder = logger.makeLoggingEventBuilder(Level.INFO);
        assertSame(NOPLoggingEventBuilder.singleton(), builder);
        verify(inner, never()).makeLoggingEventBuilder(any());
    }

    @Test
    void makeLoggingEventBuilder_delegatesWhenNotReplaying() {
        when(context.getIsReplaying()).thenReturn(false);
        LoggingEventBuilder mockBuilder = mock(LoggingEventBuilder.class);
        when(inner.makeLoggingEventBuilder(Level.WARN)).thenReturn(mockBuilder);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        LoggingEventBuilder builder = logger.makeLoggingEventBuilder(Level.WARN);
        assertSame(mockBuilder, builder);
    }

    @Test
    void fluentApi_atInfo_suppressedDuringReplay() {
        when(context.getIsReplaying()).thenReturn(true);
        // atInfo() is a default method that calls makeLoggingEventBuilder(INFO)
        // then checks isInfoEnabled(). We need isInfoEnabled to pass through.
        when(inner.isInfoEnabled()).thenReturn(true);
        ReplaySafeLogger logger = new ReplaySafeLogger(context, inner);

        // The returned builder should be a NOP — calling log() does nothing.
        LoggingEventBuilder builder = logger.atInfo();
        assertNotNull(builder);
        // Verify makeLoggingEventBuilder on inner was NOT called
        verify(inner, never()).makeLoggingEventBuilder(any());
    }
}
