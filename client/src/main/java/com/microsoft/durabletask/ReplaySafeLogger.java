// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.spi.LoggingEventBuilder;
import org.slf4j.spi.NOPLoggingEventBuilder;

/**
 * An SLF4J {@link Logger} wrapper that suppresses log output during orchestration replay.
 *
 * <p>Traditional logging methods ({@code info}, {@code debug}, etc.) and the SLF4J 2.x fluent API
 * ({@code atInfo()}, {@code atDebug()}, etc.) are both gated on
 * {@link TaskOrchestrationContext#getIsReplaying()}. The {@code isXxxEnabled()} family of methods
 * always passes through to the underlying logger unchanged.
 *
 * <p>This mirrors the {@code ReplaySafeLogger} nested class in the modern .NET
 * {@code TaskOrchestrationContext}.
 */
final class ReplaySafeLogger implements Logger {

    private final TaskOrchestrationContext context;
    private final Logger inner;

    ReplaySafeLogger(TaskOrchestrationContext context, Logger inner) {
        Helpers.throwIfArgumentNull(context, "context");
        Helpers.throwIfArgumentNull(inner, "inner");
        this.context = context;
        this.inner = inner;
    }

    // -----------------------------------------------------------------------
    // getName — always pass through
    // -----------------------------------------------------------------------

    @Override
    public String getName() {
        return inner.getName();
    }

    // -----------------------------------------------------------------------
    // isXxxEnabled — always pass through (matches .NET IsEnabled behavior)
    // -----------------------------------------------------------------------

    @Override public boolean isTraceEnabled()               { return inner.isTraceEnabled(); }
    @Override public boolean isTraceEnabled(Marker marker)  { return inner.isTraceEnabled(marker); }
    @Override public boolean isDebugEnabled()               { return inner.isDebugEnabled(); }
    @Override public boolean isDebugEnabled(Marker marker)  { return inner.isDebugEnabled(marker); }
    @Override public boolean isInfoEnabled()                { return inner.isInfoEnabled(); }
    @Override public boolean isInfoEnabled(Marker marker)   { return inner.isInfoEnabled(marker); }
    @Override public boolean isWarnEnabled()                { return inner.isWarnEnabled(); }
    @Override public boolean isWarnEnabled(Marker marker)   { return inner.isWarnEnabled(marker); }
    @Override public boolean isErrorEnabled()               { return inner.isErrorEnabled(); }
    @Override public boolean isErrorEnabled(Marker marker)  { return inner.isErrorEnabled(marker); }

    // -----------------------------------------------------------------------
    // SLF4J 2.x fluent API — gate via makeLoggingEventBuilder
    //
    // atInfo(), atDebug(), atWarn(), atError(), atTrace(), atLevel() are
    // default methods on Logger that all delegate to makeLoggingEventBuilder().
    // They bypass the traditional info/debug/warn/error/trace methods
    // entirely, so we MUST override this single entry point to prevent
    // fluent-API calls from escaping replay safety.
    // -----------------------------------------------------------------------

    @Override
    public LoggingEventBuilder makeLoggingEventBuilder(Level level) {
        if (context.getIsReplaying()) {
            return NOPLoggingEventBuilder.singleton();
        }
        return inner.makeLoggingEventBuilder(level);
    }

    // -----------------------------------------------------------------------
    // trace — gated on !isReplaying
    // -----------------------------------------------------------------------

    @Override public void trace(String msg)                                                   { if (!context.getIsReplaying()) inner.trace(msg); }
    @Override public void trace(String format, Object arg)                                    { if (!context.getIsReplaying()) inner.trace(format, arg); }
    @Override public void trace(String format, Object arg1, Object arg2)                      { if (!context.getIsReplaying()) inner.trace(format, arg1, arg2); }
    @Override public void trace(String format, Object... arguments)                           { if (!context.getIsReplaying()) inner.trace(format, arguments); }
    @Override public void trace(String msg, Throwable t)                                      { if (!context.getIsReplaying()) inner.trace(msg, t); }
    @Override public void trace(Marker marker, String msg)                                    { if (!context.getIsReplaying()) inner.trace(marker, msg); }
    @Override public void trace(Marker marker, String format, Object arg)                     { if (!context.getIsReplaying()) inner.trace(marker, format, arg); }
    @Override public void trace(Marker marker, String format, Object arg1, Object arg2)       { if (!context.getIsReplaying()) inner.trace(marker, format, arg1, arg2); }
    @Override public void trace(Marker marker, String format, Object... argArray)             { if (!context.getIsReplaying()) inner.trace(marker, format, argArray); }
    @Override public void trace(Marker marker, String msg, Throwable t)                       { if (!context.getIsReplaying()) inner.trace(marker, msg, t); }

    // -----------------------------------------------------------------------
    // debug — gated on !isReplaying
    // -----------------------------------------------------------------------

    @Override public void debug(String msg)                                                   { if (!context.getIsReplaying()) inner.debug(msg); }
    @Override public void debug(String format, Object arg)                                    { if (!context.getIsReplaying()) inner.debug(format, arg); }
    @Override public void debug(String format, Object arg1, Object arg2)                      { if (!context.getIsReplaying()) inner.debug(format, arg1, arg2); }
    @Override public void debug(String format, Object... arguments)                           { if (!context.getIsReplaying()) inner.debug(format, arguments); }
    @Override public void debug(String msg, Throwable t)                                      { if (!context.getIsReplaying()) inner.debug(msg, t); }
    @Override public void debug(Marker marker, String msg)                                    { if (!context.getIsReplaying()) inner.debug(marker, msg); }
    @Override public void debug(Marker marker, String format, Object arg)                     { if (!context.getIsReplaying()) inner.debug(marker, format, arg); }
    @Override public void debug(Marker marker, String format, Object arg1, Object arg2)       { if (!context.getIsReplaying()) inner.debug(marker, format, arg1, arg2); }
    @Override public void debug(Marker marker, String format, Object... arguments)            { if (!context.getIsReplaying()) inner.debug(marker, format, arguments); }
    @Override public void debug(Marker marker, String msg, Throwable t)                       { if (!context.getIsReplaying()) inner.debug(marker, msg, t); }

    // -----------------------------------------------------------------------
    // info — gated on !isReplaying
    // -----------------------------------------------------------------------

    @Override public void info(String msg)                                                    { if (!context.getIsReplaying()) inner.info(msg); }
    @Override public void info(String format, Object arg)                                     { if (!context.getIsReplaying()) inner.info(format, arg); }
    @Override public void info(String format, Object arg1, Object arg2)                       { if (!context.getIsReplaying()) inner.info(format, arg1, arg2); }
    @Override public void info(String format, Object... arguments)                            { if (!context.getIsReplaying()) inner.info(format, arguments); }
    @Override public void info(String msg, Throwable t)                                       { if (!context.getIsReplaying()) inner.info(msg, t); }
    @Override public void info(Marker marker, String msg)                                     { if (!context.getIsReplaying()) inner.info(marker, msg); }
    @Override public void info(Marker marker, String format, Object arg)                      { if (!context.getIsReplaying()) inner.info(marker, format, arg); }
    @Override public void info(Marker marker, String format, Object arg1, Object arg2)        { if (!context.getIsReplaying()) inner.info(marker, format, arg1, arg2); }
    @Override public void info(Marker marker, String format, Object... arguments)             { if (!context.getIsReplaying()) inner.info(marker, format, arguments); }
    @Override public void info(Marker marker, String msg, Throwable t)                        { if (!context.getIsReplaying()) inner.info(marker, msg, t); }

    // -----------------------------------------------------------------------
    // warn — gated on !isReplaying
    // -----------------------------------------------------------------------

    @Override public void warn(String msg)                                                    { if (!context.getIsReplaying()) inner.warn(msg); }
    @Override public void warn(String format, Object arg)                                     { if (!context.getIsReplaying()) inner.warn(format, arg); }
    @Override public void warn(String format, Object arg1, Object arg2)                       { if (!context.getIsReplaying()) inner.warn(format, arg1, arg2); }
    @Override public void warn(String format, Object... arguments)                            { if (!context.getIsReplaying()) inner.warn(format, arguments); }
    @Override public void warn(String msg, Throwable t)                                       { if (!context.getIsReplaying()) inner.warn(msg, t); }
    @Override public void warn(Marker marker, String msg)                                     { if (!context.getIsReplaying()) inner.warn(marker, msg); }
    @Override public void warn(Marker marker, String format, Object arg)                      { if (!context.getIsReplaying()) inner.warn(marker, format, arg); }
    @Override public void warn(Marker marker, String format, Object arg1, Object arg2)        { if (!context.getIsReplaying()) inner.warn(marker, format, arg1, arg2); }
    @Override public void warn(Marker marker, String format, Object... arguments)             { if (!context.getIsReplaying()) inner.warn(marker, format, arguments); }
    @Override public void warn(Marker marker, String msg, Throwable t)                        { if (!context.getIsReplaying()) inner.warn(marker, msg, t); }

    // -----------------------------------------------------------------------
    // error — gated on !isReplaying
    // -----------------------------------------------------------------------

    @Override public void error(String msg)                                                   { if (!context.getIsReplaying()) inner.error(msg); }
    @Override public void error(String format, Object arg)                                    { if (!context.getIsReplaying()) inner.error(format, arg); }
    @Override public void error(String format, Object arg1, Object arg2)                      { if (!context.getIsReplaying()) inner.error(format, arg1, arg2); }
    @Override public void error(String format, Object... arguments)                           { if (!context.getIsReplaying()) inner.error(format, arguments); }
    @Override public void error(String msg, Throwable t)                                      { if (!context.getIsReplaying()) inner.error(msg, t); }
    @Override public void error(Marker marker, String msg)                                    { if (!context.getIsReplaying()) inner.error(marker, msg); }
    @Override public void error(Marker marker, String format, Object arg)                     { if (!context.getIsReplaying()) inner.error(marker, format, arg); }
    @Override public void error(Marker marker, String format, Object arg1, Object arg2)       { if (!context.getIsReplaying()) inner.error(marker, format, arg1, arg2); }
    @Override public void error(Marker marker, String format, Object... arguments)            { if (!context.getIsReplaying()) inner.error(marker, format, arguments); }
    @Override public void error(Marker marker, String msg, Throwable t)                       { if (!context.getIsReplaying()) inner.error(marker, msg, t); }
}
