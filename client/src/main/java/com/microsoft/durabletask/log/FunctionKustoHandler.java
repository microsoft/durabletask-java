package com.microsoft.durabletask.log;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

class FunctionKustoHandler extends Handler {
    private static final String FUNCTIONSKUSTOPREFIX = "LanguageWorkerConsoleLog";
    private static final String DURABLEPREFIX = "DURABLE_JAVA_SDK";
    @Override
    public void publish(LogRecord record) {
        if (record != null && record.getLevel() != null) {
            PrintStream output = record.getLevel().intValue() <= Level.INFO.intValue() ? System.out : System.err;
            output.printf("%s%s [%s] {%s.%s}: %s%n",
                    FUNCTIONSKUSTOPREFIX,
                    DURABLEPREFIX,
                    record.getLevel(),
                    record.getSourceClassName(),
                    record.getSourceMethodName(),
                    record.getMessage());
            if (record.getThrown() != null) {
                output.printf("%s%s%s%n", FUNCTIONSKUSTOPREFIX, DURABLEPREFIX, Arrays.toString(record.getThrown().getStackTrace()));
            }
        }
    }

    @Override
    public void flush() {
        System.out.flush();
        System.err.flush();
    }

    @Override
    public void close() throws SecurityException { }
}
