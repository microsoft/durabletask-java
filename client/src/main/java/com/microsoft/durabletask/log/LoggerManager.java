package com.microsoft.durabletask.log;

import java.util.logging.*;

public class LoggerManager {
    private static final Logger logger = initLogger();

    private static Logger initLogger() {
        Logger logger = Logger.getAnonymousLogger();
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.ALL);
        logger.addHandler(new FunctionKustoHandler());
        return logger;
    }

    public static void setHandler(Handler handler) {
        clearHandler();
        addHandler(handler);
    }

    public static void addHandler(Handler handler) {
        logger.addHandler(handler);
    }

    private static void clearHandler() {
        for (Handler handler : logger.getHandlers()) {
            logger.removeHandler(handler);
        }
    }

    public static Logger getLogger() {
        return logger;
    }
}
