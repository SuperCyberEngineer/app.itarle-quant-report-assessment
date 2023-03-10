package org.apache.beam.examples;

import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

class LoggingConfig {

    public static void configLogger() {
        Logger mainLogger = Logger.getLogger("org.apache.beam.examples");

        mainLogger.setUseParentHandlers(false);

        ConsoleHandler handler = new ConsoleHandler();

        handler.setFormatter(new SimpleFormatter() {

            private static final String format = "[%1$tF %1$tT] [%2$s] [Thread-%3$d] [%4$s] %5$s %6$s %n";

            @Override
            public synchronized String format(LogRecord lr) {
                String msg = String.format(format, new Date(lr.getMillis()), lr.getLevel().getLocalizedName(),
                        lr.getThreadID(), ":", lr.getMessage(),
                        lr.getThrown() == null ? "" : lr.getThrown().getMessage());

                return msg;
            }
        });

        mainLogger.addHandler(handler);
    }
}
