/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import io.debezium.util.VersionParser;

/**
 * Information about this module.
 *
 * @author Peter Urbanetz
 */
public final class Module {

    public static String version() {
        return VersionParser.getVersion();
    }

    /**
     * @return symbolic name of the connector plugin
     */
    public static String name() {
        return "db2";
    }

    /**
     * @return context name used in log MDC and JMX metrics
     */
    public static String contextName() {
        return "DB2_Server";
    }
}
