/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * A state (context) associated with a DB2 task
 *
 * @author Jiri Pechanec
 *
 */
public class Db2TaskContext extends CdcSourceTaskContext<Db2ConnectorConfig> {

    public Db2TaskContext(Configuration rawConfig, Db2ConnectorConfig config) {
        super(rawConfig, config, config.getCustomMetricTags());
    }
}
