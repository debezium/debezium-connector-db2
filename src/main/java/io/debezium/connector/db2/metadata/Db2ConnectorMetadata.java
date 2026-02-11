/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2.metadata;

import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.connector.db2.Db2Connector;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;

/**
 * @author Chris Cranford
 */
public class Db2ConnectorMetadata implements ComponentMetadata {

    @Override
    public ComponentDescriptor getComponentDescriptor() {
        return new ComponentDescriptor(Db2Connector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getComponentFields() {
        return Db2ConnectorConfig.ALL_FIELDS;
    }
}
