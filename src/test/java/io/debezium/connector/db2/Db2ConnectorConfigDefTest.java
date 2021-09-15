/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import io.debezium.config.ConfigDefinitionMetadataTest;

public class Db2ConnectorConfigDefTest extends ConfigDefinitionMetadataTest {

    public Db2ConnectorConfigDefTest() {
        super(new Db2Connector());
    }
}
