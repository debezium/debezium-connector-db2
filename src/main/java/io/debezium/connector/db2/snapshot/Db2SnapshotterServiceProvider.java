/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2.snapshot;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.snapshot.SnapshotterServiceProvider;

public class Db2SnapshotterServiceProvider extends SnapshotterServiceProvider {

    @Override
    public String snapshotMode(BeanRegistry beanRegistry) {

        Db2ConnectorConfig mySqlConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, Db2ConnectorConfig.class);

        return mySqlConnectorConfig.getSnapshotMode().getValue();
    }
}
