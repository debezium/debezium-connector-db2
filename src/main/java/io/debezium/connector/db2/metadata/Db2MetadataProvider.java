/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2.metadata;

import java.util.List;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all Db2 connector metadata.
 */
public class Db2MetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(new Db2ConnectorMetadata());
    }
}
