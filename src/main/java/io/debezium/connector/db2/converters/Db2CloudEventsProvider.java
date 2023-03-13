/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2.converters;

import io.debezium.connector.db2.transforms.Db2AbstractRecordParserProvider;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for Db2.
 *
 * @author Chris Cranford
 */
public class Db2CloudEventsProvider extends Db2AbstractRecordParserProvider implements CloudEventsProvider {
    @Override
    public CloudEventsMaker createMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        return new Db2CloudEventsMaker(parser, contentType, dataSchemaUriBase);
    }
}
