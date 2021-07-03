/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2.converters;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * CloudEvents maker for records produced by the Db2 connector.
 *
 * @author Chris Cranford
 */
public class Db2CloudEventsMaker extends CloudEventsMaker {

    public Db2CloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        super(parser, contentType, dataSchemaUriBase);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";change_lsn:" + recordParser.getMetadata(Db2RecordParser.CHANGE_LSN_KEY)
                + ";commit_lsn:" + recordParser.getMetadata(Db2RecordParser.COMMIT_LSN_KEY);
    }
}
