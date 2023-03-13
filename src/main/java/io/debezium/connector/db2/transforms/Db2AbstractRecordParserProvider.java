/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2.transforms;

import io.debezium.connector.db2.Module;
import io.debezium.connector.db2.converters.Db2RecordParser;
import io.debezium.converters.spi.RecordParser;
import io.debezium.transforms.spi.RecordParserProvider;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public abstract class Db2AbstractRecordParserProvider implements RecordParserProvider {

    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public RecordParser createParser(Schema schema, Struct record) {
        return new Db2RecordParser(schema, record);
    }
}
