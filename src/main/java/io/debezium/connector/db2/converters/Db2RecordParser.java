/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2.converters;

import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.converters.spi.RecordParser;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * Parser for records produced by the Db2 connector.
 *
 * @author Chris Cranford
 */
public class Db2RecordParser extends RecordParser {

    static final String CHANGE_LSN_KEY = "change_lsn";
    static final String COMMIT_LSN_KEY = "commit_lsn";

    static final Set<String> DB2_SOURCE_FIELD = Collect.unmodifiableSet(
            CHANGE_LSN_KEY,
            COMMIT_LSN_KEY);

    public Db2RecordParser(Schema schema, Struct record) {
        super(schema, record, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public Object getMetadata(String name) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (DB2_SOURCE_FIELD.contains(name)) {
            return source().get(name);
        }

        throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from Db2 connector");
    }
}
