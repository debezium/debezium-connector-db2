/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import io.debezium.schema.SchemaFactory;

public class Db2SchemaFactory extends SchemaFactory {

    public Db2SchemaFactory() {
        super();
    }

    private static final Db2SchemaFactory db2SchemaFactoryObject = new Db2SchemaFactory();

    public static Db2SchemaFactory get() {
        return db2SchemaFactoryObject;
    }
}
