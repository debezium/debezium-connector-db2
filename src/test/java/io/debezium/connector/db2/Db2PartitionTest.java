/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import io.debezium.connector.common.AbstractPartitionTest;

public class Db2PartitionTest extends AbstractPartitionTest<Db2Partition> {

    @Override
    protected Db2Partition createPartition1() {
        return new Db2Partition("server1");
    }

    @Override
    protected Db2Partition createPartition2() {
        return new Db2Partition("server2");
    }
}
