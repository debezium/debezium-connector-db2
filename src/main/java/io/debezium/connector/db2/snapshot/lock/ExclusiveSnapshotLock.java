/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

public class ExclusiveSnapshotLock implements SnapshotLock {

    @Override
    public String name() {
        return Db2ConnectorConfig.SnapshotLockingMode.EXCLUSIVE.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, Set<String> tableIds) {

        String tableId = tableIds.iterator().next(); // For Db2 we expect just one table at time.

        return Optional.of("SELECT * FROM " + tableId + " WHERE 0=1 WITH CS");
    }
}
