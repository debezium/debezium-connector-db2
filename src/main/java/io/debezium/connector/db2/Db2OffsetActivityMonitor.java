/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset change position, the combination of the commit and change log sequence numbers,
 * is compared against the value captured when the monitor was last consulted, and when the
 * position has not moved, a warning is logged. The combination is used rather than the commit
 * log sequence number alone so that progress within a single large transaction is not
 * reported as stale.
 *
 * @author Chris Cranford
 */
public class Db2OffsetActivityMonitor implements OffsetActivityMonitor<Db2Partition, Db2OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Db2OffsetActivityMonitor.class);

    private final Duration checkInterval;

    private TxLogPosition previousPosition;

    public Db2OffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public void checkForStaleOffsets(Db2Partition partition, Db2OffsetContext offsetContext) {
        final TxLogPosition position = offsetContext.getChangePosition();

        // Check for stale state
        if (Objects.equals(previousPosition, position)) {
            LOGGER.warn("Offset position {} has not changed in at least {} milliseconds. " +
                    "This may indicate the database is idle, there are no changes for the captured tables, " +
                    "there are long running transaction(s) delaying the delivery of change events, " +
                    "or that the capture agent is not writing changes to the change tables.",
                    position, checkInterval.toMillis());
        }

        // Update tracked stats
        previousPosition = position;
    }

}