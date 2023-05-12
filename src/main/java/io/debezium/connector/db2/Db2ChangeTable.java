/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import io.debezium.relational.ChangeTable;
import io.debezium.relational.TableId;

/**
 * A logical representation of change table containing changes for a given source table.
 * There is usually one change table for each source table. When the schema of the source table
 * is changed then two change tables could be present.
 *
 * @author Jiri Pechanec, Peter Urbanetz, Luis Garcés-Erice
 *
 */
public class Db2ChangeTable extends ChangeTable {

    private final String CDC_SCHEMA;

    /**
     * A LSN from which the data in the change table are relevant
     */
    private final Lsn startLsn;

    /**
     * A LSN to which the data in the change table are relevant
     */
    private Lsn stopLsn;

    /**
     * The table in the CDC schema that captures changes, suitably quoted for Db2
     */
    private final String db2CaptureInstance;

    public Db2ChangeTable(TableId sourceTableId, String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn, String tableCdcSchema) {
        super(captureInstance, sourceTableId, resolveChangeTableId(sourceTableId, captureInstance, tableCdcSchema), changeTableObjectId);
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
        this.db2CaptureInstance = Db2ObjectNameQuoter.quoteNameIfNecessary(captureInstance);
        this.CDC_SCHEMA = tableCdcSchema;
    }

    public Db2ChangeTable(String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn, String tableCdcSchema) {
        this(null, captureInstance, changeTableObjectId, startLsn, stopLsn, tableCdcSchema);
    }

    public String getCaptureInstance() {
        return db2CaptureInstance;
    }

    public Lsn getStartLsn() {
        return startLsn;
    }

    public Lsn getStopLsn() {
        return stopLsn;
    }

    public void setStopLsn(Lsn stopLsn) {
        this.stopLsn = stopLsn;
    }

    @Override
    public String toString() {
        return "Capture instance \"" + getCaptureInstance() + "\" [sourceTableId=" + getSourceTableId()
                + ", changeTableId=" + getChangeTableId() + ", startLsn=" + startLsn + ", changeTableObjectId="
                + getChangeTableObjectId() + ", stopLsn=" + stopLsn + "]";
    }

    private static TableId resolveChangeTableId(TableId sourceTableId, String captureInstance, String cdcSchema) {
        return sourceTableId != null ? new TableId(sourceTableId.catalog(), cdcSchema, Db2ObjectNameQuoter.quoteNameIfNecessary(captureInstance)) : null;
    }
}
