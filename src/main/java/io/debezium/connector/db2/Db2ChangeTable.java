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

    /**
     * The LSN at which this capture instance starts to contain relevant changes.
     */
    private final Lsn captureStartLsn;

    /**
     * The stop marker reported for this capture instance by the Db2 ASN register table.
     */
    private final Lsn captureStopLsn;

    /**
     * The latest LSN processed for this capture instance by ASN capture.
     */
    private final Lsn captureSynchpointLsn;

    /**
     * Runtime stop LSN used by Debezium when an older capture instance is superseded by a newer one.
     */
    private Lsn effectiveStopLsn;

    /**
     * The LSN at which Debezium should switch the table schema to this capture instance.
     */
    private Lsn schemaSwitchLsn = Lsn.NULL;

    /**
     * The table in the CDC schema that captures changes, suitably quoted for Db2
     */
    private final String db2CaptureInstance;

    public Db2ChangeTable(TableId sourceTableId, String captureInstance, int changeTableObjectId, Lsn captureStartLsn, Lsn captureStopLsn,
                          Lsn captureSynchpointLsn, String tableCdcSchema) {
        super(captureInstance, sourceTableId, resolveChangeTableId(sourceTableId, captureInstance, tableCdcSchema), changeTableObjectId);
        this.captureStartLsn = captureStartLsn;
        this.captureStopLsn = captureStopLsn;
        this.captureSynchpointLsn = captureSynchpointLsn;
        this.effectiveStopLsn = captureStopLsn;
        this.db2CaptureInstance = Db2ObjectNameQuoter.quoteNameIfNecessary(captureInstance);
    }

    public Db2ChangeTable(String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn, String tableCdcSchema) {
        this(null, captureInstance, changeTableObjectId, startLsn, stopLsn, Lsn.NULL, tableCdcSchema);
    }

    public String getCaptureInstance() {
        return db2CaptureInstance;
    }

    public Lsn getStartLsn() {
        return captureStartLsn;
    }

    public Lsn getCaptureStartLsn() {
        return captureStartLsn;
    }

    public Lsn getCaptureStopLsn() {
        return captureStopLsn;
    }

    public Lsn getCaptureSynchpointLsn() {
        return captureSynchpointLsn;
    }

    public Lsn getStopLsn() {
        return effectiveStopLsn;
    }

    public void setStopLsn(Lsn stopLsn) {
        this.effectiveStopLsn = stopLsn;
    }

    public Lsn getSchemaSwitchLsn() {
        return schemaSwitchLsn;
    }

    public void setSchemaSwitchLsn(Lsn schemaSwitchLsn) {
        this.schemaSwitchLsn = schemaSwitchLsn;
    }

    public boolean hasSchemaSwitchLsn() {
        return schemaSwitchLsn.isAvailable();
    }

    @Override
    public String toString() {
        return "Capture instance \"" + getCaptureInstance() + "\" [sourceTableId=" + getSourceTableId()
                + ", changeTableId=" + getChangeTableId() + ", captureStartLsn=" + captureStartLsn + ", captureStopLsn=" + captureStopLsn
                + ", captureSynchpointLsn=" + captureSynchpointLsn + ", schemaSwitchLsn=" + schemaSwitchLsn + ", changeTableObjectId="
                + getChangeTableObjectId() + ", effectiveStopLsn=" + effectiveStopLsn + "]";
    }

    private static TableId resolveChangeTableId(TableId sourceTableId, String captureInstance, String cdcSchema) {
        return sourceTableId != null ? new TableId(sourceTableId.catalog(), cdcSchema, Db2ObjectNameQuoter.quoteNameIfNecessary(captureInstance)) : null;
    }
}
