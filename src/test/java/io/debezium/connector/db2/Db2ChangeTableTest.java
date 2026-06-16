/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

public class Db2ChangeTableTest {

    @Test
    public void shouldKeepCaptureLsnRolesSeparate() {
        final Lsn captureStartLsn = Lsn.valueOf("00000000:00000001:0000000000000001");
        final Lsn captureStopLsn = Lsn.valueOf("00000000:00000002:0000000000000002");
        final Lsn captureSynchpointLsn = Lsn.valueOf("00000000:00000003:0000000000000003");
        final Lsn effectiveStopLsn = Lsn.valueOf("00000000:00000004:0000000000000004");
        final Lsn schemaSwitchLsn = Lsn.valueOf("00000000:00000005:0000000000000005");

        final Db2ChangeTable changeTable = new Db2ChangeTable(
                TableId.parse("DB2INST1.TABLEA"),
                "CDC_DB2INST1_TABLEA",
                1,
                captureStartLsn,
                captureStopLsn,
                captureSynchpointLsn,
                "ASNCDC");

        changeTable.setStopLsn(effectiveStopLsn);
        changeTable.setSchemaSwitchLsn(schemaSwitchLsn);

        assertThat(changeTable.getCaptureStartLsn()).isEqualTo(captureStartLsn);
        assertThat(changeTable.getCaptureStopLsn()).isEqualTo(captureStopLsn);
        assertThat(changeTable.getCaptureSynchpointLsn()).isEqualTo(captureSynchpointLsn);
        assertThat(changeTable.getStopLsn()).isEqualTo(effectiveStopLsn);
        assertThat(changeTable.getSchemaSwitchLsn()).isEqualTo(schemaSwitchLsn);
        assertThat(changeTable.hasSchemaSwitchLsn()).isTrue();
    }
}
