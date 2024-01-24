/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import java.math.BigDecimal;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2ConnectorConfig.SnapshotMode;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.util.Testing;

/**
 * Test fror Db2 datatypes.
 *
 * @author Jiri Pechanec
 */
public class DatatypesFromSnapshotIT extends AbstractConnectorTest {

    private Db2Connection connection;

    private static final String[] CREATE_TABLES = {
            "CREATE TABLE dt_numeric (id int not null, df decfloat, df16 decfloat(16), df34 decfloat(34), primary key (id))"
    };
    private static final String[] INSERT_DATA = {
            "INSERT INTO dt_numeric VALUES(1, 1, 3.123456789012345678, 3.012345678901234567890123456789)"
    };

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute("DROP TABLE IF EXISTS dt_numeric");
        connection.execute(CREATE_TABLES);
        connection.execute(INSERT_DATA);

        TestHelper.enableTableCdc(connection, "DT_NUMERIC");
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);
            TestHelper.disableTableCdc(connection, "DT_NUMERIC");
            connection.execute("DROP TABLE dt_numeric");
            connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_COLVERSION");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_TABVERSION");
            connection.close();
        }
    }

    @Test
    public void numericTypesPrecise() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.TABLE_INCLUDE_LIST, "db2inst1.dt_numeric")
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        final SourceRecords records = consumeRecordsByTopic(1);
        final SourceRecord rec = records.allRecordsInOrder().get(0);

        assertVariableScaleDecimal(((Struct) rec.value()).getStruct("after").get("DF"), new BigDecimal("1"));
        // Loss of precision
        assertVariableScaleDecimal(((Struct) rec.value()).getStruct("after").get("DF16"), new BigDecimal("3.123456789012346"));
        assertVariableScaleDecimal(((Struct) rec.value()).getStruct("after").get("DF34"), new BigDecimal("3.012345678901234567890123456789"));
        stopConnector();
    }

    @Test
    public void numericTypesDouble() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.TABLE_INCLUDE_LIST, "db2inst1.dt_numeric")
                .with(Db2ConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        final SourceRecords records = consumeRecordsByTopic(1);
        final SourceRecord rec = records.allRecordsInOrder().get(0);

        Assertions.assertThat(((Struct) rec.value()).getStruct("after").get("DF"))
                .isEqualTo(Double.valueOf(1));
        // Loss of precision
        Assertions.assertThat(((Struct) rec.value()).getStruct("after").get("DF16"))
                .isEqualTo(Double.valueOf(3.123456789012346));
        Assertions.assertThat(((Struct) rec.value()).getStruct("after").get("DF34"))
                .isEqualTo(Double.valueOf(3.012345678901234567890123456789));

        stopConnector();
    }

    @Test
    public void numericTypesString() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.TABLE_INCLUDE_LIST, "db2inst1.dt_numeric")
                .with(Db2ConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING)
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        final SourceRecords records = consumeRecordsByTopic(1);
        final SourceRecord rec = records.allRecordsInOrder().get(0);

        Assertions.assertThat(((Struct) rec.value()).getStruct("after").get("DF"))
                .isEqualTo("1");
        // Loss of precision
        Assertions.assertThat(((Struct) rec.value()).getStruct("after").get("DF16"))
                .isEqualTo("3.123456789012346");
        Assertions.assertThat(((Struct) rec.value()).getStruct("after").get("DF34"))
                .isEqualTo("3.012345678901234567890123456789");

        stopConnector();
    }

    private void assertVariableScaleDecimal(Object actual, BigDecimal expected) {
        final Struct v = (Struct) actual;
        Assertions.assertThat(v.get(VariableScaleDecimal.SCALE_FIELD)).isEqualTo(expected.scale());
        Assertions.assertThat(v.get(VariableScaleDecimal.VALUE_FIELD)).isEqualTo(expected.unscaledValue().toByteArray());
    }
}
