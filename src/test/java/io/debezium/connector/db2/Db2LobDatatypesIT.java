/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2ConnectorConfig.SnapshotMode;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test verifying that Db2 LOB columns (CLOB, DBCLOB, BLOB) are captured with their
 * actual content in the initial snapshot. This guards against the regression where the JCC
 * driver's lazy {@link java.sql.Clob}/{@link java.sql.Blob} handles were read too late and
 * surfaced as {@code null} or the handle's {@code toString()}.
 * <p>
 * Streaming LOB capture is intentionally not asserted here: the ASN capture change-data table does
 * not carry the LOB content (the LOB column is null there), so the value cannot be materialized
 * during streaming regardless of this fix. This mirrors other engines where the transaction/redo
 * log omits LOB payloads by default.
 */
public class Db2LobDatatypesIT extends AbstractAsyncEngineConnectorTest {

    private Db2Connection connection;

    private static final String CLOB_VALUE = "the quick brown fox";
    private static final String DBCLOB_VALUE = "unicode text ção";
    private static final byte[] BLOB_VALUE = { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
    private static final String BLOB_HEX = "DEADBEEF";

    @BeforeEach
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute("DROP TABLE IF EXISTS dt_lob");
        connection.execute("CREATE TABLE dt_lob ("
                + "id int not null, c_clob clob(1M), c_dbclob dbclob(1M), c_blob blob(1M), primary key (id))");
        // A row present before the connector starts is captured by the snapshot. Insert via a
        // committing execute() (as the other datatype ITs do) with LOB literals.
        connection.execute("INSERT INTO dt_lob VALUES(1, '" + CLOB_VALUE + "', '" + DBCLOB_VALUE
                + "', BLOB(X'" + BLOB_HEX + "'))");

        TestHelper.enableTableCdc(connection, "DT_LOB");
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @AfterEach
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);
            TestHelper.disableTableCdc(connection, "DT_LOB");
            connection.execute("DROP TABLE dt_lob");
            connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_COLVERSION");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_TABVERSION");
            connection.close();
        }
    }

    @Test
    public void lobTypesCapturedInSnapshot() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.TABLE_INCLUDE_LIST, "db2inst1.dt_lob")
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Snapshot (op=r): the LOB content is read from the base table with the row open, which is
        // exactly the path the fix materializes.
        SourceRecords records = consumeRecordsByTopic(1);
        SourceRecord snapshot = records.recordsForTopic("testdb.DB2INST1.DT_LOB").get(0);
        Struct after = ((Struct) snapshot.value()).getStruct("after");
        assertThat(after.get("C_CLOB")).isEqualTo(CLOB_VALUE);
        assertThat(after.get("C_DBCLOB")).isEqualTo(DBCLOB_VALUE);
        // Binary values are represented as a ByteBuffer by default (binary.handling.mode=bytes).
        assertThat(after.get("C_BLOB")).isEqualTo(ByteBuffer.wrap(BLOB_VALUE));

        stopConnector();
    }
}
