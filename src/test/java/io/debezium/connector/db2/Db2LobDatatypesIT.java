/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Base64;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2ConnectorConfig.SnapshotMode;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test verifying that Db2 LOB columns (CLOB, DBCLOB, BLOB) are captured with their
 * actual content, both in the initial snapshot and while streaming. This guards against the
 * regression where the JCC driver's lazy {@link java.sql.Clob}/{@link java.sql.Blob} handles were
 * read too late and surfaced as {@code null} or the handle's {@code toString()}.
 */
public class Db2LobDatatypesIT extends AbstractAsyncEngineConnectorTest {

    private Db2Connection connection;

    private static final String CLOB_VALUE = "the quick brown fox";
    private static final String DBCLOB_VALUE = "unicode text ção";
    private static final byte[] BLOB_VALUE = { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
    private static final String BLOB_BASE64 = Base64.getEncoder().encodeToString(BLOB_VALUE);

    @BeforeEach
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute("DROP TABLE IF EXISTS dt_lob");
        connection.execute("CREATE TABLE dt_lob ("
                + "id int not null, c_clob clob(1M), c_dbclob dbclob(1M), c_blob blob(1M), primary key (id))");
        // A row present before the connector starts is captured by the snapshot.
        connection.prepareUpdate("INSERT INTO dt_lob VALUES(1, ?, ?, ?)", ps -> {
            ps.setString(1, CLOB_VALUE);
            ps.setString(2, DBCLOB_VALUE);
            ps.setBytes(3, BLOB_VALUE);
        });
        connection.commit();

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
    public void lobTypesCapturedInSnapshotAndStreaming() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.TABLE_INCLUDE_LIST, "db2inst1.dt_lob")
                .with(Db2ConnectorConfig.BINARY_HANDLING_MODE, BinaryHandlingMode.BASE64)
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // --- snapshot (op=r) ---
        SourceRecords records = consumeRecordsByTopic(1);
        SourceRecord snapshot = records.recordsForTopic("testdb.DB2INST1.DT_LOB").get(0);
        Struct after = ((Struct) snapshot.value()).getStruct("after");
        assertThat(after.get("C_CLOB")).isEqualTo(CLOB_VALUE);
        assertThat(after.get("C_DBCLOB")).isEqualTo(DBCLOB_VALUE);
        assertThat(after.get("C_BLOB")).isEqualTo(BLOB_BASE64);

        // --- streaming (op=c) ---
        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);

        connection.prepareUpdate("INSERT INTO dt_lob VALUES(2, ?, ?, ?)", ps -> {
            ps.setString(1, CLOB_VALUE);
            ps.setString(2, DBCLOB_VALUE);
            ps.setBytes(3, BLOB_VALUE);
        });
        connection.commit();
        TestHelper.refreshAndWait(connection);

        records = consumeRecordsByTopic(1);
        SourceRecord streamed = records.recordsForTopic("testdb.DB2INST1.DT_LOB").get(0);
        Struct streamedAfter = ((Struct) streamed.value()).getStruct("after");
        assertThat(streamedAfter.get("C_CLOB")).isEqualTo(CLOB_VALUE);
        assertThat(streamedAfter.get("C_DBCLOB")).isEqualTo(DBCLOB_VALUE);
        assertThat(streamedAfter.get("C_BLOB")).isEqualTo(BLOB_BASE64);

        stopConnector();
    }
}
