/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2;

import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestExtension;
import io.debezium.pipeline.AbstractBlockingSnapshotTest;
import io.debezium.util.Testing;

@ExtendWith(SkipTestExtension.class)
public class BlockingSnapshotIT extends AbstractBlockingSnapshotTest {

    private Db2Connection connection;

    @BeforeEach
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        TestHelper.disableDbCdc(connection);
        TestHelper.disableTableCdc(connection, "A");
        TestHelper.disableTableCdc(connection, "B");
        TestHelper.disableTableCdc(connection, "DEBEZIUM_SIGNAL");
        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute(
                "DROP TABLE IF EXISTS a",
                "DROP TABLE IF EXISTS b",
                "DROP TABLE IF EXISTS debezium_signal");
        connection.execute(
                "CREATE TABLE a (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE b (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");

        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);
        TestHelper.enableTableCdc(connection, "DEBEZIUM_SIGNAL");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @AfterEach
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);
            TestHelper.disableTableCdc(connection, "A");
            TestHelper.disableTableCdc(connection, "B");
            TestHelper.disableTableCdc(connection, "DEBEZIUM_SIGNAL");
            connection.rollback();
            connection.execute(
                    "DROP TABLE IF EXISTS a",
                    "DROP TABLE IF EXISTS b",
                    "DROP TABLE IF EXISTS debezium_signal");
            connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_COLVERSION");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_TABVERSION");
            connection.close();
        }
    }

    @Override
    protected void populateTable() throws SQLException {
        super.populateTable(connection);
        TestHelper.enableTableCdc(connection, "A");
    }

    @Override
    protected void populateTables() throws SQLException {
        super.populateTables();
        TestHelper.enableTableCdc(connection, "A");
        TestHelper.enableTableCdc(connection, "B");
    }

    @Override
    protected Class<Db2Connector> connectorClass() {
        return Db2Connector.class;
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        return config();
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected String topicName() {
        return "testdb.DB2INST1.A";
    }

    @Override
    protected List<String> topicNames() {
        return List.of(topicName(), "testdb.DB2INST1.B");
    }

    @Override
    protected String tableName() {
        return "DB2INST1.A";
    }

    @Override
    protected List<String> tableNames() {
        return List.of(tableName(), "DB2INST1.B");
    }

    @Override
    protected String signalTableName() {
        return "DEBEZIUM_SIGNAL";
    }

    @Override
    protected String escapedTableDataCollectionId() {
        return "\\\"DB2INST1\\\".\\\"A\\\"";
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, Db2ConnectorConfig.SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.SIGNAL_DATA_COLLECTION, "DB2INST1.DEBEZIUM_SIGNAL")
                .with(Db2ConnectorConfig.SNAPSHOT_MODE_TABLES, "DB2INST1.A")
                .with(Db2ConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 250);
    }

    @Override
    protected String pkFieldName() {
        return "PK";
    }

    @Override
    protected String valueFieldName() {
        return "AA";
    }

    @Override
    protected String connector() {
        return "db2_server";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_DATABASE;
    }

    @Override
    protected int insertMaxSleep() {
        return 100;
    }
}
