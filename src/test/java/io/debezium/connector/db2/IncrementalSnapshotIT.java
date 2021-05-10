/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import io.debezium.config.Configuration.Builder;
import io.debezium.connector.db2.Db2ConnectorConfig.SnapshotMode;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;
import io.debezium.util.Testing;

public class IncrementalSnapshotIT extends AbstractIncrementalSnapshotTest<Db2Connector> {

    private Db2Connection connection;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        TestHelper.disableDbCdc(connection);
        TestHelper.disableTableCdc(connection, "A");
        TestHelper.disableTableCdc(connection, "DEBEZIUM_SIGNAL");
        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute(
                "DROP TABLE IF EXISTS a",
                "DROP TABLE IF EXISTS debezium_signal");
        connection.execute(
                "CREATE TABLE a (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");

        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);
        TestHelper.enableTableCdc(connection, "DEBEZIUM_SIGNAL");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);
            TestHelper.disableTableCdc(connection, "A");
            TestHelper.disableTableCdc(connection, "DEBEZIUM_SIGNAL");
            connection.rollback();
            connection.execute(
                    "DROP TABLE IF EXISTS a",
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
    protected Class<Db2Connector> connectorClass() {
        return Db2Connector.class;
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
    protected String tableName() {
        return "DB2INST1.A";
    }

    @Override
    protected String signalTableName() {
        return "DEBEZIUM_SIGNAL";
    }

    protected void sendAdHocSnapshotSignal() throws SQLException {
        connection.execute(
                String.format(
                        "INSERT INTO %s VALUES('ad-hoc', 'execute-snapshot', '{\"data-collections\": [\"%s\"]}')",
                        signalTableName(), tableName()));
        TestHelper.refreshAndWait(this.connection);
    }

    @Override
    protected Builder config() {
        return TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(Db2ConnectorConfig.SIGNAL_DATA_COLLECTION, "DB2INST1.DEBEZIUM_SIGNAL");
    }

    @Override
    protected String pkFieldName() {
        return "PK";
    }

    @Override
    protected String valueFieldName() {
        return "AA";
    }
}
