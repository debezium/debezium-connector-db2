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

import io.debezium.config.Configuration;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractChunkedSnapshotTest;
import io.debezium.util.Testing;

/**
 * Oracle-specific chunked table snapshot integration tests.
 *
 * @author Chris Cranford
 */
public class Db2ChunkedSnapshotIT extends AbstractChunkedSnapshotTest<Db2Connector> {

    private Db2Connection connection;

    @BeforeEach
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.dropAllTables();
        TestHelper.disableDbCdc(connection);

        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);

        super.beforeEach();
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);
            connection.close();
        }
        super.afterEach();
    }

    @Override
    protected void populateSingleKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateSingleKeyTable(tableName, rowCount);
        enableCdc(tableName);
    }

    @Override
    protected void populateCompositeKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateCompositeKeyTable(tableName, rowCount);
        enableCdc(tableName);
    }

    @Override
    protected String getSingleKeyTableName() {
        return "DBZ1220";
    }

    @Override
    protected String getCompositeKeyTableName() {
        return "DBZ1220";
    }

    @Override
    protected List<String> getMultipleSingleKeyTableNames() {
        return List.of("DBZ1220A", "DBZ1220B", "DBZ1220C", "DBZ1220D");
    }

    @Override
    protected Class<Db2Connector> getConnectorClass() {
        return Db2Connector.class;
    }

    @Override
    protected JdbcConnection getConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfig() {
        return TestHelper.defaultConfig();
    }

    @Override
    protected void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted("db2_server", TestHelper.TEST_DATABASE);
    }

    @Override
    protected String getSingleKeyCollectionName() {
        return "DB2INST1\\.DBZ1220";
    }

    @Override
    protected String getCompositeKeyCollectionName() {
        return getSingleKeyCollectionName();
    }

    @Override
    protected String getMultipleSingleKeyCollectionNames() {
        return String.join(",", List.of("DB2INST1\\.DBZ1220A", "DB2INST1\\.DBZ1220B", "DB2INST1\\.DBZ1220C", "DB2INST1\\.DBZ1220D"));
    }

    @Override
    protected void createSingleKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id int not null, data varchar(50), primary key(id))".formatted(tableName.toUpperCase()));
    }

    @Override
    protected void createCompositeKeyTable(String tableName) throws SQLException {
        connection.execute(
                "CREATE TABLE %s (id int not null, org_name varchar(50) not null, data varchar(50), primary key(id, org_name))".formatted(tableName.toUpperCase()));
    }

    @Override
    protected void createKeylessTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id int, data varchar(50))".formatted(tableName.toUpperCase()));
    }

    @Override
    protected String getSingleKeyTableKeyColumnName() {
        return "ID";
    }

    @Override
    protected List<String> getCompositeKeyTableKeyColumnNames() {
        return List.of("ID", "ORG_NAME");
    }

    @Override
    protected String getTableTopicName(String tableName) {
        return "testdb.DB2INST1.%s".formatted(tableName.toUpperCase());
    }

    @Override
    protected String getFullyQualifiedTableName(String tableName) {
        return "DB2INST1.%s".formatted(tableName.toUpperCase());
    }

    private void enableCdc(String tableName) throws SQLException {
        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);
        TestHelper.enableTableCdc(connection, tableName);
    }

}
