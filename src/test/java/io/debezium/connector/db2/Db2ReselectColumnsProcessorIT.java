/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.processors.AbstractReselectProcessorTest;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;
import io.debezium.util.Testing;

/**
 * Db2's integration tests for {@link ReselectColumnsPostProcessor}.
 *
 * @author Chris Cranford
 */
public class Db2ReselectColumnsProcessorIT extends AbstractReselectProcessorTest<Db2Connector> {

    private Db2Connection connection;

    @Before
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        super.beforeEach();
    }

    @After
    public void afterEach() throws Exception {
        try {
            super.afterEach();
        }
        finally {
            if (connection != null) {
                try {
                    TestHelper.disableDbCdc(connection);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    TestHelper.disableTableCdc(connection, "DBZ4321");
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                connection.execute("DROP TABLE dbz4321 IF EXISTS");
                connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
                connection.execute("DELETE FROM ASNCDC.IBMQREP_COLVERSION");
                connection.execute("DELETE FROM ASNCDC.IBMQREP_TABVERSION");
                connection.close();
            }
        }
    }

    @Override
    protected Class<Db2Connector> getConnectorClass() {
        return Db2Connector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.TABLE_INCLUDE_LIST, "DB2INST1\\.DBZ4321")
                .with(Db2ConnectorConfig.CUSTOM_POST_PROCESSORS, "reselector")
                .with("post.processors.reselector.type", ReselectColumnsPostProcessor.class.getName());
    }

    @Override
    protected String topicName() {
        return "testdb.DB2INST1.DBZ4321";
    }

    @Override
    protected String tableName() {
        return "DB2INST1.DBZ4321";
    }

    @Override
    protected String reselectColumnsList() {
        return "DB2INST1.DBZ4321:DATA";
    }

    @Override
    protected void createTable() throws Exception {
        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute("DROP TABLE DBZ4321 IF EXISTS");
        TestHelper.enableDbCdc(connection);
        connection.execute("CREATE TABLE DBZ4321 (id int not null, data varchar(50), data2 int, primary key(id))");
    }

    @Override
    protected void dropTable() throws Exception {
    }

    @Override
    protected String getInsertWithValue() {
        return "INSERT INTO dbz4321 (id,data,data2) values (1,'one',1)";
    }

    @Override
    protected String getInsertWithNullValue() {
        return "INSERT INTO dbz4321 (id,data,data2) values (1,null,1)";
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning("db2_server", TestHelper.TEST_DATABASE);
    }

    @Override
    protected String fieldName(String fieldName) {
        return fieldName.toUpperCase();
    }

    @Override
    protected void enableTableForCdc() throws Exception {
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);
        TestHelper.enableTableCdc(connection, "DBZ4321");
    }
}
