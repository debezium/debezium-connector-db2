/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2;

import java.sql.SQLException;
import java.util.List;

import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.pipeline.notification.AbstractNotificationsIT;
import io.debezium.util.Testing;

public class NotificationsIT extends AbstractNotificationsIT<Db2Connector> {

    private Db2Connection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllTables();
        connection = TestHelper.testConnection();

        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute(
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key (id))",
                "INSERT INTO tablea VALUES(1, 'a')");

        TestHelper.enableTableCdc(connection, "TABLEA");
        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);

            TestHelper.disableTableCdc(connection, "TABLEA");
            connection.execute("DROP TABLE tablea");
            connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_COLVERSION");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_TABVERSION");
            connection.close();
        }
    }

    @Override
    protected Class<Db2Connector> connectorClass() {
        return Db2Connector.class;
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, Db2ConnectorConfig.SnapshotMode.INITIAL);
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
    protected String snapshotStatusResult() {
        return "COMPLETED";
    }

    protected List<String> collections() {
        return List.of("DB2INST1.TABLEA");
    }
}
