/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2;

import java.sql.SQLException;

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

        connection = TestHelper.testConnection();

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);

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
    protected String database() {
        return TestHelper.TEST_DATABASE;
    }

    @Override
    protected String server() {
        return TestHelper.TEST_DATABASE;
    }

    @Override
    protected String snapshotStatusResult() {
        return "COMPLETED";
    }
}
