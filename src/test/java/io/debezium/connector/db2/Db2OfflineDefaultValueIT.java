/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.relational.TableId;

/**
 * Default value handling integration tests using offline schema evolution processes.
 *
 * @author Chris Cranford
 */
public class Db2OfflineDefaultValueIT extends AbstractDb2DefaultValueIT {
    @Override
    protected void performSchemaChange(Configuration config, Db2Connection connection, String alterStatement) throws Exception {
        stopConnector();

        final TableId tableId = TableId.parse("DB2INST1.DV_TEST");

        TestHelper.deactivateTable(connection, tableId.table());
        TestHelper.disableTableCdc(connection, tableId.table());

        final String sourceTable = alterStatement.replace("%table%", tableId.table());
        connection.execute(sourceTable);

        TestHelper.enableTableCdc(connection, tableId.table());
        TestHelper.activeTable(connection, tableId.table());

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning("db2_server", TestHelper.TEST_DATABASE);
    }
}
