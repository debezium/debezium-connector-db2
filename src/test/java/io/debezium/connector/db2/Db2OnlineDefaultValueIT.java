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
 * Default value handling integration tests using online schema evolution processes.
 *
 * @author Chris Cranford
 */
public class Db2OnlineDefaultValueIT extends AbstractDb2DefaultValueIT {
    @Override
    protected void performSchemaChange(Configuration config, Db2Connection connection, String alterStatement) throws Exception {
        final TableId tableId = TableId.parse("DB2INST1.DV_TEST");

        connection.lockTable(tableId);

        final String sourceTable = alterStatement.replace("%table%", tableId.table());
        final String changeTable = alterStatement.replace("%table%", TestHelper.getCdcTableName(connection, tableId.table()));
        connection.execute(sourceTable);
        connection.execute(changeTable);

        TestHelper.deactivateTable(connection, tableId.table());
        TestHelper.activeTable(connection, tableId.table());
    }
}
