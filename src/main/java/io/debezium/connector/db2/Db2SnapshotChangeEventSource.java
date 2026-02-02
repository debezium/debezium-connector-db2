/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.db2.Db2ConnectorConfig.SnapshotIsolationMode;
import io.debezium.connector.db2.Db2OffsetContext.Loader;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

public class Db2SnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<Db2Partition, Db2OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Db2SnapshotChangeEventSource.class);

    private final Db2ConnectorConfig connectorConfig;
    private final Db2Connection jdbcConnection;

    public Db2SnapshotChangeEventSource(Db2ConnectorConfig connectorConfig, MainConnectionProvidingConnectionFactory<Db2Connection> connectionFactory,
                                        Db2DatabaseSchema schema, EventDispatcher<Db2Partition, TableId> dispatcher, Clock clock,
                                        SnapshotProgressListener<Db2Partition> snapshotProgressListener,
                                        NotificationService<Db2Partition, Db2OffsetContext> notificationService, SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
    }

    @Override
    protected SnapshotContext<Db2Partition, Db2OffsetContext> prepare(Db2Partition partition, boolean onDemand) {
        return new Db2SnapshotContext(partition, jdbcConnection.getRealDatabaseName(), onDemand);
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext<Db2Partition, Db2OffsetContext> snapshotContext) throws Exception {
        ((Db2SnapshotContext) snapshotContext).isolationLevelBeforeStart = jdbcConnection.connection().getTransactionIsolation();
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<Db2Partition, Db2OffsetContext> ctx) throws Exception {
        return jdbcConnection.readTableNames(null, null, null, new String[]{ "TABLE" });
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<Db2Partition, Db2OffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.READ_UNCOMMITTED) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
        else if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.READ_COMMITTED) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
        else if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.EXCLUSIVE
                || connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
            jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            ((Db2SnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("db2_schema_snapshot");

            LOGGER.info("Executing schema locking");
            try (Statement statement = jdbcConnection.connection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                for (TableId tableId : snapshotContext.capturedTables) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while locking table " + tableId);
                    }

                    Optional<String> lockingStatement = snapshotterService.getSnapshotLock().tableLockingStatement(connectorConfig.snapshotLockTimeout(),
                            quoteTableName(tableId));

                    if (lockingStatement.isPresent()) {
                        LOGGER.info("Locking table {}", tableId);
                        statement.executeQuery(lockingStatement.get()).close();
                    }
                }
            }
        }
        else {
            throw new IllegalStateException("Unknown locking mode specified.");
        }
    }

    private String quoteTableName(TableId tableId) {

        return String.format("%s.%s",
                Db2ObjectNameQuoter.quoteNameIfNecessary(tableId.schema()),
                Db2ObjectNameQuoter.quoteNameIfNecessary(tableId.table()));
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<Db2Partition, Db2OffsetContext> snapshotContext) throws SQLException {
        // Exclusive mode: locks should be kept until the end of transaction.
        // read_uncommitted mode; read_committed mode: no locks have been acquired.
        if (connectorConfig.getSnapshotIsolationMode() == SnapshotIsolationMode.REPEATABLE_READ) {
            jdbcConnection.connection().rollback(((Db2SnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
            LOGGER.info("Schema locks released.");
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<Db2Partition, Db2OffsetContext> ctx, Db2OffsetContext previousOffset) throws Exception {
        if (previousOffset != null && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot()) {
            ctx.offset = previousOffset;
            tryStartingSnapshot(ctx);
            return;
        }

        ctx.offset = new Db2OffsetContext(
                connectorConfig,
                TxLogPosition.valueOf(jdbcConnection.getMaxLsn()),
                null,
                false);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<Db2Partition, Db2OffsetContext> snapshotContext,
                                      Db2OffsetContext previousOffset, SnapshottingTask snapshottingTask)
            throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            LOGGER.info("Reading structure of schema '{}'", schema);

            Tables.TableFilter tableFilter = snapshottingTask.isOnDemand() ? Tables.TableFilter.fromPredicate(snapshotContext.capturedTables::contains)
                    : connectorConfig.getTableFilters().dataCollectionFilter();

            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    null,
                    schema,
                    tableFilter,
                    null,
                    false);
        }
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<Db2Partition, Db2OffsetContext> snapshotContext,
                                                    Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    @Override
    protected void completed(SnapshotContext<Db2Partition, Db2OffsetContext> snapshotContext) {
        close(snapshotContext);
    }

    @Override
    protected void aborted(SnapshotContext<Db2Partition, Db2OffsetContext> snapshotContext) {
        close(snapshotContext);
    }

    @Override
    protected void postSnapshot() throws InterruptedException {
        if (connectionPool != null) {
            for (JdbcConnection conn : connectionPool) {
                if (!jdbcConnection.equals(conn)) {
                    try {
                        if (conn.isConnected()) {
                            conn.rollback();
                        }
                    }
                    catch (SQLException e) {
                        throw new DebeziumException("Failed to rollback snapshot connection", e);
                    }
                }
            }
        }
    }

    private void close(SnapshotContext<Db2Partition, Db2OffsetContext> snapshotContext) {
        try {
            jdbcConnection.connection().setTransactionIsolation(((Db2SnapshotContext) snapshotContext).isolationLevelBeforeStart);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to set transaction isolation level.", e);
        }
    }

    /**
     * Generate a valid db2 query string for the specified table
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<Db2Partition, Db2OffsetContext> snapshotContext, TableId tableId, List<String> columns) {

        return snapshotterService.getSnapshotQuery().snapshotQuery(quoteTableName(tableId), columns);
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class Db2SnapshotContext extends RelationalSnapshotContext<Db2Partition, Db2OffsetContext> {

        private int isolationLevelBeforeStart;
        private Savepoint preSchemaSnapshotSavepoint;

        Db2SnapshotContext(Db2Partition partition, String catalogName, boolean onDemand) {
            super(partition, catalogName, onDemand);
        }
    }

    @Override
    protected Db2OffsetContext copyOffset(RelationalSnapshotContext<Db2Partition, Db2OffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }

}
