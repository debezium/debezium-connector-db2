/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.db2.jcc.DB2Driver;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.db2.platform.Db2PlatformAdapter;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.BoundedConcurrentHashMap;

/**
 * {@link JdbcConnection} extension to be used with IBM Db2
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec, Peter Urbanetz
 *
 */
public class Db2Connection extends JdbcConnection {

    private static final String GET_DATABASE_NAME = "SELECT CURRENT SERVER FROM SYSIBM.SYSDUMMY1"; // DB2

    private static Logger LOGGER = LoggerFactory.getLogger(Db2Connection.class);

    private static final String STATEMENTS_PLACEHOLDER = "#";

    private static final String LOCK_TABLE = "SELECT * FROM # WITH CS"; // DB2

    private static final String LSN_TO_TIMESTAMP = "SELECT CURRENT TIMEstamp FROM sysibm.sysdummy1  WHERE ? > X'00000000000000000000000000000000'";

    private static final String GET_LIST_OF_KEY_COLUMNS = "SELECT "
            + "CAST((t.TBSPACEID * 65536 +  t.TABLEID )AS INTEGER ) as objectid, "
            + "c.colname,c.colno,c.keyseq "
            + "FROM syscat.tables  as t "
            + "inner join syscat.columns as c  on t.tabname = c.tabname and t.tabschema = c.tabschema and c.KEYSEQ > 0 AND "
            + "t.tbspaceid = CAST(BITAND( ? , 4294901760) / 65536 AS SMALLINT) AND t.tableid=  CAST(BITAND( ? , 65535) AS SMALLINT)";

    private static final int CHANGE_TABLE_DATA_COLUMN_OFFSET = 4;

    private static final String QUOTED_CHARACTER = "\"";

    private static final String URL_PATTERN = "jdbc:db2://${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            DB2Driver.class.getName(),
            Db2Connection.class.getClassLoader(),
            JdbcConfiguration.PORT.withDefault(Db2ConnectorConfig.PORT.defaultValueAsString()));

    /**
     * actual name of the database, which could differ in casing from the database name given in the connector config.
     */
    private final String realDatabaseName;

    private final BoundedConcurrentHashMap<Lsn, Instant> lsnToInstantCache;

    private final Db2ConnectorConfig connectorConfig;
    private final Db2PlatformAdapter platform;

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public Db2Connection(Db2ConnectorConfig config) {
        super(config.getJdbcConfig(), FACTORY, QUOTED_CHARACTER, QUOTED_CHARACTER);

        connectorConfig = config;
        lsnToInstantCache = new BoundedConcurrentHashMap<>(100);
        realDatabaseName = retrieveRealDatabaseName();
        platform = connectorConfig.getDb2Platform().createAdapter(connectorConfig);
    }

    /**
     * @return the current largest log sequence number
     */
    public Lsn getMaxLsn() throws SQLException {
        return queryAndMap(platform.getMaxLsnQuery(), singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOGGER.trace("Current maximum lsn is {}", ret);
            return ret;
        }, "Maximum LSN query must return exactly one value"));
    }

    /**
     * Provides all changes recorded by the DB2 CDC capture process for a given table.
     *
     * @param tableId  - the requested table changes
     * @param fromLsn  - closed lower bound of interval of changes to be provided
     * @param toLsn    - closed upper bound of interval  of changes to be provided
     * @param consumer - the change processor
     * @throws SQLException
     */
    public void getChangesForTable(TableId tableId, Lsn fromLsn, Lsn toLsn, ResultSetConsumer consumer) throws SQLException {
        final String query = platform.getAllChangesForTableQuery().replace(STATEMENTS_PLACEHOLDER, cdcNameForTable(tableId));
        prepareQuery(query, statement -> {
            statement.setBytes(1, fromLsn.getBinary());
            statement.setBytes(2, toLsn.getBinary());

        }, consumer);
    }

    /**
     * Provides all changes recorder by the DB2 CDC capture process for a set of tables.
     *
     * @param changeTables    - the requested tables to obtain changes for
     * @param intervalFromLsn - closed lower bound of interval of changes to be provided
     * @param intervalToLsn   - closed upper bound of interval  of changes to be provided
     * @param consumer        - the change processor
     * @throws SQLException
     */
    public void getChangesForTables(Db2ChangeTable[] changeTables, Lsn intervalFromLsn, Lsn intervalToLsn, BlockingMultiResultSetConsumer consumer)
            throws SQLException, InterruptedException {
        final String[] queries = new String[changeTables.length];
        final StatementPreparer[] preparers = new StatementPreparer[changeTables.length];

        int idx = 0;
        for (Db2ChangeTable changeTable : changeTables) {
            final String query = platform.getAllChangesForTableQuery().replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
            queries[idx] = query;
            // If the table was added in the middle of queried buffer we need
            // to adjust from to the first LSN available
            LOGGER.trace("Getting changes for table {} in range[{}, {}]", changeTable, intervalFromLsn, intervalToLsn);
            preparers[idx] = statement -> {
                statement.setBytes(1, intervalFromLsn.getBinary());
                statement.setBytes(2, intervalToLsn.getBinary());

            };

            idx++;
        }
        prepareQuery(queries, preparers, consumer);
    }

    /**
     * Obtain the next available position in the database log.
     *
     * @param lsn - LSN of the current position
     * @return LSN of the next position in the database
     * @throws SQLException
     */
    public Lsn incrementLsn(Lsn lsn) throws SQLException {
        return lsn.increment();
    }

    /**
     * Map a commit LSN to a point in time when the commit happened.
     *
     * @param lsn - LSN of the commit
     * @return time when the commit was recorded into the database log
     * @throws SQLException
     */
    public Instant timestampOfLsn(Lsn lsn) throws SQLException {
        final String query = LSN_TO_TIMESTAMP;

        if (lsn.getBinary() == null) {
            return null;
        }

        Instant cachedInstant = lsnToInstantCache.get(lsn);
        if (cachedInstant != null) {
            return cachedInstant;
        }

        return prepareQueryAndMap(query, statement -> {
            statement.setBytes(1, lsn.getBinary());
        }, singleResultMapper(rs -> {
            final Timestamp ts = rs.getTimestamp(1);
            final Instant ret = (ts == null) ? null : ts.toInstant();
            LOGGER.trace("Timestamp of lsn {} is {}", lsn, ret);
            if (ret != null) {
                lsnToInstantCache.put(lsn, ret);
            }
            return ret;
        }, "LSN to timestamp query must return exactly one value"));
    }

    @Override
    public Optional<Instant> getCurrentTimestamp() throws SQLException {
        return queryAndMap("SELECT CURRENT_TIMESTAMP result FROM sysibm.sysdummy1",
                rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty());
    }

    /**
     * Creates an exclusive lock for a given table.
     *
     * @param tableId to be locked
     * @throws SQLException
     */
    public void lockTable(TableId tableId) throws SQLException {
        final String lockTableStmt = LOCK_TABLE.replace(STATEMENTS_PLACEHOLDER, tableId.table());
        execute(lockTableStmt);
    }

    private String cdcNameForTable(TableId tableId) {
        return Db2ObjectNameQuoter.quoteNameIfNecessary(tableId.schema() + '_' + tableId.table());
    }

    public static class CdcEnabledTable {
        private final String tableId;
        private final String captureName;
        private final Lsn fromLsn;

        private CdcEnabledTable(String tableId, String captureName, Lsn fromLsn) {
            this.tableId = tableId;
            this.captureName = captureName;
            this.fromLsn = fromLsn;
        }

        public String getTableId() {
            return tableId;
        }

        public String getCaptureName() {
            return captureName;
        }

        public Lsn getFromLsn() {
            return fromLsn;
        }
    }

    public Set<Db2ChangeTable> listOfChangeTables() throws SQLException {

        return queryAndMap(platform.getListOfCdcEnabledTablesQuery(), rs -> {
            final Set<Db2ChangeTable> changeTables = new HashSet<>();
            while (rs.next()) {
                /**
                 changeTables.add(
                 new ChangeTable(
                 new TableId(realDatabaseName, rs.getString(1), rs.getString(2)),
                 rs.getString(3),
                 rs.getInt(4),
                 Lsn.valueOf(rs.getBytes(6)),
                 Lsn.valueOf(rs.getBytes(7))

                 )
                 **/
                changeTables.add(
                        new Db2ChangeTable(
                                new TableId("", rs.getString(1), rs.getString(2)),
                                rs.getString(4),
                                rs.getInt(9),
                                Lsn.valueOf(rs.getBytes(5)),
                                Lsn.valueOf(rs.getBytes(6)),
                                connectorConfig.getCdcChangeTablesSchema()

                        ));
            }
            return changeTables;
        });
    }

    public Set<Db2ChangeTable> listOfNewChangeTables(Lsn fromLsn, Lsn toLsn) throws SQLException {

        return prepareQueryAndMap(platform.getListOfNewCdcEnabledTablesQuery(),
                ps -> {
                    ps.setBytes(1, fromLsn.getBinary());
                    ps.setBytes(2, toLsn.getBinary());
                },
                rs -> {
                    final Set<Db2ChangeTable> changeTables = new HashSet<>();
                    while (rs.next()) {
                        changeTables.add(new Db2ChangeTable(
                                rs.getString(2),
                                rs.getInt(1),
                                Lsn.valueOf(rs.getBytes(3)),
                                Lsn.valueOf(rs.getBytes(4)),
                                connectorConfig.getCdcChangeTablesSchema()));
                    }
                    return changeTables;
                });
    }

    public Table getTableSchemaFromTable(Db2ChangeTable changeTable) throws SQLException {
        final DatabaseMetaData metadata = connection().getMetaData();

        List<Column> columns = new ArrayList<>();
        try (ResultSet rs = metadata.getColumns(
                null,
                changeTable.getSourceTableId().schema(),
                changeTable.getSourceTableId().table(),
                null)) {
            while (rs.next()) {
                readTableColumn(rs, changeTable.getSourceTableId(), null).ifPresent(ce -> columns.add(ce.create()));
            }
        }

        final List<String> pkColumnNames = readPrimaryKeyNames(metadata, changeTable.getSourceTableId());
        Collections.sort(columns);
        return Table.editor()
                .tableId(changeTable.getSourceTableId())
                .addColumns(columns)
                .setPrimaryKeyNames(pkColumnNames)
                .create();
    }

    public Table getTableSchemaFromChangeTable(Db2ChangeTable changeTable) throws SQLException {
        final DatabaseMetaData metadata = connection().getMetaData();
        final TableId changeTableId = changeTable.getChangeTableId();

        List<ColumnEditor> columnEditors = new ArrayList<>();
        try (ResultSet rs = metadata.getColumns(null, changeTableId.schema(), changeTableId.table(), null)) {
            while (rs.next()) {
                readTableColumn(rs, changeTableId, null).ifPresent(columnEditors::add);
            }
        }

        // The first 5 columns and the last column of the change table are CDC metadata
        // final List<Column> columns = columnEditors.subList(CHANGE_TABLE_DATA_COLUMN_OFFSET, columnEditors.size() - 1).stream()
        final List<Column> columns = columnEditors.subList(CHANGE_TABLE_DATA_COLUMN_OFFSET, columnEditors.size()).stream()
                .map(c -> c.position(c.position() - CHANGE_TABLE_DATA_COLUMN_OFFSET).create())
                .collect(Collectors.toList());

        final List<String> pkColumnNames = new ArrayList<>();
        /**  URB
         prepareQuery(GET_LIST_OF_KEY_COLUMNS, ps -> ps.setInt(1, changeTable.getChangeTableObjectId()), rs -> {
         while (rs.next()) {
         pkColumnNames.add(rs.getString(2));
         }
         });
         **/
        prepareQuery(GET_LIST_OF_KEY_COLUMNS, ps -> {
            ps.setInt(1, changeTable.getChangeTableObjectId());
            ps.setInt(1, changeTable.getChangeTableObjectId());
        }, rs -> {
            while (rs.next()) {
                pkColumnNames.add(rs.getString(2));
            }
        });
        Collections.sort(columns);
        return Table.editor()
                .tableId(changeTable.getSourceTableId())
                .addColumns(columns)
                .setPrimaryKeyNames(pkColumnNames)
                .create();
    }

    public String getNameOfChangeTable(String captureName) {
        return captureName + "_CT";
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }

    @Override
    protected boolean isTableUniqueIndexIncluded(String indexName, String columnName) {
        // ignore indices with no name;
        return indexName != null;
    }

    private String retrieveRealDatabaseName() {
        try {
            return queryAndMap(
                    GET_DATABASE_NAME,
                    singleResultMapper(rs -> rs.getString(1), "Could not retrieve database name"));
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't obtain database name", e);
        }
    }

    /**
     * Returns a JDBC connection string for the current configuration.
     *
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values from the configuration
     */
    public String connectionString() {
        return connectionString(URL_PATTERN);
    }

    @Override
    public Optional<Boolean> nullsSortLast() {
        // "The null value is higher than all other values"
        // https://www.ibm.com/docs/en/db2/11.5?topic=subselect-order-by-clause
        return Optional.of(true);
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        StringBuilder quoted = new StringBuilder();

        if (tableId.catalog() != null && !tableId.catalog().isEmpty()) {
            quoted.append(Db2ObjectNameQuoter.quoteNameIfNecessary(tableId.catalog())).append(".");
        }

        if (tableId.schema() != null && !tableId.schema().isEmpty()) {
            quoted.append(Db2ObjectNameQuoter.quoteNameIfNecessary(tableId.schema())).append(".");
        }

        quoted.append(Db2ObjectNameQuoter.quoteNameIfNecessary(tableId.table()));
        return quoted.toString();
    }

    @Override
    public JdbcConnection prepareQuery(String[] multiQuery, StatementPreparer[] preparers, BlockingMultiResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
        final ResultSet[] resultSets = new ResultSet[multiQuery.length];
        final PreparedStatement[] preparedStatements = new PreparedStatement[multiQuery.length];

        try {
            for (int i = 0; i < multiQuery.length; i++) {
                final String query = multiQuery[i];
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("running '{}'", query);
                }
                // Purposely create the statement this way
                final PreparedStatement statement = createPreparedStatement(query);
                preparedStatements[i] = statement;
                preparers[i].accept(statement);
                resultSets[i] = statement.executeQuery();
            }
            if (resultConsumer != null) {
                resultConsumer.accept(resultSets);
            }
        }
        finally {
            for (ResultSet rs : resultSets) {
                if (rs != null) {
                    try {
                        rs.close();
                    }
                    catch (Exception ei) {
                    }
                }
            }
            // Db2 requires closing prepared statements to avoid caching result-set column structures
            for (PreparedStatement ps : preparedStatements) {
                closePreparedStatement(ps);
            }
        }
        return this;
    }

    @Override
    public JdbcConnection prepareQueryWithBlockingConsumer(String preparedQueryString, StatementPreparer preparer, BlockingResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
        // Db2 requires closing prepared statements to avoid caching result-set column structures
        try (PreparedStatement statement = createPreparedStatement(preparedQueryString)) {
            preparer.accept(statement);
            try (ResultSet resultSet = statement.executeQuery();) {
                if (resultConsumer != null) {
                    resultConsumer.accept(resultSet);
                }
            }
        }
        return this;
    }

    @Override
    public JdbcConnection prepareQuery(String preparedQueryString) throws SQLException {
        // Db2 requires closing prepared statements to avoid caching result-set column structures
        try (PreparedStatement statement = createPreparedStatement(preparedQueryString)) {
            statement.executeQuery();
        }
        return this;
    }

    @Override
    public JdbcConnection prepareQuery(String preparedQueryString, StatementPreparer preparer, ResultSetConsumer resultConsumer)
            throws SQLException {
        // Db2 requires closing prepared statements to avoid caching result-set column structures
        try (PreparedStatement statement = createPreparedStatement(preparedQueryString)) {
            preparer.accept(statement);
            try (ResultSet resultSet = statement.executeQuery();) {
                if (resultConsumer != null) {
                    resultConsumer.accept(resultSet);
                }
            }
        }
        return this;
    }

    @Override
    public <T> T prepareQueryAndMap(String preparedQueryString, StatementPreparer preparer, ResultSetMapper<T> mapper)
            throws SQLException {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        // Db2 requires closing prepared statements to avoid caching result-set column structures
        try (PreparedStatement statement = createPreparedStatement(preparedQueryString)) {
            preparer.accept(statement);
            try (ResultSet resultSet = statement.executeQuery();) {
                return mapper.apply(resultSet);
            }
        }
    }

    @Override
    public JdbcConnection prepareUpdate(String stmt, StatementPreparer preparer) throws SQLException {
        // Db2 requires closing prepared statements to avoid caching result-set column structures
        try (PreparedStatement statement = createPreparedStatement(stmt)) {
            if (preparer != null) {
                preparer.accept(statement);
            }
            LOGGER.trace("Executing statement '{}'", stmt);
            statement.execute();
        }
        return this;
    }

    @Override
    public JdbcConnection prepareQuery(String preparedQueryString, List<?> parameters,
                                       ParameterResultSetConsumer resultConsumer)
            throws SQLException {
        // Db2 requires closing prepared statements to avoid caching result-set column structures
        try (PreparedStatement statement = createPreparedStatement(preparedQueryString)) {
            int index = 1;
            for (final Object parameter : parameters) {
                statement.setObject(index++, parameter);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultConsumer != null) {
                    resultConsumer.accept(parameters, resultSet);
                }
            }
        }
        return this;
    }

    @Override
    public TableId createTableId(String databaseName, String schemaName, String tableName) {
        return new TableId(null, schemaName, tableName);
    }

    public boolean validateLogPosition(Partition partition, OffsetContext offset, CommonConnectorConfig config) {

        final Lsn storedLsn = ((Db2OffsetContext) offset).getChangePosition().getCommitLsn();

        String oldestFirstChangeQuery = String.format("SELECT min(RESTART_SEQ) FROM %s.IBMSNAP_CAPMON;", connectorConfig.getCdcControlSchema());

        try {
            final String oldestScn = singleOptionalValue(oldestFirstChangeQuery, rs -> rs.getString(1));

            if (oldestScn == null) {
                return false;
            }

            LOGGER.trace("Oldest SCN in logs is '{}'", oldestScn);
            return storedLsn == null || Lsn.valueOf(oldestScn).compareTo(storedLsn) < 0;
        }
        catch (SQLException e) {
            throw new DebeziumException("Unable to get last available log position", e);
        }
    }

    public <T> T singleOptionalValue(String query, ResultSetExtractor<T> extractor) throws SQLException {
        return queryAndMap(query, rs -> rs.next() ? extractor.apply(rs) : null);
    }

    private PreparedStatement createPreparedStatement(String query) {
        try {
            LOGGER.trace("Creating prepared statement '{}'", query);
            return connection().prepareStatement(query);
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    private void closePreparedStatement(PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            }
            catch (SQLException e) {
                // ignored
            }
        }
    }
}
