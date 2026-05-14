/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2.util;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.connector.db2.Lsn;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Testing;

/**
 * @author Horia Chiorean (hchiorea@redhat.com), Luis Garcés-Erice
 */
public class TestHelper {

    public static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    public static final String TEST_DATABASE = "testdb";
    public static final int WAIT_FOR_CDC = 3 * 5000;
    public static final String CONTROL_TABLE_SCHEMA_NAME = "ASNCDC";
    public static final String PRUNE_SET_TABLE_NAME = "IBMSNAP_PRUNE_SET";
    public static final String PRUNE_CTL_TABLE_NAME = "IBMSNAP_PRUNCNTL";
    public static final String PRUNE_CAP_TRACE_TABLE_NAME = "IBMSNAP_CAPTRACE";
    public static final String REGISTER_CTL_TABLE_NAME = "IBMSNAP_REGISTER";
    public static final String CAP_PARAMS_TABLE_NAME = "IBMSNAP_CAPPARMS";
    public static final String CAP_SIGNAL_TABLE_NAME = "IBMSNAP_SIGNAL";
    public static final String LSN_ZERO_STRING = "0x0000000000000000";
    public static final Lsn LSN_FROM_ZERO_STRING = Lsn.valueOf(LSN_ZERO_STRING);
    public static final String MAP_KEY_CHANGE_TABLE_OWNER = "MAP_KEY_CHANGE_TABLE_OWNER";
    public static final String MAP_KEY_CHANGE_TABLE_NAME = "MAP_KEY_CHANGE_TABLE_NAME";
    public static final String MAP_KEY_PRUNECTL_MAP_ID = "MAP_KEY_PRUNECTL_MAP_ID";
    private static int currentIdForTestEvents = 1000;

    /**
     * Key for schema parameter used to store a source column's type name.
     */
    public static final String TYPE_NAME_PARAMETER_KEY = "__debezium.source.column.type";

    /**
     * Key for schema parameter used to store a source column's type length.
     */
    public static final String TYPE_LENGTH_PARAMETER_KEY = "__debezium.source.column.length";

    /**
     * Key for schema parameter used to store a source column's type scale.
     */
    public static final String TYPE_SCALE_PARAMETER_KEY = "__debezium.source.column.scale";

    private static final String STATEMENTS_TABLE_PLACEHOLDER = "#";
    private static final String STATEMENTS_SCHEMA_PLACEHOLDER = "@";

    private static final String ENABLE_DB_CDC = "VALUES ASNCDC.ASNCDCSERVICES('start','asncdc')";
    private static final String DISABLE_DB_CDC = "VALUES ASNCDC.ASNCDCSERVICES('stop','asncdc')";
    private static final String STATUS_DB_CDC = "VALUES ASNCDC.ASNCDCSERVICES('status','asncdc')";
    private static final String ENABLE_TABLE_CDC = "CALL ASNCDC.ADDTABLE('@', '#' )";
    private static final String DISABLE_TABLE_CDC = "CALL ASNCDC.REMOVETABLE('@', '#' )";
    private static final String RESTART_ASN_CDC = "VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc')";

    private static Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    public static JdbcConfiguration adminJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, "testdb")
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 50000)
                .withDefault(JdbcConfiguration.USER, "db2inst1")
                .withDefault(JdbcConfiguration.PASSWORD, "admin")
                .build();
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, TEST_DATABASE)
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 50000)
                .withDefault(JdbcConfiguration.USER, "db2inst1")
                .withDefault(JdbcConfiguration.PASSWORD, "admin")
                .build();
    }

    /**
     * Returns a default configuration suitable for most test cases. Can be amended/overridden in individual tests as
     * needed.
     */
    public static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(ConfigurationNames.DATABASE_CONFIG_PREFIX + field, value));

        return builder.with(CommonConnectorConfig.TOPIC_PREFIX, "testdb")
                .with(Db2ConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(FileSchemaHistory.FILE_PATH, DB_HISTORY_PATH)
                .with(Db2ConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    public static Db2Connection adminConnection() {
        return new Db2Connection(new Db2ConnectorConfig(defaultConfig().build()));
    }

    public static Db2Connection testConnection() {
        return new Db2Connection(new Db2ConnectorConfig(defaultConfig().build()));
    }

    /**
     * Enables CDC for a given database, if not already enabled.
     *
     * @throws SQLException
     *             if anything unexpected fails
     */
    public static void enableDbCdc(Db2Connection connection) throws SQLException {
        LOGGER.info("Enabling database CDC");
        connection.execute(ENABLE_DB_CDC);
        try (Statement stmt = connection.connection().createStatement()) {
            AtomicInteger count = new AtomicInteger();
            try {
                Awaitility.await()
                        .atMost(5, TimeUnit.MINUTES)
                        .pollInterval(Duration.ofSeconds(1))
                        .until(() -> {
                            count.incrementAndGet();
                            try (ResultSet rs = stmt.executeQuery(STATUS_DB_CDC)) {
                                if (rs.next()) {
                                    Clob clob = rs.getClob(1);
                                    String test = clob.getSubString(1, (int) clob.length());
                                    LOGGER.debug("Checking DB CDC Status: '{}'", test);
                                    if (test.contains("is doing work")) {
                                        return true;
                                    }
                                    else if (test.contains("The command was not processed")) {
                                        connection.execute(ENABLE_DB_CDC);
                                    }
                                }
                                return false;
                            }
                        });
            }
            catch (ConditionTimeoutException e) {
                throw new SQLException("ASNCAP server did not start in 5 minutes over %d attempts.".formatted(count.get()), e);
            }
        }
        LOGGER.info("Database CDC enabled.");
    }

    /**
     * Disables CDC for a given database, if not already disabled.
     *
     * @throws SQLException
     *             if anything unexpected fails
     */
    public static void disableDbCdc(Db2Connection connection) throws SQLException {
        connection.execute(DISABLE_DB_CDC);
    }

    /**
     * Enables CDC for a table if not already enabled and generates the wrapper
     * functions for that table.
     *
     * @param name
     *            the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void enableTableCdc(Db2Connection connection, String name) throws SQLException {
        enableTableCdc(connection, "DB2INST1", name);
    }

    /**
     * Enables CDC for a table if not already enabled and generates the wrapper
     * functions for that table.
     *
     * @param schemaName
     *            the name of the schema, may not be {@code null}
     * @param tableName
     *            the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void enableTableCdc(Db2Connection connection, String schemaName, String tableName) throws SQLException {
        Objects.requireNonNull(schemaName);
        Objects.requireNonNull(tableName);
        String enableCdcForTableStmt = ENABLE_TABLE_CDC.replace(STATEMENTS_SCHEMA_PLACEHOLDER, schemaName).replace(STATEMENTS_TABLE_PLACEHOLDER, tableName);
        connection.execute(enableCdcForTableStmt);

        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER  = '" + schemaName + "' AND SOURCE_TABLE = '" + tableName + "'");
        connection.execute(RESTART_ASN_CDC);
    }

    /**
     * Disables CDC for a table for which it was enabled before.
     *
     * @param name
     *            the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void disableTableCdc(Db2Connection connection, String name) throws SQLException {
        disableTableCdc(connection, "DB2INST1", name);
    }

    /**
     * Disables CDC for a table for which it was enabled before.
     *
     * @param schemaName
     *            the name of the schema, may not be {@code null}
     * @param tableName
     *            the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void disableTableCdc(Db2Connection connection, String schemaName, String tableName) throws SQLException {
        Objects.requireNonNull(schemaName);
        Objects.requireNonNull(tableName);
        String disableCdcForTableStmt = DISABLE_TABLE_CDC.replace(STATEMENTS_SCHEMA_PLACEHOLDER, schemaName).replace(STATEMENTS_TABLE_PLACEHOLDER, tableName);
        connection.execute(disableCdcForTableStmt);
        connection.execute(RESTART_ASN_CDC);
    }

    public static void waitForSnapshotToBeCompleted() throws InterruptedException {
        int waitForSeconds = 60;
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        final Metronome metronome = Metronome.sleeper(Duration.ofSeconds(1), Clock.system());

        while (true) {
            if (waitForSeconds-- <= 0) {
                Assertions.fail("Snapshot was not completed on time");
            }
            try {
                final boolean completed = (boolean) mbeanServer.getAttribute(new ObjectName("debezium.db2_server:type=connector-metrics,context=snapshot,server=testdb"),
                        "SnapshotCompleted");
                if (completed) {
                    break;
                }
            }
            catch (InstanceNotFoundException e) {
                // Metrics has not started yet
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
            metronome.pause();
        }
    }

    public static void refreshAndWait(Db2Connection connection) throws SQLException {
        connection.execute(RESTART_ASN_CDC);
        waitForCDC();
    }

    public static String getCdcTableName(Db2Connection connection, String sourceTable) throws SQLException {
        return connection.queryAndMap("SELECT CD_OWNER, CD_TABLE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER='DB2INST1' AND SOURCE_TABLE = '" +
                sourceTable + "'", rs -> rs.next() ? rs.getString(1) + "." + rs.getString(2) : null);
    }

    public static void activeTable(Db2Connection connection, String tableName) throws SQLException {
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1' AND SOURCE_TABLE = '" + tableName + "'");
        TestHelper.refreshAndWait(connection);
    }

    public static void deactivateTable(Db2Connection connection, String tableName) throws SQLException {
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'I' WHERE SOURCE_OWNER = 'DB2INST1' AND SOURCE_TABLE = '" + tableName + "'");
        TestHelper.refreshAndWait(connection);
    }

    public static void waitForCDC() {
        try {
            Thread.sleep(WAIT_FOR_CDC);
        }
        catch (Exception e) {

        }
    }

    public static void dropAllTables() throws SQLException {
        try (Db2Connection connection = testConnection()) {
            LOGGER.info("Attempting to drop all tables (if exists)");
            connection.query("SELECT TABNAME FROM syscat.tables WHERE TABSCHEMA = 'DB2INST1'", rs -> {
                while (rs.next()) {
                    final String tableName = rs.getString(1);
                    LOGGER.info("Disabling CDC for table {}", tableName);
                    disableTableCdc(connection, "DB2INST1", tableName);
                    LOGGER.warn("Dropping table {}", tableName);
                    connection.execute("DROP TABLE IF EXISTS " + tableName);
                }
            });
        }
    }

    public static Lsn getLsnSyncPointForPruneSet(final String applyQual,
                                                 final String setName,
                                                 final String targetSystem) {
        {
            final Db2Connection conn = testConnection();
            LOGGER.info("Getting the current LSN for the prune set defined by setName {} " +
                    "target_server {} apply_qual {}", setName, targetSystem, applyQual);
            AtomicReference<byte[]> lsnBytes = new AtomicReference<>();
            try {
                conn.prepareQuery(
                        "SELECT SYNCHPOINT " +
                                "FROM " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_SET_TABLE_NAME + " " +
                                "WHERE SET_NAME = ? " +
                                "AND TARGET_SERVER = ? " +
                                "AND APPLY_QUAL = ?",
                        ps -> {
                            ps.setString(1, setName);
                            ps.setString(2, targetSystem);
                            ps.setString(3, applyQual);
                        },
                        rs -> {
                            int numRowsFound = 0;
                            while (rs.next()) {
                                numRowsFound++;
                                LOGGER.info("Found {} rows in the prune set table", numRowsFound);
                                lsnBytes.set(rs.getBytes("SYNCHPOINT"));
                            }
                            LOGGER.info("Done reporting the contents of Prune set with {} rows.", numRowsFound);
                        });
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return Lsn.valueOf(lsnBytes.get());
        }
    }

    public static Lsn getLastChangeLsnForChangeTable(
                                                     final String changeTableOwner,
                                                     final String changeTableName) {

        final Db2Connection conn = testConnection();
        LOGGER.info("Getting the last change LSN for change table {}.{}", changeTableOwner, changeTableName);
        AtomicReference<byte[]> lsnBytes = new AtomicReference<>();
        try {
            conn.prepareQuery(
                    "SELECT IBMSNAP_COMMITSEQ " +
                            "FROM " + changeTableOwner + "." + changeTableName + " " +
                            "ORDER BY IBMSNAP_COMMITSEQ DESC LIMIT 1",
                    ps -> {
                    },
                    rs -> {
                        while (rs.next()) {
                            lsnBytes.set(rs.getBytes("IBMSNAP_COMMITSEQ"));
                        }
                    });
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Lsn.valueOf(lsnBytes.get());
    }

    public static int getSizeOfChangeTable(
                                           final String changeTableOwner,
                                           final String changeTableName) {

        final AtomicReference<Integer> size = new AtomicReference<>();
        final Db2Connection conn = testConnection();
        LOGGER.info("Getting size of change table {}.{}", changeTableOwner, changeTableName);
        try {
            conn.prepareQuery(
                    "SELECT COUNT(*) " +
                            "FROM " + changeTableOwner + "." + changeTableName + " ",
                    ps -> {
                    },
                    rs -> {
                        if (rs.next()) {
                            size.set(rs.getInt(1));
                        }
                        else {
                            size.set(0);
                        }
                        LOGGER.info("Size of table {} is {}", changeTableOwner + "." + changeTableName, size.get());
                    });
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return size.get();
    }

    public static void getChangeTablesForPhysicalTableFromRegistrationTable(
                                                                            final String sourceSchemaName, final String sourceTableName,
                                                                            final Map<String, String> pruneControlAndCapMap) {
        final AtomicReference<String> physChangeOwner = new AtomicReference<>();
        final AtomicReference<String> physChangeTable = new AtomicReference<>();
        final JdbcConnection conn = testConnection();
        LOGGER.info("Getting Physical Change Table from Registration Table");
        try {
            conn.prepareQuery(
                    "SELECT PHYS_CHANGE_OWNER, PHYS_CHANGE_TABLE \n" +
                            "FROM " + CONTROL_TABLE_SCHEMA_NAME + "." + REGISTER_CTL_TABLE_NAME + "\n" +
                            "WHERE SOURCE_OWNER = ? \n" +
                            "AND SOURCE_TABLE = ?",
                    ps -> {
                        ps.setString(1, sourceSchemaName);
                        ps.setString(2, sourceTableName);
                    },
                    rs -> {
                        while (rs.next()) {
                            physChangeOwner.set(rs.getString("PHYS_CHANGE_OWNER"));
                            physChangeTable.set(rs.getString("PHYS_CHANGE_TABLE"));
                        }
                    });
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        LOGGER.info("PhysChangeOwner: {}, PhysChangeTable: {}", physChangeOwner, physChangeTable);
        pruneControlAndCapMap.put(MAP_KEY_CHANGE_TABLE_OWNER, physChangeOwner.get());
        pruneControlAndCapMap.put(MAP_KEY_CHANGE_TABLE_NAME, physChangeTable.get());
    }

    public static void checkAndCorrectPhysicalChangeTablesBetweenRegistrationAndPrunectlForTable(
                                                                                                 final String sourceSchemaName,
                                                                                                 final String sourceTableName,
                                                                                                 final Map<String, String> pruneControlAndCapMap) {
        final JdbcConnection conn = testConnection();
        LOGGER.info("Updating the prunectl table to use the correct change table names as found on the registration tables");
        if (!pruneControlAndCapMap.containsKey(MAP_KEY_CHANGE_TABLE_OWNER) || !pruneControlAndCapMap.containsKey(MAP_KEY_CHANGE_TABLE_NAME)) {
            getChangeTablesForPhysicalTableFromRegistrationTable(sourceSchemaName, sourceTableName, pruneControlAndCapMap);
        }
        final String physChangeOwner = pruneControlAndCapMap.get(MAP_KEY_CHANGE_TABLE_OWNER);
        final String physChangeTable = pruneControlAndCapMap.get(MAP_KEY_CHANGE_TABLE_NAME);
        try {
            conn.prepareUpdate(
                    "UPDATE " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_CTL_TABLE_NAME + " " +
                            "SET PHYS_CHANGE_OWNER = ?, PHYS_CHANGE_TABLE = ? " +
                            "WHERE SOURCE_OWNER = ? " +
                            "AND SOURCE_TABLE = ?",
                    ps -> {
                        ps.setString(1, physChangeOwner);
                        ps.setString(2, physChangeTable);
                        ps.setString(3, sourceSchemaName);
                        ps.setString(4, sourceTableName);
                    });
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void getPruneSetRecordForPruneSet(final String targetServer, final String applyQual, final String setName) {
        try {
            JdbcConnection conn = testConnection();
            LOGGER.info("Confirming Prune Set Data is in Place");
            final AtomicReference<Integer> numRecordsInSetTable = new AtomicReference<>();
            final AtomicReference<Integer> numRecordsInSetTableForSpecificSet = new AtomicReference<>();
            conn.prepareQuery(
                    "SELECT ips.TARGET_SERVER, ips.SYNCHPOINT, ips.SET_NAME, ips.APPLY_QUAL " +
                            "FROM " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_SET_TABLE_NAME + " ips ",
                    s -> {
                    },
                    rs -> {
                        int rowsFound = 0;
                        while (rs.next()) {
                            rowsFound++;
                            LOGGER.info("Prune Set record found. \nTargetServer: {}\nApplyQual: {}\nSetName: {}\n" +
                                    "SynchPoint: {}",
                                    rs.getString("TARGET_SERVER"),
                                    rs.getString("APPLY_QUAL"),
                                    rs.getString("SET_NAME"),
                                    rs.getBytes("SYNCHPOINT"));
                        }
                        LOGGER.info("Done reporting the the contents of Prune set with {} rows.", rowsFound);
                        numRecordsInSetTable.set(rowsFound);
                    });
            LOGGER.info("Confirming Prune Set Data is in Place");
            conn.prepareQuery(
                    "SELECT ips.TARGET_SERVER, ips.SYNCHPOINT, ips.SET_NAME, ips.APPLY_QUAL " +
                            "FROM " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_SET_TABLE_NAME + " ips " +
                            "WHERE APPLY_QUAL = ? " +
                            "AND SET_NAME = ?",
                    s -> {
                        s.setString(1, applyQual);
                        s.setString(2, setName);
                    },
                    rs -> {
                        int rowsFound = 0;
                        while (rs.next()) {
                            rowsFound++;
                            LOGGER.info("Prune Set record found for specific match. \nTargetServer: {}\nApplyQual: {}\nSetName: {}\n" +
                                    "SynchPoint: {}",
                                    rs.getString("TARGET_SERVER"),
                                    rs.getString("APPLY_QUAL"),
                                    rs.getString("SET_NAME"),
                                    rs.getBytes("SYNCHPOINT"));
                        }
                        LOGGER.info("Done reporting the the contents of Prune set with {} rows.", rowsFound);
                        numRecordsInSetTableForSpecificSet.set(rowsFound);
                    });
            LOGGER.info("Rows on prune set table: {}", numRecordsInSetTable.get());
            LOGGER.info("Rows on prune set matching prune set information: {}", numRecordsInSetTableForSpecificSet.get());
            if (numRecordsInSetTableForSpecificSet.get() == 0) {
                throw new IllegalStateException("No matching rows found on the pruneSet table for the provided information.");
            }
            if (numRecordsInSetTableForSpecificSet.get() > 1) {
                throw new IllegalStateException("More than one target system found ");
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void insertRecordToPruneSetTable(final String targetServer, final String applyQual, final String setName) {
        JdbcConnection conn = testConnection();
        LOGGER.info("Inserting record to Prune Set Table");
        try {
            conn.prepareUpdate(
                    "INSERT INTO " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_SET_TABLE_NAME + " " +
                            "(TARGET_SERVER, APPLY_QUAL, SET_NAME, SYNCHTIME, SYNCHPOINT) " +
                            "VALUES (?, ?, ?, ?, ?)",
                    ps -> {
                        ps.setString(1, targetServer);
                        ps.setString(2, applyQual);
                        ps.setString(3, setName);
                        ps.setTimestamp(4, Timestamp.from(Instant.EPOCH));
                        ps.setBytes(5, LSN_FROM_ZERO_STRING.getBinary());
                    });
            conn.commit();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setOldSynchpointOnRegistrationTable(final String sourceSchemaName, final String sourceTableName) {
        LOGGER.info("Setting Old Synchpoint on Registration Table");
        JdbcConnection conn = testConnection();
        try {
            conn.prepareUpdate(
                    "UPDATE " + CONTROL_TABLE_SCHEMA_NAME + "." + REGISTER_CTL_TABLE_NAME + " " +
                            "SET CD_OLD_SYNCHPOINT = ? " +
                            "WHERE SOURCE_OWNER = ? AND SOURCE_TABLE = ?",
                    ps -> {
                        ps.setBytes(1, LSN_FROM_ZERO_STRING.getBinary());
                        ps.setString(2, sourceSchemaName);
                        ps.setString(3, sourceTableName);

                    });
            conn.commit();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Lsn getOldSynchpointOnRegistrationTable(final String sourceSchemaName, final String sourceTableName) {
        LOGGER.info("Getting Old Synchpoint on Registration Table");
        final AtomicReference<byte[]> snapshotBytes = new AtomicReference<>();
        JdbcConnection conn = testConnection();
        try {
            conn.prepareQuery(
                    "SELECT CD_OLD_SYNCHPOINT " +
                            "FROM " + CONTROL_TABLE_SCHEMA_NAME + "." + REGISTER_CTL_TABLE_NAME + " " +
                            "WHERE SOURCE_OWNER = ? AND SOURCE_TABLE = ?",
                    ps -> {
                        ps.setString(1, sourceSchemaName);
                        ps.setString(2, sourceTableName);
                    },
                    rs -> {
                        while (rs.next()) {
                            snapshotBytes.set(rs.getBytes("CD_OLD_SYNCHPOINT"));
                        }
                    });
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Lsn.valueOf(snapshotBytes.get());
    }

    public static void createDBRecords(final int numToInsert) throws SQLException {

        JdbcConnection connection = testConnection();
        for (int j = 0; j < numToInsert; j++) {
            LOGGER.info("Inserting record with id {}", currentIdForTestEvents);
            connection.execute(
                    "INSERT INTO tablea VALUES(" + currentIdForTestEvents + ", 'b')");
            LOGGER.info("Inserted record with id {}", currentIdForTestEvents);
            connection.commit();
            currentIdForTestEvents++;
        }
    }

    public static void readCapTrace(int numToRead) throws SQLException {
        JdbcConnection conn = testConnection();
        LOGGER.info("Reading capture trace table to see activity.");
        conn.prepareQuery(
                "SELECT OPERATION, TRACE_TIME, DESCRIPTION " +
                        "FROM " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_CAP_TRACE_TABLE_NAME + " " +
                        "ORDER BY TRACE_TIME DESC LIMIT ?",
                s -> {
                    s.setInt(1, numToRead);
                },
                rs -> {
                    int rowsFound = 0;
                    while (rs.next()) {
                        rowsFound++;
                        LOGGER.info("Cap Trace: Operation: {} TRACE_TIME: {}, DESCRIPTION: {}",
                                rs.getString("OPERATION"),
                                rs.getString("TRACE_TIME"),
                                rs.getString("DESCRIPTION"));
                    }
                    LOGGER.info("Done reporting the the contents of Prune set with {} rows.", rowsFound);
                });
    }

    public static String getPruneCtlRecordsForTable(final String sourceTableSchemaName, final String sourceTableName,
                                                    final String targetServer, final String applyQual, final String setName,
                                                    final Map<String, String> pruneControlAndCapMap) {
        JdbcConnection conn = testConnection();
        LOGGER.info("Getting PruneCtl Information");
        final AtomicReference<String> mapId = new AtomicReference<>();

        try {
            conn.prepareQuery(
                    "SELECT TARGET_SERVER, TARGET_OWNER, TARGET_TABLE, SYNCHTIME, SYNCHPOINT, " +
                            "SOURCE_OWNER, SOURCE_TABLE, SOURCE_VIEW_QUAL, APPLY_QUAL, SET_NAME, " +
                            "CNTL_SERVER, TARGET_STRUCTURE, CNTL_ALIAS, PHYS_CHANGE_OWNER, " +
                            "PHYS_CHANGE_TABLE, MAP_ID " +
                            "FROM " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_CTL_TABLE_NAME + " " +
                            "WHERE SOURCE_OWNER = ? " +
                            "AND SOURCE_TABLE = ? ",
                    ps -> {
                        ps.setString(1, sourceTableSchemaName);
                        ps.setString(2, sourceTableName);
                    },
                    rs -> {
                        while (rs.next()) {
                            LOGGER.info("Prune Data record found on non-specific query on prunectl. \nTargetServer: {}\nApplyQual: {}\nSetName: {}\n" +
                                    "SynchPoint: {}\nPhysChangeOwner: {}\nPhysChangeTable: {}",
                                    rs.getString("TARGET_SERVER"),
                                    rs.getString("APPLY_QUAL"),
                                    rs.getString("SET_NAME"),
                                    rs.getString("SYNCHPOINT"),
                                    rs.getString("PHYS_CHANGE_OWNER"),
                                    rs.getString("PHYS_CHANGE_TABLE"));
                        }
                        LOGGER.info("Done reporting the the contents of Prune ctl without prune set filters.");
                    });
            conn.prepareQuery(
                    "SELECT TARGET_SERVER, TARGET_OWNER, TARGET_TABLE, SYNCHTIME, SYNCHPOINT, " +
                            "SOURCE_OWNER, SOURCE_TABLE, SOURCE_VIEW_QUAL, APPLY_QUAL, SET_NAME, " +
                            "CNTL_SERVER, TARGET_STRUCTURE, CNTL_ALIAS, PHYS_CHANGE_OWNER, " +
                            "PHYS_CHANGE_TABLE, MAP_ID " +
                            "FROM " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_CTL_TABLE_NAME + " " +
                            "WHERE SOURCE_OWNER = ? " +
                            "AND SOURCE_TABLE = ? " +
                            "AND APPLY_QUAL = ? " +
                            "AND SET_NAME = ? " +
                            "AND TARGET_SERVER = ? " +
                            "LIMIT 1",
                    ps -> {
                        ps.setString(1, sourceTableSchemaName);
                        ps.setString(2, sourceTableName);
                        ps.setString(3, applyQual);
                        ps.setString(4, setName);
                        ps.setString(5, targetServer);
                    },
                    rs -> {
                        int numFoundForSpecificQuery = 0;
                        while (rs.next()) {
                            numFoundForSpecificQuery++;
                            LOGGER.info("Prune Data record found on Prunectl. \nTargetServer: {}\nApplyQual: {}\nSetName: {}\n" +
                                    "SynchPoint: {}\nPhysChangeOwner: {}\nPhysChangeTable: {}",
                                    rs.getString("TARGET_SERVER"),
                                    rs.getString("APPLY_QUAL"),
                                    rs.getString("SET_NAME"),
                                    rs.getString("SYNCHPOINT"),
                                    rs.getString("PHYS_CHANGE_OWNER"),
                                    rs.getString("PHYS_CHANGE_TABLE"));
                            mapId.set(rs.getString("MAP_ID"));
                        }
                        LOGGER.info("Done reporting the the contents of Prune ctl with {} records found.", numFoundForSpecificQuery);
                        if (numFoundForSpecificQuery == 0) {
                            LOGGER.error("No records for table {}.{}, using applyQual {}, targetServer {} setName {}",
                                    sourceTableSchemaName, sourceTableName, applyQual, targetServer, setName);
                            throw new IllegalStateException("The database does not currently have matching records for prune " +
                                    "on both the registration and pruneCtl tables for the expected configuration.");
                        }
                    });
            pruneControlAndCapMap.put(MAP_KEY_PRUNECTL_MAP_ID, mapId.get());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return mapId.get();
    }

    public static void signalCapRestartForMapId(final String mapId) {
        JdbcConnection conn = testConnection();
        LOGGER.info("Restarting Cap for MapId: {}");
        try {
            conn.prepareUpdate(
                    "INSERT INTO " + CONTROL_TABLE_SCHEMA_NAME + "." + CAP_SIGNAL_TABLE_NAME + "\n" +
                            "(SIGNAL_TYPE, SIGNAL_SUBTYPE, SIGNAL_INPUT_IN, SIGNAL_STATE)\n" +
                            "VALUES ('CMD', 'CAPSTART', ?, 'P')",
                    ps -> {
                        ps.setString(1, mapId);

                    });
            conn.commit();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        LOGGER.info("Sent signal record for restarting Cap for MapId: {}", mapId);
    }

    public static void setCapParamsPruneInterval(int pruneIntervalInSec) {
        {
            final Db2Connection conn = testConnection();
            LOGGER.info("Setting PruneInterval to {} seconds", pruneIntervalInSec);
            try {
                conn.prepareUpdate(
                        "UPDATE " + CONTROL_TABLE_SCHEMA_NAME + "." + CAP_PARAMS_TABLE_NAME + " " +
                                "SET PRUNE_INTERVAL = ?",
                        ps -> {
                            ps.setInt(1, pruneIntervalInSec);
                        });
                conn.commit();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /*
     * Table must be on the registration table, with a non-null CD_OLD_SYNCHPOINT.
     * It must then be on the prunectl tale, where the ts, aq, and sn will be checked, and a mapId will be checked
     * It must then be on the pruneSet table, where the ts, aq, and sn will be checked, and a synctime be identified.
     * The pruneSet table will then be checked for anything with the same setName and applyQual but a different targetServer.
     */
    public static void validatePruneReadinessForTable(final String sourceTableSchema, final String sourceTableName,
                                                      final String targetServer, final String applyQual, final String setName) {
        final Map<String, String> validationMap = new HashMap<String, String>();
        LOGGER.info("___________________" + " Prune Setup Validation " + "___________________");
        getChangeTablesForPhysicalTableFromRegistrationTable(sourceTableSchema, sourceTableName, validationMap);
        final Lsn currentRegisteredCdOldSynchpoint = TestHelper.getOldSynchpointOnRegistrationTable(sourceTableSchema, sourceTableName);
        if (currentRegisteredCdOldSynchpoint.equals(Lsn.NULL) || currentRegisteredCdOldSynchpoint == null) {
            LOGGER.error("The LSN on the registration table for {}.{} is null.  " +
                    "This will mark it inactive and it won't be pruned", sourceTableSchema, sourceTableName);
            throw new IllegalStateException("The LSN on the registration table for {}.{} is null. This will mark it inactive and it won't be pruned");
        }
        final String changeSchemaName = validationMap.get(MAP_KEY_CHANGE_TABLE_OWNER);
        final String changeTableName = validationMap.get(MAP_KEY_CHANGE_TABLE_NAME);
        LOGGER.info("Change Table Name: {}.{}", changeSchemaName, changeTableName);

        getPruneCtlRecordsForTable(sourceTableSchema, sourceTableName, targetServer, applyQual, setName, validationMap);
        final String mapId = validationMap.get(MAP_KEY_PRUNECTL_MAP_ID);
        LOGGER.info("Prunectl MapId for this prune set: {}", mapId);

        getPruneSetRecordForPruneSet(targetServer, applyQual, setName);

    }
}
