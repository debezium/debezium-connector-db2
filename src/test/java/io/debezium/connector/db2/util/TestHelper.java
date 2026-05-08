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
    public static final String REGISTER_CTL_TABLE_NAME = "IBMSNAP_REGISTER";
    public static final String CAP_PARAMS_TABLE_NAME = "IBMSNAP_CAPPARMS";
    public static final String CAP_SIGNAL_TABLE_NAME = "IBMSNAP_SIGNAL";
    public static final String LSN_ZERO_STRING = "0x0000000000000000";
    public static final Lsn LSN_FROM_ZERO_STRING = Lsn.valueOf(LSN_ZERO_STRING);
    public static final String MAP_KEY_CHANGE_TABLE_OWNER = "MAP_KEY_CHANGE_TABLE_OWNER";
    public static final String MAP_KEY_CHANGE_TABLE_NAME = "MAP_KEY_CHANGE_TABLE_NAME";

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

    public static Map<String, String> prepareToPruneControlAndReinitializeCap(
                                                                              final String targetTableSchemaName,
                                                                              final String targetTableName,
                                                                              final String applyQual,
                                                                              final String setName,
                                                                              final String targetSystem,
                                                                              final int pruneIntervalInSec) {

        final Map<String, String> pruneControlAndReinitializeCapMap = new HashMap<>();
        final String mapId = "1000";

        final AtomicReference<String> physChangeOwner = new AtomicReference<>();
        final AtomicReference<String> physChangeTable = new AtomicReference<>();

        try {
            final Db2Connection conn = testConnection();
            { // CapParams PruneInterval Block
                LOGGER.info("Setting PruneInterval to {} seconds", pruneIntervalInSec);
                conn.prepareUpdate(
                        "UPDATE " + CONTROL_TABLE_SCHEMA_NAME + "." + CAP_PARAMS_TABLE_NAME + "\n" +
                                "SET PRUNE_INTERVAL = ?",
                        ps -> {
                            ps.setInt(1, pruneIntervalInSec);
                            ps.execute();
                        });
                conn.commit();
            } // CapParams PruneInterval Block
            { // Get Physical Change Table from Register Block
                LOGGER.info("Getting Physical Change Table from Register Block");
                conn.prepareQuery(
                        "SELECT PHYS_CHANGE_OWNER, PHYS_CHANGE_TABLE \n" +
                                "FROM " + CONTROL_TABLE_SCHEMA_NAME + "." + REGISTER_CTL_TABLE_NAME + "\n" +
                                "WHERE SOURCE_OWNER = ? \n" +
                                "AND SOURCE_TABLE = ?",
                        ps -> {
                            ps.setString(1, targetTableSchemaName);
                            ps.setString(2, targetTableName);
                        },
                        rs -> {
                            while (rs.next()) {
                                physChangeOwner.set(rs.getString("PHYS_CHANGE_OWNER"));
                                physChangeTable.set(rs.getString("PHYS_CHANGE_TABLE"));
                            }
                        });
                LOGGER.info("PhysChangeOwner: {}, PhysChangeTable: {}", physChangeOwner, physChangeTable);
                pruneControlAndReinitializeCapMap.put(MAP_KEY_CHANGE_TABLE_OWNER, physChangeOwner.get());
                pruneControlAndReinitializeCapMap.put(MAP_KEY_CHANGE_TABLE_NAME, physChangeTable.get());
            } // Get Physical Change Table from Register Block
            { // Truncate PruneCtl Block
                LOGGER.info("Truncating PruneCtl Block");
                conn.execute("TRUNCATE TABLE " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_CTL_TABLE_NAME + " IMMEDIATE");
                conn.commit();
            } // Truncate PruneCtl Block
            { // PruneCtl Block
                LOGGER.info("Setting PruneCtl Block");

                conn.prepareUpdate(
                        "INSERT INTO " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_CTL_TABLE_NAME + " (" +
                                "TARGET_SERVER, TARGET_OWNER, TARGET_TABLE, SYNCHTIME, SYNCHPOINT, " +
                                "SOURCE_OWNER, SOURCE_TABLE, SOURCE_VIEW_QUAL, APPLY_QUAL, SET_NAME, " +
                                "CNTL_SERVER, TARGET_STRUCTURE, CNTL_ALIAS, PHYS_CHANGE_OWNER, " +
                                "PHYS_CHANGE_TABLE, MAP_ID) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        ps -> {
                            ps.setString(1, targetSystem);
                            ps.setString(2, targetTableSchemaName);
                            ps.setString(3, targetTableName);
                            ps.setTimestamp(4, Timestamp.from(Instant.EPOCH));
                            ps.setBytes(5, LSN_FROM_ZERO_STRING.getBinary());
                            ps.setString(6, targetTableSchemaName);
                            ps.setString(7, targetTableName);
                            ps.setShort(8, Short.parseShort("0"));
                            ps.setString(9, applyQual);
                            ps.setString(10, setName);
                            ps.setString(11, "CTL_SER");
                            ps.setShort(12, Short.parseShort("8"));
                            ps.setString(13, "CTLALAS");
                            ps.setString(14, physChangeOwner.get());
                            ps.setString(15, physChangeTable.get());
                            ps.setString(16, mapId);
                            ps.execute();
                        });
                conn.commit();
            } // PruneCtl Block
            { // Truncate PruneSet Block
                LOGGER.info("Truncating PruneSet Block");
                conn.execute("TRUNCATE TABLE " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_SET_TABLE_NAME + " IMMEDIATE");
                conn.commit();
            } // Truncate PruneSet Block
            { // Populate PruneSet Block
                LOGGER.info("Populating PruneSet Block");
                conn.prepareUpdate(
                        "INSERT INTO " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_SET_TABLE_NAME + "\n" +
                                "(TARGET_SERVER, APPLY_QUAL, SET_NAME, SYNCHTIME, SYNCHPOINT)\n" +
                                "VALUES(?,?,?,?,?)",
                        ps -> {
                            ps.setString(1, targetSystem);
                            ps.setString(2, applyQual);
                            ps.setString(3, setName);
                            ps.setTimestamp(4, Timestamp.from(Instant.EPOCH));
                            ps.setBytes(5, hexStringToByteArray(LSN_ZERO_STRING));
                            ps.execute();
                        });
                conn.commit();
            } // Populate PruneSet Block
            { // CapSignal - Restart Cap for MapId Block
                LOGGER.info("Restarting Cap for MapId Block");
                conn.prepareUpdate(
                        "INSERT INTO " + CONTROL_TABLE_SCHEMA_NAME + "." + CAP_SIGNAL_TABLE_NAME + "\n" +
                                "(SIGNAL_TYPE, SIGNAL_SUBTYPE, SIGNAL_INPUT_IN, SIGNAL_STATE)\n" +
                                "VALUES ('CMD', 'CAPSTART', ?, 'P')",
                        ps -> {
                            ps.setString(1, mapId);
                            ps.execute();
                        });
                conn.commit();
            } // CapSignal - Restart Cap for MapId Block
            { // Confirm Prune Data is in Place Block
                LOGGER.info("Confirming Prune Data is in Place Block");
                conn.prepareQuery(
                        "SELECT ips.TARGET_SERVER, ips.synchpoint, ips.SET_NAME, ips.TARGET_SERVER, ips.APPLY_QUAL, " +
                                "ip.PHYS_CHANGE_OWNER, ip.PHYS_CHANGE_TABLE \n" +
                                "FROM " + CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_SET_TABLE_NAME + " ips, \n" +
                                CONTROL_TABLE_SCHEMA_NAME + "." + PRUNE_CTL_TABLE_NAME + " ip \n" +
                                "WHERE ips.SET_NAME = ip.SET_NAME = ?\n" +
                                "AND ips.TARGET_SERVER = ip.TARGET_SERVER = ?\n" +
                                "AND ips.APPLY_QUAL = ip.APPLY_QUAL = ?",
                        ps -> {
                            ps.setString(1, setName);
                            ps.setString(2, targetSystem);
                            ps.setString(3, applyQual);
                        },
                        rs -> {
                            while (rs.next()) {
                                LOGGER.info("Prune Data record found. \nTargetServer: {}\nApplyQual: {}\nSetName: {}\n" +
                                        "SynchPoint: {}\nPhysChangeOwner: {}\nPhysChangeTable: {}",
                                        rs.getString("TARGET_SERVER"),
                                        rs.getString("APPLY_QUAL"),
                                        rs.getString("SET_NAME"),
                                        rs.getString("synchpoint"),
                                        rs.getString("PHYS_CHANGE_OWNER"),
                                        rs.getString("PHYS_CHANGE_TABLE"));
                            }
                        });
            } // Confirm Prune Data is in Place Block
        }
        catch (SQLException e) {
            System.err.println("SQL Error: " + e.getSQLState() + " " + e.getMessage());
            e.printStackTrace();
        }
        return pruneControlAndReinitializeCapMap;
    }

    public static Lsn getLsnSyncPointForPruneSet(final String applyQual,
                                                 final String setName,
                                                 final String targetSystem) {
        {
            final Db2Connection conn = testConnection();
            LOGGER.info("Getting the current LSN on the table.");
            AtomicReference<String> lsnString = new AtomicReference<>();
            try {
                conn.prepareQuery(
                        "SELECT SYNCHPOINT \n" +
                                "FROM " + CONTROL_TABLE_SCHEMA_NAME + "." + REGISTER_CTL_TABLE_NAME + "\n" +
                                "WHERE SET_NAME = ?\n" +
                                "AND TARGET_SERVER = ?\n" +
                                "AND APPLY_QUAL = ?",
                        ps -> {
                            ps.setString(1, setName);
                            ps.setString(2, targetSystem);
                            ps.setString(3, applyQual);
                        },
                        rs -> {
                            while (rs.next()) {
                                lsnString.set(rs.getString("SYNCHPOINT"));

                            }
                        });
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return Lsn.valueOf(lsnString.get());
        }
    }

    public static Lsn getLastChangeLsnForChangeTable(
                                                     final String changeTableOwner,
                                                     final String changeTableName) {

        final Db2Connection conn = testConnection();
        LOGGER.info("Getting the last change LSN for {}.{}", changeTableOwner, changeTableName);
        AtomicReference<String> lsnString = new AtomicReference<>();
        try {
            conn.prepareQuery(
                    "SELECT IBMSNAP_COMMITSEQ \n" +
                            "FROM " + changeTableOwner + "." + changeTableName + "\n" +
                            "ORDER BY IBMSNAP_COMMITSEQ DESC LIMIT 1",
                    ps -> {
                    },
                    rs -> {
                        while (rs.next()) {
                            lsnString.set(rs.getString("IBMSNAP_COMMITSEQ"));
                        }
                    });
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Lsn.valueOf(lsnString.get());
    }

    // Helper method to convert hex string to byte array
    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
}
