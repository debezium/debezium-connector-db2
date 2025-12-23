/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import static io.debezium.util.Strings.listOfTrimmed;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.db2.platform.Db2PlatformAdapter;
import io.debezium.connector.db2.platform.LuwPlatform;
import io.debezium.connector.db2.platform.ZOsPlatform;
import io.debezium.document.Document;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.spi.schema.DataCollectionId;

/**
 * The list of configuration options for DB2 connector
 *
 * @author Jiri Pechanec, Luis Garcés-Erice
 */
public class Db2ConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    private static final String DEFAULT_CDC_SCHEMA = "ASNCDC";

    public static final int DEFAULT_QUERY_FETCH_SIZE = 10_000;

    public static final int DEFAULT_STREAMING_QUERY_TIMESPAN_SECONDS = 0; // No limit

    protected static final int DEFAULT_PORT = 50000;

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Performs a snapshot of data and schema upon each connector start.
         */
        ALWAYS("always"),

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector.
         */
        INITIAL("initial"),

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector but does not transition to streaming.
         */
        INITIAL_ONLY("initial_only"),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         * @deprecated to be removed in Debezium 3.0, replaced by {{@link #NO_DATA}}
         */
        SCHEMA_ONLY("schema_only"),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        NO_DATA("no_data"),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the redo log at the current redo log position.
         * This can be used for recovery only if the connector has existing offsets and the schema.history.internal.kafka.topic does not exist (deleted).
         * This recovery option should be used with care as it assumes there have been no schema changes since the connector last stopped,
         * otherwise some events during the gap may be processed with an incorrect schema and corrupted.
         */
        RECOVERY("recovery"),

        /**
         * Perform a snapshot when it is needed.
         */
        WHEN_NEEDED("when_needed"),

        /**
         * Allows over snapshots by setting connectors properties prefixed with 'snapshot.mode.configuration.based'.
         */
        CONFIGURATION_BASED("configuration_based"),

        /**
         * Inject a custom snapshotter, which allows for more control over snapshots.
         */
        CUSTOM("custom");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    /**
     * The set of predefined snapshot locking mode options.
     */
    public enum SnapshotLockingMode implements EnumeratedValue {

        /**
         * This mode will use exclusive lock TABLOCKX
         */
        EXCLUSIVE("exclusive"),

        /**
         * This mode will avoid using ANY table locks during the snapshot process.
         * This mode should be used carefully only when no schema changes are to occur.
         */
        NONE("none"),

        CUSTOM("custom");

        private final String value;

        SnapshotLockingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotLockingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotLockingMode option : SnapshotLockingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotLockingMode parse(String value, String defaultValue) {
            SnapshotLockingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined snapshot isolation mode options.
     *
     * https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.5.0/com.ibm.db2.luw.apdv.java.doc/src/tpc/imjcc_r0052429.html
     */
    public enum SnapshotIsolationMode implements EnumeratedValue {

        /**
         * This mode will block all reads and writes for the entire duration of the snapshot.
         *
         * The connector will execute {@code SELECT * FROM .. WITH (TABLOCKX)}
         */
        EXCLUSIVE("exclusive"),

        /**
         * This mode uses REPEATABLE READ isolation level. This mode will avoid taking any table
         * locks during the snapshot process, except schema snapshot phase where exclusive table
         * locks are acquired for a short period.  Since phantom reads can occur, it does not fully
         * guarantee consistency.
         */
        REPEATABLE_READ("repeatable_read"),

        /**
         * This mode uses READ COMMITTED isolation level. This mode does not take any table locks during
         * the snapshot process. In addition, it does not take any long-lasting row-level locks, like
         * in repeatable read isolation level. Snapshot consistency is not guaranteed.
         */
        READ_COMMITTED("read_committed"),

        /**
         * This mode uses READ UNCOMMITTED isolation level. This mode takes neither table locks nor row-level locks
         * during the snapshot process.  This way other transactions are not affected by initial snapshot process.
         * However, snapshot consistency is not guaranteed.
         */
        READ_UNCOMMITTED("read_uncommitted");

        private final String value;

        SnapshotIsolationMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotIsolationMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotIsolationMode option : SnapshotIsolationMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotIsolationMode parse(String value, String defaultValue) {
            SnapshotIsolationMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of supported Db2 platforms
     */
    public enum Db2Platform implements EnumeratedValue {

        /**
         * Linux, Unix, Windows
         */
        LUW("LUW") {
            @Override
            public Db2PlatformAdapter createAdapter(Db2ConnectorConfig config) {
                return new LuwPlatform(config);
            }

            @Override
            public String platfromName() {
                return "LUW";
            }
        },

        /**
         * z/OS
         */
        Z("ZOS") {
            @Override
            public Db2PlatformAdapter createAdapter(Db2ConnectorConfig config) {
                return new ZOsPlatform(config);
            }

            @Override
            public String platfromName() {
                return "z/OS";
            }
        };

        private final String value;

        Db2Platform(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public abstract Db2PlatformAdapter createAdapter(Db2ConnectorConfig config);

        public abstract String platfromName();

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static Db2Platform parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (Db2Platform option : Db2Platform.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static Db2Platform parse(String value, String defaultValue) {
            Db2Platform mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    /**
     * Define if the connector, running in Z/OS mode, should ignore the stopLsn (IBMSNAP_REGISTER.CD_OLD_SYNCHPOINT) during polling
     */
    public static final Field Z_STOP_LSN_IGNORE = Field.create("z.stop.lsn.ignore")
            .withDisplayName("z/OS ignore stop LSN")
            .withDefault(false)
            .withType(Type.BOOLEAN)
            .withDescription("If true, causes the connector to ignore the stop LSN value from the " +
                    "IBMSNAP_REGISTER.CD_OLD_SYNCHPOINT column when polling. " +
                    "Apply this if events are getting dropped due to the stop LSN being " +
                    "smaller than the current range of LSNs. Only applies to z/OS platform.");

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
                    + "'schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

    public static final Field SNAPSHOT_ISOLATION_MODE = Field.create("snapshot.isolation.mode")
            .withDisplayName("Snapshot isolation mode")
            .withEnum(SnapshotIsolationMode.class, SnapshotIsolationMode.REPEATABLE_READ)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls which transaction isolation level is used and how long the connector locks the monitored tables. "
                    + "The default is '" + SnapshotIsolationMode.REPEATABLE_READ.getValue()
                    + "', which means that repeatable read isolation level is used. In addition, exclusive locks are taken only during schema snapshot. "
                    + "Using a value of '" + SnapshotIsolationMode.EXCLUSIVE.getValue()
                    + "' ensures that the connector holds the exclusive lock (and thus prevents any reads and updates) for all monitored tables during the entire snapshot duration. "
                    + "In '" + SnapshotIsolationMode.READ_COMMITTED.getValue()
                    + "' mode no table locks or any *long-lasting* row-level locks are acquired, but connector does not guarantee snapshot consistency."
                    + "In '" + SnapshotIsolationMode.READ_UNCOMMITTED.getValue()
                    + "' mode neither table nor row-level locks are acquired, but connector does not guarantee snapshot consistency.");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create("snapshot.locking.mode")
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.EXCLUSIVE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 2))
            .withDescription(
                    "Controls how the connector holds locks on tables while performing the schema snapshot when `snapshot.isolation.mode` is `REPEATABLE_READ` or `EXCLUSIVE`. The 'exclusive' "
                            + "which means the connector will hold a table lock for exclusive table access for just the initial portion of the snapshot "
                            + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
                            + "each table, and this is done using a flashback query that requires no locks. However, in some cases it may be desirable to avoid "
                            + "locks entirely which can be done by specifying 'none'. This mode is only safe to use if no schema changes are happening while the "
                            + "snapshot is taken.");

    public static final Field CDC_CONTROL_SCHEMA = Field.create("cdc.control.schema")
            .withDisplayName("CDC control schema")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 0))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_CDC_SCHEMA)
            .withDescription(
                    "The name of the schema where CDC control structures are located; defaults to '" + DEFAULT_CDC_SCHEMA + "'");

    public static final Field CDC_CHANGE_TABLES_SCHEMA = Field.create("cdc.change.tables.schema")
            .withDisplayName("CDC change tables schema")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 1))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_CDC_SCHEMA)
            .withDescription(
                    "The name of the schema where CDC change tables are located; defaults to '" + DEFAULT_CDC_SCHEMA + "'");

    public static final Field DB2_PLATFORM = Field.create("db2.platform")
            .withDisplayName("Db2 platform")
            .withEnum(Db2Platform.class, Db2Platform.LUW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 2))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Informs connector which Db2 implementation platform it is connected to. "
                    + "The default is '" + Db2Platform.LUW
                    + "', which means Windows, UNIX, Linux. "
                    + "Using a value of '" + Db2Platform.Z
                    + "' ensures that the Db2 for z/OS specific SQL statements are used.");

    public static final Field QUERY_FETCH_SIZE = CommonConnectorConfig.QUERY_FETCH_SIZE
            .withDescription(
                    "The maximum number of records that should be loaded into memory while streaming. A value of '0' uses the default JDBC fetch size. The default value is '10000'.")
            .withDefault(DEFAULT_QUERY_FETCH_SIZE);

    public static final Field STREAMING_QUERY_TIMESPAN_SECONDS = Field.create("streaming.query.timespan.seconds")
            .withDescription(
                    "The maximum number of seconds for a streaming query to include that query's result set, starting from the earliest row in this query.  " +
                            "Used to limit the size of queries when the change table is large to avoid excessive resource usage. If 0, no timespan limit will apply.")
            .withType(Type.INT)
            .withImportance(Importance.LOW)
            .withWidth(Width.SHORT)
            .withDefault(DEFAULT_STREAMING_QUERY_TIMESPAN_SECONDS)
            .withValidation((config, field, problems) -> {
                Integer value = config.getInteger(field);
                if (value == 1) {
                    problems.accept(field, value, "The value of 1 can cause an infinite loop.  " +
                            "Please set it to a higher setting or zero to disable.  120 seconds is a commonly used value.");
                    return 1;
                }
                else if (value < 0) {
                    problems.accept(field, value, "The value must be greater than or equal to 0, but not 1.");
                    return 1;
                }
                return 0;
            });

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(Db2SourceInfoStructMaker.class.getName());

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("Db2")
            .excluding(CommonConnectorConfig.QUERY_FETCH_SIZE,
                    CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER)
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME)
            .connector(
                    SNAPSHOT_MODE,
                    INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
                    SCHEMA_NAME_ADJUSTMENT_MODE,
                    QUERY_FETCH_SIZE,
                    CDC_CONTROL_SCHEMA,
                    CDC_CHANGE_TABLES_SCHEMA,
                    DB2_PLATFORM)
            .events(SOURCE_INFO_STRUCT_MAKER)
            .excluding(
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_EXCLUDE_LIST,
                    // additional fields
                    BINARY_HANDLING_MODE,
                    INCLUDE_SCHEMA_COMMENTS,
                    INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES,
                    SNAPSHOT_MAX_THREADS,
                    DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY)
            .create();

    protected static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final String databaseName;

    private final SnapshotMode snapshotMode;
    private final SnapshotIsolationMode snapshotIsolationMode;
    private final SnapshotLockingMode snapshotLockingMode;

    private final Db2Platform db2Platform;
    private final boolean zStopLsnIgnore;
    private final String cdcChangeTablesSchema;
    private final String cdcControlSchema;

    private final int streamingQueryTimespanSeconds;
    private final boolean streamingQueryTimespanEnabled;

    public Db2ConnectorConfig(Configuration config) {
        super(
                Db2Connector.class,
                config,
                new SystemTablesPredicate(),
                x -> x.schema() + "." + x.table(),
                false,
                ColumnFilterMode.SCHEMA,
                false);

        this.databaseName = config.getString(DATABASE_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        this.snapshotIsolationMode = SnapshotIsolationMode.parse(config.getString(SNAPSHOT_ISOLATION_MODE), SNAPSHOT_ISOLATION_MODE.defaultValueAsString());
        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SNAPSHOT_LOCKING_MODE.defaultValueAsString());

        this.db2Platform = Db2Platform.parse(config.getString(DB2_PLATFORM), DB2_PLATFORM.defaultValueAsString());
        this.zStopLsnIgnore = config.getBoolean(Z_STOP_LSN_IGNORE);
        this.cdcChangeTablesSchema = config.getString(CDC_CHANGE_TABLES_SCHEMA);
        this.cdcControlSchema = config.getString(CDC_CONTROL_SCHEMA);

        this.streamingQueryTimespanSeconds = config.getInteger(STREAMING_QUERY_TIMESPAN_SECONDS);
        if (this.streamingQueryTimespanSeconds <= 0) {
            this.streamingQueryTimespanEnabled = false;
        }
        else if (this.streamingQueryTimespanSeconds == 1) {
            throw new ConfigException("The {} setting of 1 can cause an infinite loop.  " +
                    "Please set it to a higher setting or zero to disable.", STREAMING_QUERY_TIMESPAN_SECONDS.name());
        }
        else {
            this.streamingQueryTimespanEnabled = true;
        }
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public SnapshotIsolationMode getSnapshotIsolationMode() {
        return this.snapshotIsolationMode;
    }

    public Optional<SnapshotLockingMode> getSnapshotLockingMode() {
        return Optional.of(this.snapshotLockingMode);
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public Db2Platform getDb2Platform() {
        return db2Platform;
    }

    public boolean isZStopLsnIgnore() {
        return zStopLsnIgnore;
    }

    public String getCdcChangeTablesSchema() {
        return cdcChangeTablesSchema;
    }

    public String getCdcControlSchema() {
        return cdcControlSchema;
    }

    public int getStreamingQueryTimespanSeconds() {
        return streamingQueryTimespanSeconds;
    }

    public boolean isStreamingQueryTimespanEnabled() {
        return streamingQueryTimespanEnabled;
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    private static class SystemTablesPredicate implements TableFilter {

        @Override
        public boolean isIncluded(TableId t) {
            return t.schema() != null &&
                    !(t.table().toLowerCase().startsWith("ibmsnap_") ||
                            t.schema().toUpperCase().startsWith(DEFAULT_CDC_SCHEMA) ||
                            t.schema().toUpperCase().startsWith("SYSTOOLS") ||
                            t.table().toLowerCase().startsWith("ibmqrep_"));

        }
    }

    @Override
    public HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return Lsn.valueOf(recorded.getString(SourceInfo.CHANGE_LSN_KEY))
                        .compareTo(Lsn.valueOf(desired.getString(SourceInfo.CHANGE_LSN_KEY))) < 1;
            }
        };
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    /**
     * Returns any SELECT overrides, if present.
     */
    @Override
    public Map<DataCollectionId, String> getSnapshotSelectOverridesByTable() {
        String tableListString = getConfig().getString(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE);

        if (tableListString == null) {
            return Collections.emptyMap();
        }

        Map<TableId, String> snapshotSelectOverridesByTable = new HashMap<>();
        final List<String> tableList = listOfTrimmed(tableListString, ',', s -> s);

        for (String table : tableList) {
            snapshotSelectOverridesByTable.put(
                    TableId.parse(table, false),
                    getConfig().getString(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + "." + table));
        }

        return Collections.unmodifiableMap(snapshotSelectOverridesByTable);
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }
}
