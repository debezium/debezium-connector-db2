/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Abstract default value integration test.
 *
 * This test is extended by two variants, online and offline schema evolution. All tests should
 * be included in this class and therefore should pass both variants to make sure that the
 * schema evolution process works regardless of mode used by the user.
 *
 * @author Chris Cranford
 */
public abstract class AbstractDb2DefaultValueIT extends AbstractAsyncEngineConnectorTest {

    private Db2Connection connection;
    private Configuration config;

    // BINARY
    // BLOB/VARBINARY
    // DBCLOB
    // VARGRAPHIC
    // XML

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        TestHelper.disableDbCdc(connection);
        TestHelper.disableTableCdc(connection, "DV_TEST");

        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute("DROP TABLE IF EXISTS dv_test");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);
            TestHelper.disableTableCdc(connection, "DV_TEST");
            connection.execute("DROP TABLE IF EXISTS dv_test");
            connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_COLVERSION");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_TABVERSION");
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-4990")
    @Ignore("The ASN capture process does to not capture changes for a table using boolean data types.")
    public void shouldHandleBooleanDefaultTypes() throws Exception {
        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_boolean", "boolean",
                        "true", "false",
                        true, false,
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-4990")
    public void shouldHandleNumericDefaultTypes() throws Exception {
        // TODO: remove once https://github.com/Apicurio/apicurio-registry/issues/2990 is fixed
        if (VerifyRecord.isApucurioAvailable()) {
            skipAvroValidation();
        }

        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_bigint", "bigint",
                        "1", "2",
                        1L, 2L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_integer", "integer",
                        "1", "2",
                        1, 2,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_smallint", "smallint",
                        "1", "2",
                        (short) 1, (short) 2,
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-4990")
    public void shouldHandleFloatPointDefaultTypes() throws Exception {
        // TODO: remove once https://github.com/Apicurio/apicurio-registry/issues/2980 is fixed
        if (VerifyRecord.isApucurioAvailable()) {
            skipAvroValidation();
        }

        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                // DECFLOAT is variable scale precise type and Kafka does not support STRUCT
                // as default value so it is set to null
                new ColumnDefinition("val_decfloat", "decfloat",
                        "3.14", "6.28", null,
                        null, AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_decimal", "decimal(5,2)",
                        "3.14", "6.28",
                        BigDecimal.valueOf(3.14), BigDecimal.valueOf(6.28),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_numeric", "numeric(5,2)",
                        "3.14", "6.28",
                        BigDecimal.valueOf(3.14), BigDecimal.valueOf(6.28),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_double", "double",
                        "3.14", "6.28",
                        3.14d, 6.28d,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_float", "float",
                        "3.14", "6.28",
                        3.14d, 6.28d,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_real", "real",
                        "3.14", "6.28",
                        3.14f, 6.28f,
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-4990")
    public void shouldHandleCharacterDefaultTypes() throws Exception {
        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_varchar", "varchar(100)",
                        "'hello'", "'world'",
                        "hello", "world",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_nvarchar", "nvarchar(100)",
                        "'cedric'", "'entertainer'",
                        "cedric", "entertainer",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_char", "char(5)",
                        "'YES'", "'NO'",
                        "YES  ", "NO   ",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_nchar", "nchar(5)",
                        "'ON'", "'OFF'",
                        "ON   ", "OFF  ",
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-4990")
    public void shouldHandleDateTimeDefaultTypes() throws Exception {
        // todo: documentation says DATETIME, TIMESTAMP emitted in milliseconds, but values appear to be in microseconds
        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_date", "date",
                        "'2022-01-01'", "'2022-01-02'",
                        18993, 18994,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_datetime", "datetime",
                        "'2022-01-01 01:02:03'", "'2022-01-02 01:02:03'",
                        1640998923000000L, 1641085323000000L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_time", "time",
                        "'01:02:03'", "'02:03:04'",
                        3723000, 7384000,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_timestamp", "timestamp",
                        "'2022-01-01 01:02:03'", "'2022-01-02 01:02:03'",
                        1640998923000000L, 1641085323000000L,
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    protected abstract void performSchemaChange(Configuration config, Db2Connection connection, String alterStatement) throws Exception;

    /**
     * Handles executing the full common set of default value tests for the supplied column definitions.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void shouldHandleDefaultValuesCommon(List<ColumnDefinition> columnDefinitions) throws Exception {
        testDefaultValuesCreateTableAndSnapshot(columnDefinitions);
        testDefaultValuesAlterTableModifyExisting(columnDefinitions);
        testDefaultValuesAlterTableAdd(columnDefinitions);
        TestDefaultValuesByRestartAndLoadingHistoryTopic();
    }

    /**
     * Restarts the connector and verifies when the database history topic is loaded that we can parse
     * all the loaded history statements without failures.
     */
    private void TestDefaultValuesByRestartAndLoadingHistoryTopic() throws Exception {
        stopConnector();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning("db2_server", TestHelper.TEST_DATABASE);
    }

    /**
     * Creates the table and pre-inserts a record captured during the snapshot phase.  The snapshot
     * record will be validated against the supplied column definitions.
     *
     * The goal of this method is to test that when a table is snapshot which uses default values
     * that both the in-memory schema representation and the snapshot pipeline change event have
     * the right default value resolution.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void testDefaultValuesCreateTableAndSnapshot(List<ColumnDefinition> columnDefinitions) throws Exception {
        // Build SQL
        final StringBuilder createSql = new StringBuilder();
        createSql.append("CREATE TABLE dv_test (id int not null");
        for (ColumnDefinition column : columnDefinitions) {
            createSql.append(", ")
                    .append(column.name)
                    .append(" ").append(column.definition)
                    .append(" ").append("default ").append(column.addDefaultValue);
            createSql.append(", ")
                    .append(column.name).append("_null")
                    .append(" ").append(column.definition)
                    .append(" ").append("default null");
            if (column.temporalType) {
                final String currentDefaultValue = column.getCurrentRegister();
                createSql.append(", ")
                        .append(column.name).append("_sysdate")
                        .append(" ").append(column.definition)
                        .append(" ").append("default ").append(currentDefaultValue);
                createSql.append(", ")
                        .append(column.name).append("_sysdate_nonnull")
                        .append(" ").append(column.definition)
                        .append(" ").append("default ").append(currentDefaultValue).append(" not null");
            }
        }
        createSql.append(", primary key(id))");

        // Create table and add cdc support
        connection.execute(createSql.toString());

        // Insert snapshot record
        connection.execute("INSERT INTO dv_test (id) values (1)");
        TestHelper.enableTableCdc(connection, "DV_TEST");

        // store config so it can be used by other methods
        config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.TABLE_INCLUDE_LIST, "db2inst1.dv_test")
                .build();

        // start connector
        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        TestHelper.waitForSnapshotToBeCompleted();

        SourceRecords records = consumeRecordsByTopic(1);

        // Verify we got only 1 record for our test
        List<SourceRecord> tableRecords = records.recordsForTopic(TestHelper.TEST_DATABASE + ".DB2INST1.DV_TEST");
        assertThat(tableRecords).hasSize(1);
        assertNoRecordsToConsume();

        SourceRecord record = tableRecords.get(0);
        VerifyRecord.isValidRead(record, "ID", 1);
        for (ColumnDefinition column : columnDefinitions) {
            switch (column.assertionType) {
                case FIELD_DEFAULT_EQUAL:
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase() + "_NULL", null);
                    break;
                case FIELD_NO_DEFAULT:
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase() + "_NULL", null);
                    break;
                default:
                    throw new RuntimeException("Unexpected assertion type: " + column.assertionType);
            }
            if (column.temporalType) {
                assertSchemaFieldWithDefaultCurrentDate(record, column.name.toUpperCase() + "_SYSDATE", null);
                if (column.expectedAddDefaultValue instanceof String) {
                    assertSchemaFieldDefaultAndNonNullValue(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", "0");
                }
                else if (column.definition.equalsIgnoreCase("TIMESTAMP")) {
                    assertSchemaFieldWithDefaultCurrentDate(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", 0L);
                }
                else {
                    assertSchemaFieldWithDefaultCurrentDate(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", 0);
                }
            }
        }

        waitForStreamingRunning("db2_server", TestHelper.TEST_DATABASE);

        TestHelper.enableDbCdc(connection);
        TestHelper.activeTable(connection, "DV_TEST");
        TestHelper.refreshAndWait(connection);

        connection.execute("INSERT INTO dv_test (id) values (0)");
        TestHelper.refreshAndWait(connection);
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.TEST_DATABASE + ".DB2INST1.DV_TEST")).hasSize(1);
        assertNoRecordsToConsume();
    }

    /**
     * Alters the underlying table changing the default value to its second form.  This method then inserts
     * a new record that is then validated against the supplied column definitions.
     *
     * The goal of this method is to test that when DDL modifies an existing column in an existing table
     * that the right default value resolution occurs and that the in-memory schema representation is
     * correct as well as the change event capture pipeline.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void testDefaultValuesAlterTableModifyExisting(List<ColumnDefinition> columnDefinitions) throws Exception {
        // Build SQL
        final StringBuilder alterSql = new StringBuilder();
        alterSql.append("ALTER TABLE %table% ");
        Iterator<ColumnDefinition> iterator = columnDefinitions.iterator();
        while (iterator.hasNext()) {
            final ColumnDefinition column = iterator.next();
            alterSql.append("ALTER COLUMN ")
                    .append(column.name)
                    .append(" SET ").append("default ").append(column.modifyDefaultValue);
            alterSql.append(" ALTER COLUMN ")
                    .append(column.name).append("_null")
                    .append(" SET default null ");
            // cannot add alter for temporal columns as they are already set to their respective
            // defaults and DB2 will not allow setting them to what they already are, so please
            // see the creation of the table method for where these are defined.
        }

        performSchemaChange(config, connection, alterSql.toString());
        TestHelper.refreshAndWait(connection);

        connection.execute("INSERT INTO dv_test (id) values (2)");

        SourceRecords records = consumeRecordsByTopic(1);
        records.allRecordsInOrder().forEach(System.out::println);
        assertNoRecordsToConsume();

        // Verify we got only 1 record for our test
        List<SourceRecord> tableRecords = records.recordsForTopic(TestHelper.TEST_DATABASE + ".DB2INST1.DV_TEST");
        assertThat(tableRecords).hasSize(1);

        SourceRecord record = tableRecords.get(0);
        VerifyRecord.isValidInsert(record, "ID", 2);

        for (ColumnDefinition column : columnDefinitions) {
            switch (column.assertionType) {
                case FIELD_DEFAULT_EQUAL:
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase() + "_NULL", null);
                    break;
                case FIELD_NO_DEFAULT:
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase() + "_NULL", null);
                    break;
                default:
                    throw new RuntimeException("Unexpected assertion type: " + column.assertionType);
            }
            if (column.temporalType) {
                assertSchemaFieldWithDefaultCurrentDate(record, column.name.toUpperCase() + "_SYSDATE", null);
                if (column.expectedAddDefaultValue instanceof String) {
                    assertSchemaFieldDefaultAndNonNullValue(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", "0");
                }
                else if (column.definition.equalsIgnoreCase("TIMESTAMP")) {
                    assertSchemaFieldWithDefaultCurrentDate(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", 0L);
                }
                else {
                    assertSchemaFieldWithDefaultCurrentDate(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", 0);
                }
            }
        }
    }

    /**
     * Alters the underlying table changing adding a new column prefixed with {@code A} to each of the column
     * definition with the initial default value definition.
     *
     * The goal of this method is to test that when DDL adds a new column to an existing table that the right
     * default value resolution occurs and that the in-memory schema representation is correct as well as the
     * change event capture pipeline.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void testDefaultValuesAlterTableAdd(List<ColumnDefinition> columnDefinitions) throws Exception {
        // Build SQL
        final StringBuilder alterSql = new StringBuilder();
        alterSql.append("ALTER TABLE %table% ");
        Iterator<ColumnDefinition> iterator = columnDefinitions.iterator();
        while (iterator.hasNext()) {
            final ColumnDefinition column = iterator.next();
            alterSql.append("ADD COLUMN ")
                    .append("a").append(column.name)
                    .append(" ").append(column.definition)
                    .append(" ").append("default ").append(column.addDefaultValue);
            alterSql.append(" ADD COLUMN ")
                    .append("a").append(column.name).append("_null")
                    .append(" ").append(column.definition)
                    .append(" ").append("default null ");
            if (column.temporalType) {
                alterSql.append(" ADD COLUMN ")
                        .append("a").append(column.name).append("_sysdate")
                        .append(" ").append(column.definition)
                        .append(" ").append("default ").append(column.getCurrentRegister());
                alterSql.append(" ADD COLUMN ")
                        .append("a").append(column.name).append("_sysdate_nonnull")
                        .append(" ").append(column.definition)
                        .append(" ").append("default ").append(column.getCurrentRegister()).append(" not null ");
            }
        }

        performSchemaChange(config, connection, alterSql.toString());
        TestHelper.refreshAndWait(connection);

        connection.execute("INSERT INTO dv_test (id) values (3)");
        TestHelper.refreshAndWait(connection);

        SourceRecords records = consumeRecordsByTopic(1);

        // Verify we got only 1 record for our test
        List<SourceRecord> tableRecords = records.recordsForTopic(TestHelper.TEST_DATABASE + ".DB2INST1.DV_TEST");
        assertThat(tableRecords).hasSize(1);

        SourceRecord record = tableRecords.get(0);
        for (ColumnDefinition column : columnDefinitions) {
            switch (column.assertionType) {
                case FIELD_DEFAULT_EQUAL:
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase() + "_NULL", null);
                    assertSchemaFieldWithSameDefaultAndValue(record, "A" + column.name.toUpperCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, "A" + column.name.toUpperCase() + "_NULL", null);
                    break;
                case FIELD_NO_DEFAULT:
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase() + "_NULL", null);
                    assertSchemaFieldNoDefaultWithValue(record, "A" + column.name.toUpperCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, "A" + column.name.toUpperCase() + "_NULL", null);
                    break;
                default:
                    throw new RuntimeException("Unexpected assertion type: " + column.assertionType);
            }
            if (column.temporalType) {
                assertSchemaFieldWithDefaultCurrentDate(record, column.name.toUpperCase() + "_SYSDATE", null);
                assertSchemaFieldWithDefaultCurrentDate(record, "A" + column.name.toUpperCase() + "_SYSDATE", null);
                if (column.expectedAddDefaultValue instanceof String) {
                    assertSchemaFieldDefaultAndNonNullValue(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", "0");
                    assertSchemaFieldDefaultAndNonNullValue(record, "A" + column.name.toUpperCase() + "_SYSDATE_NONNULL", "0");
                }
                else if (column.definition.equalsIgnoreCase("TIMESTAMP")) {
                    assertSchemaFieldWithDefaultCurrentDate(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", 0L);
                    assertSchemaFieldWithDefaultCurrentDate(record, "A" + column.name.toUpperCase() + "_SYSDATE_NONNULL", 0L);
                }
                else {
                    assertSchemaFieldWithDefaultCurrentDate(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", 0);
                    assertSchemaFieldWithDefaultCurrentDate(record, "A" + column.name.toUpperCase() + "_SYSDATE_NONNULL", 0);
                }
            }
        }
    }

    /**
     * Asserts that the schema field's default value and after emitted event value are the same.
     *
     * @param record the change event record, never {@code null}
     * @param fieldName the field name, never {@code null}
     * @param expectedValue the expected value in the field's default and "after" struct
     */
    private static void assertSchemaFieldWithSameDefaultAndValue(SourceRecord record, String fieldName, Object expectedValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, expectedValue, r -> {
            assertThat(r).as("Unexpected field value: " + fieldName).isEqualTo(expectedValue);
        });
    }

    /**
     * Asserts that the schema field's default value is not set and that the emitted event value matches.
     *
     * @param record the change event record, never {@code null}
     * @param fieldName the field name, never {@code null}
     * @param fieldValue the expected value in the field's "after" struct
     */
    // asserts that the field schema has no default value and an emitted value
    private static void assertSchemaFieldNoDefaultWithValue(SourceRecord record, String fieldName, Object fieldValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, null, r -> {
            assertThat(r).as("Unexpected field value: " + fieldName).isEqualTo(fieldValue);
        });
    }

    private static void assertSchemaFieldValueWithDefault(SourceRecord record, String fieldName, Object expectedDefault, Consumer<Object> valueCheck) {
        final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        final Field field = after.schema().field(fieldName);
        assertThat(field).as("Expected non-null field for " + fieldName).isNotNull();
        final Object defaultValue = field.schema().defaultValue();
        if (expectedDefault == null) {
            assertThat(defaultValue).isNull();
            return;
        }
        else {
            assertThat(defaultValue).as("Expected non-null default value for field " + fieldName).isNotNull();
        }
        assertThat(defaultValue.getClass()).isEqualTo(expectedDefault.getClass());
        assertThat(defaultValue).as("Unexpected default value: " + fieldName + " with field value: " + after.get(fieldName)).isEqualTo(expectedDefault);
        valueCheck.accept(after.get(fieldName));
    }

    private static void assertSchemaFieldWithDefaultCurrentDate(SourceRecord record, String fieldName, Object expectedValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, expectedValue, r -> {
            if (expectedValue == null) {
                assertThat(r).isNull();
            }
            else if (expectedValue instanceof Long) {
                assertThat((long) r).as("Unexpected field value: " + fieldName).isGreaterThanOrEqualTo(1L);
            }
            else if (expectedValue instanceof Integer) {
                assertThat((int) r).as("Unexpected field value: " + fieldName).isGreaterThanOrEqualTo(1);
            }
        });
    }

    private static void assertSchemaFieldDefaultAndNonNullValue(SourceRecord record, String fieldName, Object defaultValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, defaultValue, r -> {
            assertThat(r).as("Unexpected field value: " + fieldName).isNotNull();
        });
    }

    /**
     * Defines the different assertion types for a given column definition.
     */
    enum AssertionType {
        // field and default values are identical
        FIELD_DEFAULT_EQUAL,
        // schema has no default value specified
        FIELD_NO_DEFAULT
    }

    /**
     * Defines a column definition and its attributes that are used by tests.
     */
    private static class ColumnDefinition {
        public final String name;
        public final String definition;
        public final String addDefaultValue;
        public final String modifyDefaultValue;
        public final Object expectedAddDefaultValue;
        public final Object expectedModifyDefaultValue;
        public final AssertionType assertionType;
        public final boolean temporalType;

        ColumnDefinition(String name, String definition, String addDefaultValue, String modifyDefaultValue,
                         Object expectedAddDefaultValue, Object expectedModifyDefaultValue, AssertionType assertionType) {
            this.name = name;
            this.definition = definition;
            this.addDefaultValue = addDefaultValue;
            this.modifyDefaultValue = modifyDefaultValue;
            this.expectedAddDefaultValue = expectedAddDefaultValue;
            this.expectedModifyDefaultValue = expectedModifyDefaultValue;
            this.assertionType = assertionType;
            this.temporalType = definition.equalsIgnoreCase("date")
                    || definition.toUpperCase().startsWith("TIMESTAMP")
                    || definition.equalsIgnoreCase("TIME");
        }

        public String getCurrentRegister() {
            if (definition.equalsIgnoreCase("DATE")) {
                return "CURRENT DATE";
            }
            else if (definition.equalsIgnoreCase("TIMESTAMP")) {
                return "CURRENT TIMESTAMP";
            }
            else if (definition.equalsIgnoreCase("TIME")) {
                return "CURRENT TIME";
            }
            else {
                throw new RuntimeException("Unexpected temporal type for current time register: " + definition);
            }
        }
    }

}
