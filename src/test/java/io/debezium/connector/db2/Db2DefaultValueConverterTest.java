/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;

/**
 * Unit tests for {@link Db2DefaultValueConverter} that verify default value conversion
 * without requiring a database connection.
 *
 * @author Jiri Pechanec
 */
public class Db2DefaultValueConverterTest {

    private Db2ValueConverters valueConverters;
    private Db2DefaultValueConverter defaultValueConverter;

    @BeforeEach
    public void setUp() {
        valueConverters = new Db2ValueConverters(DecimalMode.PRECISE, TemporalPrecisionMode.ADAPTIVE);
        // Pass null for connection as we don't need it for these tests
        defaultValueConverter = new Db2DefaultValueConverter(valueConverters, null);
    }

    @Test
    @FixFor("debezium/dbz#2042")
    public void shouldAdjustDecimalScaleForDefaultValueZero() {
        // Create a DECIMAL(18,8) column with default value "0"
        final var column = Column.editor()
                .name("amount")
                .type("DECIMAL")
                .jdbcType(Types.DECIMAL)
                .length(18)
                .scale(8)
                .optional(true)
                .create();

        // Parse the default value "0"
        final var result = defaultValueConverter.parseDefaultValue(column, "0");

        // Verify the result
        assertThat(result).isPresent();
        assertThat(result.get()).isInstanceOf(BigDecimal.class);

        final var decimal = (BigDecimal) result.get();
        assertThat(decimal.scale()).isEqualTo(8);
        assertThat(decimal).isEqualTo(new BigDecimal("0.00000000"));
    }

    @Test
    @FixFor("debezium/dbz#2042")
    public void shouldAdjustDecimalScaleForDefaultValueWithPartialScale() {
        // Create a DECIMAL(5,2) column with default value "3.1"
        final var column = Column.editor()
                .name("price")
                .type("DECIMAL")
                .jdbcType(Types.DECIMAL)
                .length(5)
                .scale(2)
                .optional(true)
                .create();

        // Parse the default value "3.1" (scale 1)
        final var result = defaultValueConverter.parseDefaultValue(column, "3.1");

        // Verify the result
        assertThat(result).isPresent();
        assertThat(result.get()).isInstanceOf(BigDecimal.class);

        final var decimal = (BigDecimal) result.get();
        assertThat(decimal.scale()).isEqualTo(2);
        assertThat(decimal).isEqualTo(new BigDecimal("3.10"));
    }

    @Test
    @FixFor("debezium/dbz#2042")
    public void shouldNotAdjustDecimalScaleWhenAlreadyCorrect() {
        // Create a DECIMAL(18,8) column with default value "0.00000000"
        final var column = Column.editor()
                .name("amount")
                .type("DECIMAL")
                .jdbcType(Types.DECIMAL)
                .length(18)
                .scale(8)
                .optional(true)
                .create();

        // Parse the default value "0.00000000" (already has scale 8)
        final var result = defaultValueConverter.parseDefaultValue(column, "0.00000000");

        // Verify the result
        assertThat(result).isPresent();
        assertThat(result.get()).isInstanceOf(BigDecimal.class);

        final var decimal = (BigDecimal) result.get();
        assertThat(decimal.scale()).isEqualTo(8);
        assertThat(decimal).isEqualTo(new BigDecimal("0.00000000"));
    }

    @Test
    @FixFor("debezium/dbz#2042")
    public void shouldHandleDecimalWithScaleZero() {
        // Create a DECIMAL(10,0) column with default value "100"
        final var column = Column.editor()
                .name("quantity")
                .type("DECIMAL")
                .jdbcType(Types.DECIMAL)
                .length(10)
                .scale(0)
                .optional(true)
                .create();

        // Parse the default value "100"
        final var result = defaultValueConverter.parseDefaultValue(column, "100");

        // Verify the result
        assertThat(result).isPresent();
        assertThat(result.get()).isInstanceOf(BigDecimal.class);

        final var decimal = (BigDecimal) result.get();
        assertThat(decimal.scale()).isEqualTo(0);
        assertThat(decimal).isEqualTo(new BigDecimal("100"));
    }

    @Test
    @FixFor("debezium/dbz#2042")
    public void shouldAdjustNumericScaleForDefaultValueZero() {
        // Create a NUMERIC(18,8) column with default value "0"
        // NUMERIC is an alias for DECIMAL in DB2
        final var column = Column.editor()
                .name("balance")
                .type("NUMERIC")
                .jdbcType(Types.NUMERIC)
                .length(18)
                .scale(8)
                .optional(true)
                .create();

        // Parse the default value "0"
        final var result = defaultValueConverter.parseDefaultValue(column, "0");

        // Verify the result
        assertThat(result).isPresent();
        assertThat(result.get()).isInstanceOf(BigDecimal.class);

        final var decimal = (BigDecimal) result.get();
        assertThat(decimal.scale()).isEqualTo(8);
        assertThat(decimal).isEqualTo(new BigDecimal("0.00000000"));
    }

    @Test
    @FixFor("debezium/dbz#2042")
    public void shouldHandleNullDefaultValue() {
        // Create a DECIMAL(18,8) column with NULL default value
        final var column = Column.editor()
                .name("amount")
                .type("DECIMAL")
                .jdbcType(Types.DECIMAL)
                .length(18)
                .scale(8)
                .optional(true)
                .create();

        // Parse the default value "NULL"
        final var result = defaultValueConverter.parseDefaultValue(column, "NULL");

        // Verify the result - NULL default values return empty Optional
        assertThat(result).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2042")
    public void shouldHandleDecimalWithDifferentScales() {
        // Test various scale combinations
        final int[][] testCases = {
                { 10, 4 }, // DECIMAL(10,4)
                { 18, 8 }, // DECIMAL(18,8)
                { 5, 2 }, // DECIMAL(5,2)
                { 20, 10 }, // DECIMAL(20,10)
                { 15, 5 } // DECIMAL(15,5)
        };

        for (final int[] testCase : testCases) {
            final int length = testCase[0];
            final int scale = testCase[1];

            final var column = Column.editor()
                    .name("test_column")
                    .type("DECIMAL")
                    .jdbcType(Types.DECIMAL)
                    .length(length)
                    .scale(scale)
                    .optional(true)
                    .create();

            final var result = defaultValueConverter.parseDefaultValue(column, "0");

            assertThat(result).isPresent();
            assertThat(result.get()).isInstanceOf(BigDecimal.class);

            final var decimal = (BigDecimal) result.get();
            assertThat(decimal.scale())
                    .as("Scale should be %d for DECIMAL(%d,%d)", scale, length, scale)
                    .isEqualTo(scale);
        }
    }

    @Test
    @FixFor("debezium/dbz#2042")
    public void shouldHandleIntegerDefaultValue() {
        // Create an INTEGER column with default value "42"
        final var column = Column.editor()
                .name("count")
                .type("INTEGER")
                .jdbcType(Types.INTEGER)
                .optional(true)
                .create();

        // Parse the default value "42"
        final var result = defaultValueConverter.parseDefaultValue(column, "42");

        // Verify the result (should not be affected by BigDecimal scale adjustment)
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(42);
    }

    @Test
    @FixFor("debezium/dbz#2042")
    public void shouldHandleVarcharDefaultValue() {
        // Create a VARCHAR column with default value "'hello'"
        final var column = Column.editor()
                .name("message")
                .type("VARCHAR")
                .jdbcType(Types.VARCHAR)
                .length(100)
                .optional(true)
                .create();

        // Parse the default value "'hello'"
        final var result = defaultValueConverter.parseDefaultValue(column, "'hello'");

        // Verify the result (should not be affected by BigDecimal scale adjustment)
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("hello");
    }
}
