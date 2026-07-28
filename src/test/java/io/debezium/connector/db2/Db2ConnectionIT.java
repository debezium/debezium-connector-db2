/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.debezium.connector.db2.util.TestHelper;

/**
 * Integration test for {@link Db2Connection}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class Db2ConnectionIT {

    @Test
    public void shouldEnableCdcForDatabase() throws Exception {
        try (Db2Connection connection = TestHelper.adminConnection()) {
            connection.connect();
            TestHelper.enableDbCdc(connection);
        }
    }

    @Test
    public void shouldReturnCommitTimestampFromUow() throws Exception {
        try (Db2Connection connection = TestHelper.adminConnection()) {
            connection.connect();

            Lsn lsn = Lsn.valueOf("00000000000000000000000000000123");
            Instant expected = Instant.parse("2026-01-15T10:00:00Z");

            try {
                // seed IBMSNAP_UOW with lsn -> expected
                TestHelper.insertUowRow(connection, lsn, expected);

                Instant actual = connection.timestampOfLsn(lsn);
                assertThat(actual).isEqualTo(expected);
            }
            finally {
                TestHelper.deleteUowRow(connection, lsn);
            }
        }
    }

    @Test
    public void shouldFallBackToCurrentTimestampWhenUowRowMissing() throws Exception {
        try (Db2Connection connection = TestHelper.adminConnection()) {
            connection.connect();

            Lsn lsn = Lsn.valueOf("00000000000000000000000000000099"); // no UOW row exists

            // Ensure no stray row exists from a prior failed run
            TestHelper.deleteUowRow(connection, lsn);

            Instant before = connection.getCurrentTimestamp().orElseThrow().minusSeconds(10);
            Instant actual = connection.timestampOfLsn(lsn);
            Instant after = connection.getCurrentTimestamp().orElseThrow().plusSeconds(10);

            assertThat(actual).isBetween(before, after);
        }
    }

    @Test
    public void shouldReturnNullForNullLsn() throws Exception {
        try (Db2Connection connection = TestHelper.adminConnection()) {
            connection.connect();
            assertThat(connection.timestampOfLsn(Lsn.NULL)).isNull();
        }
    }
}
