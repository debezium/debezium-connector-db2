/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2.platform;

/**
 * Implementation details differing between Db2 flavours
 *
 * @author Jiri Pechanec
 */
public interface Db2PlatformAdapter {

    String getMaxLsnQuery();

    String getAllChangesForTableQuery();

    String getEndLsnForSecondsFromLsnQuery();

    String getListOfCdcEnabledTablesQuery();

    String getListOfNewCdcEnabledTablesQuery();
}
