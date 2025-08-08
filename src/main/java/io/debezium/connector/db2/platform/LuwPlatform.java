/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2.platform;

import io.debezium.connector.db2.Db2ConnectorConfig;

/**
 * Implementation details for LUW Platform (Linux, Unix, Windows)
 *
 * @author Jiri Pechanec
 */
public class LuwPlatform implements Db2PlatformAdapter {

    private final String getMaxLsn;
    private final String getAllChangesForTable;
    private final String getListOfCdcEnabledTables;
    private final String getListOfNewCdcEnabledTables;
    private static final String STATEMENTS_PLACEHOLDER = "#";
    private final String getEndLsnForSecondsFromLsn;

    public LuwPlatform(Db2ConnectorConfig connectorConfig) {

        this.getMaxLsn = "SELECT max(t.SYNCHPOINT) FROM ( SELECT CD_NEW_SYNCHPOINT AS SYNCHPOINT FROM " + connectorConfig.getCdcControlSchema()
                + ".IBMSNAP_REGISTER UNION ALL SELECT SYNCHPOINT AS SYNCHPOINT FROM " + connectorConfig.getCdcControlSchema() + ".IBMSNAP_REGISTER) t";

        this.getAllChangesForTable = "SELECT "
                + "CASE "
                + "WHEN IBMSNAP_OPERATION = 'D' AND (LEAD(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ)) ='I' THEN 3 "
                + "WHEN IBMSNAP_OPERATION = 'I' AND (LAG(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ)) ='D' THEN 4 "
                + "WHEN IBMSNAP_OPERATION = 'D' THEN 1 "
                + "WHEN IBMSNAP_OPERATION = 'I' THEN 2 "
                + "END "
                + "OPCODE,"
                + "cdc.* "
                + "FROM " + connectorConfig.getCdcChangeTablesSchema() + ".# cdc WHERE   IBMSNAP_COMMITSEQ >= ? AND IBMSNAP_COMMITSEQ <= ? "
                + "order by IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ";

        this.getListOfCdcEnabledTables = "select r.SOURCE_OWNER, r.SOURCE_TABLE, r.CD_OWNER, r.CD_TABLE, r.SYNCHPOINT, r.CD_OLD_SYNCHPOINT, t.TBSPACEID, t.TABLEID , CAST((t.TBSPACEID * 65536 +  t.TABLEID )AS INTEGER )from "
                + connectorConfig.getCdcControlSchema()
                + ".IBMSNAP_REGISTER r left JOIN SYSCAT.TABLES t ON r.SOURCE_OWNER  = t.TABSCHEMA AND r.SOURCE_TABLE = t.TABNAME  WHERE r.SOURCE_OWNER <> ''";

        // No new Tables 1=0
        this.getListOfNewCdcEnabledTables = "select CAST((t.TBSPACEID * 65536 +  t.TABLEID )AS INTEGER ) AS OBJECTID, " +
                "       CD_OWNER CONCAT '.' CONCAT CD_TABLE, " +
                "       CD_NEW_SYNCHPOINT, " +
                "       CD_OLD_SYNCHPOINT " +
                "from " + connectorConfig.getCdcControlSchema()
                + ".IBMSNAP_REGISTER  r left JOIN SYSCAT.TABLES t ON r.SOURCE_OWNER  = t.TABSCHEMA AND r.SOURCE_TABLE = t.TABNAME " +
                "WHERE r.SOURCE_OWNER <> '' AND CD_NEW_SYNCHPOINT > ? AND (CD_OLD_SYNCHPOINT < ? OR CD_OLD_SYNCHPOINT IS NULL)";

        this.getEndLsnForSecondsFromLsn = "" +
                "SELECT " +
                "       uow.IBMSNAP_LOGMARKER AS COMMITSEQ_TIME, " +
                "       uow.IBMSNAP_COMMITSEQ AS COMMITSEQ " +
                "FROM " +
                connectorConfig.getCdcControlSchema() + ".IBMSNAP_UOW uow " +
                "WHERE " +
                "       uow.IBMSNAP_COMMITSEQ > ? " +
                "       AND uow.IBMSNAP_LOGMARKER <= " +
                "              ( " +
                "                     COALESCE(" +
                "                       (SELECT uow.IBMSNAP_LOGMARKER + ? SECONDS " +
                "                       FROM " + connectorConfig.getCdcControlSchema() + ".IBMSNAP_UOW uow " +
                "                       WHERE uow.IBMSNAP_COMMITSEQ = ? " +
                "                       LIMIT 1), " +
                "                       (SELECT MIN(uow.IBMSNAP_LOGMARKER) " +
                "                       FROM " + connectorConfig.getCdcControlSchema() + ".IBMSNAP_UOW uow " +
                "                       WHERE uow.IBMSNAP_COMMITSEQ > ? )" +
                "                     ) " +
                "               ) " +
                "ORDER BY " +
                "       uow.IBMSNAP_COMMITSEQ DESC " +
                "LIMIT 1";

    }

    @Override
    public String getMaxLsnQuery() {
        return getMaxLsn;
    }

    @Override
    public String getAllChangesForTableQuery() {
        return getAllChangesForTable;
    }

    @Override
    public String getListOfCdcEnabledTablesQuery() {
        return getListOfCdcEnabledTables;
    }

    @Override
    public String getListOfNewCdcEnabledTablesQuery() {
        return getListOfNewCdcEnabledTables;
    }

    @Override
    public String getEndLsnForSecondsFromLsnQuery() {
        return getEndLsnForSecondsFromLsn;
    }
}
