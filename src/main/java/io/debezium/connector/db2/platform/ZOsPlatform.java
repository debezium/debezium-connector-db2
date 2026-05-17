/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2.platform;

import io.debezium.connector.db2.Db2ConnectorConfig;

/**
 * Implementation details for z/OS
 *
 * @author Jiri Pechanec
 */
public class ZOsPlatform implements Db2PlatformAdapter {

    private final String getMaxLsn;
    private final String getAllChangesForTable;
    private final String getListOfCdcEnabledTables;
    private final String getListOfNewCdcEnabledTables;
    private final String getEndLsnForSecondsFromLsn;

    public ZOsPlatform(Db2ConnectorConfig connectorConfig) {

        this.getMaxLsn = "SELECT max(t.SYNCHPOINT) FROM ( SELECT CD_NEW_SYNCHPOINT AS SYNCHPOINT FROM " + connectorConfig.getCdcControlSchema()
                + ".IBMSNAP_REGISTER UNION ALL SELECT SYNCHPOINT AS SYNCHPOINT FROM " + connectorConfig.getCdcControlSchema()
                + ".IBMSNAP_REGISTER) t for read only with ur";

        this.getAllChangesForTable = "WITH tmp AS (SELECT cdc.IBMSNAP_OPERATION, cdc.IBMSNAP_COMMITSEQ, cdc.IBMSNAP_INTENTSEQ, " +
                "ROW_NUMBER() OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ) rn FROM "
                + connectorConfig.getCdcChangeTablesSchema() + ".# cdc WHERE  cdc.IBMSNAP_COMMITSEQ >= ? AND cdc.IBMSNAP_COMMITSEQ <= ? " +
                " order by IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ), " +
                " tmp2 AS (SELECT " +
                " CASE " +
                " WHEN cdc.IBMSNAP_OPERATION = 'U' THEN 5" +
                " WHEN cdc.IBMSNAP_OPERATION = 'D' AND cdc2.IBMSNAP_OPERATION ='I' THEN 3 " +
                " WHEN cdc.IBMSNAP_OPERATION = 'I' AND cdc2.IBMSNAP_OPERATION ='D' THEN 4 " +
                " WHEN cdc.IBMSNAP_OPERATION = 'D' THEN 1 " +
                " WHEN cdc.IBMSNAP_OPERATION = 'I' THEN 2 " +
                " END " +
                " OPCODE, " +
                " cdc.IBMSNAP_COMMITSEQ, cdc.IBMSNAP_INTENTSEQ, cdc.IBMSNAP_OPERATION " +
                " FROM tmp cdc left JOIN tmp cdc2 " +
                " ON  cdc.IBMSNAP_COMMITSEQ = cdc2.IBMSNAP_COMMITSEQ AND " +
                " ((cdc.IBMSNAP_OPERATION = 'D' AND cdc.rn = cdc2.rn - 1) " +
                "  OR (cdc.IBMSNAP_OPERATION = 'I' AND cdc.rn = cdc2.rn + 1))) " +
                " select res.OPCODE, cdc.* from " + connectorConfig.getCdcChangeTablesSchema()
                + ".# cdc inner join tmp2 res on cdc.IBMSNAP_COMMITSEQ=res.IBMSNAP_COMMITSEQ and cdc.IBMSNAP_INTENTSEQ=res.IBMSNAP_INTENTSEQ "
                + "order by IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ";

        this.getListOfCdcEnabledTables = "select r.SOURCE_OWNER, r.SOURCE_TABLE, r.CD_OWNER, r.CD_TABLE, r.SYNCHPOINT, r.CD_OLD_SYNCHPOINT, t.DBID, t.OBID , CAST((t.DBID * 65536 +  t.OBID )AS INTEGER )from "
                + connectorConfig.getCdcControlSchema()
                + ".IBMSNAP_REGISTER r left JOIN SYSIBM.SYSTABLES t ON r.SOURCE_OWNER  = t.CREATOR AND r.SOURCE_TABLE = t.NAME  WHERE r.SOURCE_OWNER <> '' for read only with ur";

        this.getListOfNewCdcEnabledTables = "select CAST((t.DBID * 65536 +  t.OBID )AS INTEGER ) AS OBJECTID, " +
                "       CD_OWNER CONCAT '.' CONCAT CD_TABLE, " +
                "       CD_NEW_SYNCHPOINT, " +
                "       CD_OLD_SYNCHPOINT " +
                "from " + connectorConfig.getCdcControlSchema()
                + ".IBMSNAP_REGISTER  r left JOIN SYSIBM.SYSTABLES t ON r.SOURCE_OWNER  = t.CREATOR AND r.SOURCE_TABLE = t.NAME " +
                "WHERE r.SOURCE_OWNER <> '' AND 1=0 AND CD_NEW_SYNCHPOINT > ? AND CD_OLD_SYNCHPOINT < ?  for read only with ur";

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
