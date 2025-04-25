package org.apache.spark.sql.execution.datasources.mdjdbc.connection.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据源类型枚举
 *
 * @author ouhh
 * @since 2020-07-12
 */
public enum DBDriverTypes {
    /**
     * oracle
     */
    ORACLE("oracle", "oracle", "oracle.jdbc.OracleDriver"),
    /**
     * mysql
     */
    MYSQL("mysql", "mysql", "com.mysql.cj.jdbc.Driver"),
    /**
     * xugu
     */
    XUGU("xugu", "xugu", "com.xugu.cloudjdbc.Driver"),
    /**
     * doris
     */
    DORIS("doris", "doris", "com.mysql.cj.jdbc.Driver"),
    /**
     * sqlserver
     */
    SQLSERVER("SQL Server", "sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    /**
     * 达梦6
     */
    DM6("达梦6", "dm6", "dm.jdbc.driver.DmDriver"),
    /**
     * 达梦7
     */
    DM7("达梦", "dm7", "dm.jdbc.driver.DmDriver"),
    /**
     * hive
     */
    HIVE("hive", "hive", "org.apache.hive.jdbc.HiveDriver"),
    /**
     * hbase
     */
    HBASE("hbase", "hbase", ""),
    /**
     * mongoDB
     */
    MONGODB("mongodb", "mongodb", ""),
    /**
     * 人大金仓 8.2 8.3 8.6 8.6(86)
     */
    KINGBASE("人大金仓", "kingbase", "meritdata.com.kingbase8.Driver"),

    KINGASE83("人大金仓", "kingbase83", "com.kingbase83.Driver"),

    KINGASE8("人大金仓", "kingbase8", "com.kingbase8.Driver"),
    KINGASE86("人大金仓", "kingbase86", "com.kingbase86.Driver"),


    /**
     * Greenplum
     */
    GREENPLUM("greenplum", "greenplum", "org.postgresql.Driver"),
    /**
     * GaussDB
     */
    DWS("dws", "dws", "org.postgresql.Driver"),
    /**
     * PostgreSQL
     */
    POSTGRESQL("PostgreSQL", "postgresql", "org.postgresql.Driver"),
    /**
     * DB2
     */
    DB2("db2", "db2", "com.ibm.db2.jcc.DB2Driver"),

    ISCASDB("iscasdb", "iscasdb", "com.iscasdb.jdbc.Driver"),

    OSCAR("oscar", "oscar", "com.oscar.Driver"),

    SHENTONG("神通", "shentong", "com.ibm.db2.jcc.DB2Driver"),

    KYLIGENCE("kyligence", "kyligence", "org.apache.kylin.jdbc.Driver"),

    GBASE8A("gbase8a", "gbase8a", "com.gbase.jdbc.Driver"),

    GBASE8T("gbase8t", "gbase8t", "com.meritdata.informix.jdbc.IfxDriver"),

    /**
     * clickHouse
     */
    CLICKHOUSE("clickhouse", "clickhouse", "ru.yandex.clickhouse.ClickHouseDriver"),

    /**
     * hana
     */
    HANA("hana", "hana", "com.sap.db.jdbc.Driver"),

    /**
     * maxcompute
     */
    MAXCOMPUTE("maxcompute", "maxcompute", "com.aliyun.odps.jdbc.OdpsDriver"),

    /**
     * maxcompute
     */
    PRESTO("presto", "presto", "com.facebook.presto.jdbc.PrestoDriver"),

    /**
     * trino
     */
    TRINO("trino", "trino", "io.trino.jdbc.TrinoDriver"),


    /**
     * apidriver
     */
    API("api", "api", "com.meritdata.cloud.system.manager.basic.datasource.utils.ApiDriver"),

    /**
     * vertica
     */
    VERTICA("vertica", "vertica", "com.vertica.jdbc.Driver"),

    /**
     * tidb
     */
    TIDB("tidb", "tidb", "com.mysql.cj.jdbc.Driver"),

    /**
     * tidb
     */
    SGRDB("sgrdb", "sgrdb", "com.mysql.cj.jdbc.Driver"),

    /**
     * tidb
     */
    TERADATA("teradata", "teradata", "com.teradata.jdbc.TeraDriver");

    /**
     * 枚举值
     */
    private final String value;
    /**
     * 枚举值
     */
    private final String driver;
    /**
     * 枚举名称
     */
    private final String name;


    DBDriverTypes(String name, String value, String driver) {
        this.value = value;
        this.driver = driver;
        this.name = name;
    }

    public String getValue() {
        return this.value;
    }

    public String getDriver() {
        return this.driver;
    }

    public String getName() {
        return this.name;
    }

    public boolean is(String value) {
        return this.value.equals(value);
    }

    public static DBDriverTypes of(String value) {
        if (value != null && !"".equals(value)) {
            DBDriverTypes[] types = DBDriverTypes.values();
            for (DBDriverTypes type : types) {
                if (type.value.equalsIgnoreCase(value)) {
                    return type;
                }
            }
        }
        return null;
    }

    public static boolean contains(String value) {
        return of(value) != null;
    }

    @Override
    public String toString() {
        return value;
    }
}
