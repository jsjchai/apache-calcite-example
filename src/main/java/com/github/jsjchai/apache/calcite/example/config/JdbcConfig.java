package com.github.jsjchai.apache.calcite.example.config;

import cn.hutool.setting.dialect.Props;
import org.apache.calcite.adapter.jdbc.JdbcSchema;

import javax.sql.DataSource;

/**
 * 配置数据源
 */
public class JdbcConfig {


    private static final Props DB_PROPS = new Props("db.properties");


    public static DataSource postgresqlDataSource() {
        String url = DB_PROPS.getStr("datasource.postgresql.jdbc.url");
        String driverClassName = DB_PROPS.getStr("datasource.postgresql.driver-class-name");
        String username = DB_PROPS.getStr("datasource.postgresql.username");
        String password = DB_PROPS.getStr("datasource.postgresql.password");
        return JdbcSchema.dataSource(url, driverClassName, username, password);
    }


    public static DataSource oracleDataSource() {
        String url = DB_PROPS.getStr("datasource.oracle.jdbc.url");
        String driverClassName = DB_PROPS.getStr("datasource.oracle.driver-class-name");
        String username = DB_PROPS.getStr("datasource.oracle.username");
        String password = DB_PROPS.getStr("datasource.oracle.password");
        return JdbcSchema.dataSource(url, driverClassName, username, password);
    }


    public static DataSource mysqlSource() {
        String url = DB_PROPS.getStr("datasource.mysql.jdbc.url");
        String driverClassName = DB_PROPS.getStr("datasource.mysql.driver-class-name");
        String username = DB_PROPS.getStr("datasource.mysql.username");
        String password = DB_PROPS.getStr("datasource.mysql.password");
        return JdbcSchema.dataSource(url, driverClassName, username, password);
    }

    public static String getOracleSchema(){
        return DB_PROPS.getStr("oracle.schema");
    }

    public static String getPostgresqlSchema(){
        return DB_PROPS.getStr("postgresql.schema");
    }


    public static String getMysqlSchema(){
        return DB_PROPS.getStr("mysql.schema");
    }
}
