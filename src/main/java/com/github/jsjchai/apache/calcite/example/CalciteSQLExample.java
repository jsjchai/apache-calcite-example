package com.github.jsjchai.apache.calcite.example;

import cn.hutool.core.date.StopWatch;
import cn.hutool.core.io.FileUtil;
import cn.hutool.db.Entity;
import com.github.jsjchai.apache.calcite.example.config.JdbcConfig;
import com.github.jsjchai.apache.calcite.example.util.ResultSetUtil;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class CalciteSQLExample {

    private static final Logger LOGGER = Logger.getLogger(CalciteSQLExample.class.getName());

    private static String sql;

    private static String oracleSchema;
    private static String postgresqlSchema;


    public static void main(String[] args) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start("总耗时");
        init();
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        Schema messageSchema = JdbcSchema.create(rootSchema, postgresqlSchema, JdbcConfig.postgresqlDataSource(), null, postgresqlSchema);
        Schema yhSchema = JdbcSchema.create(rootSchema, oracleSchema, JdbcConfig.oracleDataSource(), null, oracleSchema);
        rootSchema.add(postgresqlSchema, messageSchema);
        rootSchema.add(oracleSchema, yhSchema);

        StopWatch sqlWatch = new StopWatch();
        sqlWatch.start("执行sql");

        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        List<Entity> data = ResultSetUtil.toEntityList(resultSet);

        data.forEach(e -> LOGGER.info(e.toString()));

        resultSet.close();
        statement.close();
        connection.close();

        watch.stop();
        sqlWatch.stop();
        LOGGER.info( watch.prettyPrint(TimeUnit.SECONDS));
        LOGGER.info( sqlWatch.prettyPrint(TimeUnit.SECONDS));

    }

    private static void init() {
        sql = FileUtil.readUtf8String("sql/postgresqljoinOracle.sql");
        oracleSchema = JdbcConfig.getOracleSchema();
        postgresqlSchema = JdbcConfig.getPostgresqlSchema();
    }


}
