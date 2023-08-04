package com.github.jsjchai.apache.calcite.example.main;

import cn.hutool.core.date.StopWatch;
import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import com.github.jsjchai.apache.calcite.example.config.JdbcConfig;
import com.github.jsjchai.apache.calcite.example.util.ResultSetUtil;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 *  RelBuilder拼接sql oracle join postgresql
 */
public class CalciteJdbcExample {

    private static final Logger LOGGER = Logger.getLogger(CalciteJdbcExample.class.getName());

    private static String oracleSchema;
    private static String postgresqlSchema;

    private static final String ORACLE_TABLE_NAME = "table1";
    private static final String POSTGRESQL_TABLE_NAME = "table2";


    private static void init() {
        oracleSchema = JdbcConfig.getOracleSchema();
        postgresqlSchema = JdbcConfig.getPostgresqlSchema();
    }

    public static void main(String[] args) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start("总耗时");
        init();

        //设置连接参数
        Properties info = new Properties();
        //不区分sql大小写
        info.setProperty("caseSensitive", "false");
        info.setProperty("lex", "mysql");


        // Build our connection
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

        // Unwrap our connection using the CalciteConnection
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        // Get a pointer to our root schema for our Calcite Connection
        SchemaPlus rootSchema = calciteConnection.getRootSchema();


        // Attach our Postgres Jdbc Datasource to our Root Schema
        rootSchema.add(postgresqlSchema, JdbcSchema.create(rootSchema, postgresqlSchema, JdbcConfig.postgresqlDataSource(), null, postgresqlSchema));

        // Attach our MySQL Jdbc Datasource to our Root Schema
        rootSchema.add(oracleSchema, JdbcSchema.create(rootSchema, oracleSchema, JdbcConfig.oracleDataSource(), null, oracleSchema));


        // Build a framework config to attach to our Calcite Planners and  Optimizers
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();

        RelBuilder rb = RelBuilder.create(config);

        // First parameter is the Schema, the second is the table name
        RelNode left = rb.scan(oracleSchema, ORACLE_TABLE_NAME).build();
        RelNode right = rb.scan(postgresqlSchema, POSTGRESQL_TABLE_NAME).build();

        RelNode node = rb
                .push(left)
                .push(right)
                .join(JoinRelType.INNER,
                        rb.and(
                                rb.equals(rb.field(2, ORACLE_TABLE_NAME, "col1"), rb.field(2, POSTGRESQL_TABLE_NAME, "col1")),
                                rb.equals(rb.field(2, ORACLE_TABLE_NAME, "col2"), rb.field(2, POSTGRESQL_TABLE_NAME, "col2"))
                        )
                )
                // If you want to select from more than one table, you can do so by adding a second scan parameter
                .filter(
                        rb.greaterThan(rb.field("id"), rb.literal("1"))
                )
                // These are the fields you want to return from your query
                .project(
                        rb.field("id"),
                        rb.field("col1"),
                        rb.field("col2"),
                        rb.field("col3")

                )
                .build();

        //String joinCondition = node.explain();

        LOGGER.info("优化器：" + RelOptUtil.toString(node));

        HepProgram program = HepProgram.builder().build();
        HepPlanner planner = new HepPlanner(program);

        planner.setRoot(node);

        RelNode optimizedNode = planner.findBestExp();

        final RelShuttle shuttle = new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(TableScan scan) {
                final RelOptTable table = scan.getTable();
                if (scan instanceof LogicalTableScan && Bindables.BindableTableScan.canHandle(table)) {
                    return Bindables.BindableTableScan.create(scan.getCluster(), table);
                }
                return super.visit(scan);
            }
        };

        optimizedNode = optimizedNode.accept(shuttle);

        StopWatch sqlWatch = new StopWatch();
        sqlWatch.start("执行sql");

        final RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement ps = runner.prepareStatement(optimizedNode);

        ps.execute();

        ResultSet resultSet = ps.getResultSet();

        List<Entity> data = ResultSetUtil.toEntityList(resultSet);

        data.forEach(e -> LOGGER.info(e.toString()));

        DbUtil.close(ps);
        DbUtil.close(connection);

        watch.stop();
        sqlWatch.stop();


        LOGGER.info("总数量：" + data.size());
        LOGGER.info(watch.prettyPrint(TimeUnit.SECONDS));
        LOGGER.info(sqlWatch.prettyPrint(TimeUnit.SECONDS));
    }
}
