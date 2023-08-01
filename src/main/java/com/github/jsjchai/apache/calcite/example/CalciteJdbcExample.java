package com.github.jsjchai.apache.calcite.example;

import cn.hutool.db.DbUtil;
import cn.hutool.setting.dialect.Props;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class CalciteJdbcExample {

    private static final String POSTGRESQL_SCHEMA = "pg";

    private static final String MYSQL_SCHEMA = "mysql";

    private static DataSource postgresDataSource;

    private static DataSource mysqlDataSource;

    private static final Props DB_PROPS = new Props("db_dev.properties");


    private static void init() {

        // Instantiate a data source, this can be autowired in using Spring as well
        postgresDataSource = JdbcSchema.dataSource(
                DB_PROPS.getStr("datasource.postgresql.jdbc.url"),
                // Change this if you want to use something like MySQL, Oracle, etc.
                DB_PROPS.getStr("datasource.postgresql.driver-class-name"),
                // username
                DB_PROPS.getStr("datasource.postgresql.username"),
                // password
                DB_PROPS.getStr("datasource.postgresql.password")
        );

        // Instantiate a data source, this can be autowired in using Spring as well
        mysqlDataSource = JdbcSchema.dataSource(
                DB_PROPS.getStr("datasource.mysql.jdbc.url"),
                // Change this if you want to use something like MySQL, Oracle, etc.
                DB_PROPS.getStr("datasource.mysql.driver-class-name"),
                // username
                DB_PROPS.getStr("datasource.mysql.username"),
                // password
                DB_PROPS.getStr("datasource.mysql.password")
        );
    }

    public static void main(String[] args) throws Exception {

        init();

        // Build our connection
        Connection connection = DriverManager.getConnection("jdbc:calcite:");

        // Unwrap our connection using the CalciteConnection
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        // Get a pointer to our root schema for our Calcite Connection
        SchemaPlus rootSchema = calciteConnection.getRootSchema();


        // Attach our Postgres Jdbc Datasource to our Root Schema
        rootSchema.add(POSTGRESQL_SCHEMA, JdbcSchema.create(rootSchema, POSTGRESQL_SCHEMA, postgresDataSource, null, POSTGRESQL_SCHEMA));

        // Attach our MySQL Jdbc Datasource to our Root Schema
        rootSchema.add(MYSQL_SCHEMA, JdbcSchema.create(rootSchema, MYSQL_SCHEMA, mysqlDataSource, null, MYSQL_SCHEMA));


        // Build a framework config to attach to our Calcite Planners and  Optimizers
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();

        RelBuilder rb = RelBuilder.create(config);

        RelNode node = rb
                // First parameter is the Schema, the second is the table name
                .scan(POSTGRESQL_SCHEMA, "pg_table_name")
                .scan(MYSQL_SCHEMA, "mysql_table_name")
                // If you want to select from more than one table, you can do so by adding a second scan parameter
                .filter(
                        rb.equals(rb.field("field"), rb.literal("literal"))
                )
                // These are the fields you want to return from your query
                .project(
                        rb.field("id"),
                        rb.field("col1"),
                        rb.field("col2")
                )
                .build();


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

        final RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement ps = runner.prepareStatement(optimizedNode);

        ps.execute();

        //ResultSet resultSet = ps.getResultSet();

        DbUtil.close(ps);
    }
}
