package com.github.jsjchai.apache.calcite.example;

import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import cn.hutool.db.handler.EntityListHandler;
import cn.hutool.db.handler.RsHandler;
import cn.hutool.db.sql.SqlExecutor;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;



class CalciteTests {

    private String jdbcUrl;
    private Connection connection;
    private DataSource mockDataSource;

    @BeforeEach
    public void setupTests() throws SQLException {
        jdbcUrl = MockDb.INSTANCE.getUrl();
        connection = DriverManager.getConnection(jdbcUrl, "", "");

        // You can populate test data into the database like so:
        try (Statement s = connection.createStatement()) {
            s.execute("create table mytemptable(" +
                    "id integer not null primary key," +
                    "exampleFoo varchar(25)," +
                    "exampleBar varchar(25))");

            s.execute("insert into mytemptable values(1, 'test', '1234')");
            s.execute("insert into mytemptable values(2, 'test2', 'xyz')");
        }

        mockDataSource = JdbcSchema.dataSource(jdbcUrl, "org.hsqldb.jdbcDriver", "", "");
    }

    @Test
    void calciteUnitTestExample() throws SQLException {

        // Build our connection
        Connection connection = DriverManager.getConnection("jdbc:calcite:");

        // Unwrap our connection using the CalciteConnection
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        // Get a pointer to our root schema for our Calcite Connection
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // Attach our Postgres Jdbc Datasource to our Root Schema
        rootSchema.add("exampleSchema", JdbcSchema.create(rootSchema, "exampleSchema", mockDataSource, null, null));

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();

        RelBuilder r = RelBuilder.create(config);

        RelNode node = r
                // First parameter is the Schema, the second is the table name
                .scan("exampleSchema", "MYTEMPTABLE")
                // If you want to select from more than one table, you can do so by adding a second scan parameter
                .filter(
                        r.equals(r.field("ID"), r.literal(2))
                )
                // These are the fields you want to return from your query
                .project(
                        r.field("ID"),
                        r.field("EXAMPLEFOO"),
                        r.field("EXAMPLEBAR")
                )
                .build();

        RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement ps = runner.prepareStatement(node);

        RsHandler<List<Entity>> handler = new EntityListHandler();

        List<Entity> list = SqlExecutor.query(ps,handler);
        list.forEach(System.out::println);
        DbUtil.close(ps);
        DbUtil.close(connection);

    }

    static class MockDb {
        MockDb() {}
        static final MockDb INSTANCE = new MockDb();
        private final AtomicInteger id = new AtomicInteger(1);

        public String getUrl() {
            return "jdbc:hsqldb:mem:db" + id.getAndIncrement();
        }
    }
}