package com.github.jsjchai.apache.calcite.example;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CalciteSQLExample {

    private static final String SQL = "";
    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        Schema messageSchema = JdbcSchema.create(rootSchema, "pg", postgresqlDataSource(), null, "pg");
        Schema yhSchema = JdbcSchema.create(rootSchema, "oracle", oracleDataSource(), null, "oracle");
        rootSchema.add("pg", messageSchema);
        rootSchema.add("oracle", yhSchema);


        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(SQL);

       while (resultSet.next()){
           Map<String,Object> map =  new HashMap<>();
           map.put("yonghuxm",resultSet.getString("yonghuxm"));
           map.put("lianxidh",resultSet.getString("lianxidh"));
           map.put("id",resultSet.getString("id"));

           System.out.println(map);

       }

        resultSet.close();
        statement.close();
        connection.close();

    }

    private static DataSource postgresqlDataSource() throws ClassNotFoundException {
        return JdbcSchema.dataSource("jdbc:postgresql://127.0.0.1:5432/pg","org.postgresql.Driver","test","test");
    }


    private static DataSource oracleDataSource() throws ClassNotFoundException {
        return JdbcSchema.dataSource("jdbc:oracle:thin:@127.0.0.1:1521:orcl","oracle.jdbc.OracleDriver","test","test");
    }




}
