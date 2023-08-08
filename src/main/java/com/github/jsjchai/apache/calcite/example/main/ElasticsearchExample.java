package com.github.jsjchai.apache.calcite.example.main;

import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsjchai.apache.calcite.example.util.ResultSetUtil;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.logging.Logger;

public class ElasticsearchExample {

    private static final Logger LOGGER = Logger.getLogger(ElasticsearchExample.class.getName());

    public static void main(String[] args) throws SQLException, IOException {

        //es client
        RestClient client = RestClient.builder(HttpHost.create("http://127.0.0.1:9200")).build();

        final Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");

        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        final SchemaPlus root = calciteConnection.getRootSchema();

        root.add("elastic", new ElasticsearchSchema(client, new ObjectMapper(), null));


        // add calcite view programmatically
        final String sql = "select cast(_MAP['city'] AS varchar(20)) AS city, "
                + " cast(_MAP['loc'][0] AS float) AS longitude,"
                + " cast(_MAP['loc'][1] AS float) AS latitude,"
                + " cast(_MAP['pop'] AS integer) AS pop, "
                + " cast(_MAP['state'] AS varchar(2)) AS state, "
                + " cast(_MAP['id'] AS varchar(5)) AS id "
                + "from elastic.zips";

        Statement statement = calciteConnection.createStatement();

        ResultSet resultSet = statement.executeQuery(sql);

        List<Entity> data = ResultSetUtil.toEntityList(resultSet);

        LOGGER.info("size:"+data.size());

        DbUtil.close(statement);
        DbUtil.close(connection);

        client.close();
    }
}
