/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jsjchai.apache.calcite.example.es;

import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.jsjchai.apache.calcite.example.util.ResultSetUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Set of tests for ES adapter. Uses real instance via {@link EmbeddedElasticsearchPolicy}. Document
 * source is local {@code zips-mini.json} file (located in test classpath).
 */
@ResourceLock(value = "elasticsearch-scrolls", mode = ResourceAccessMode.READ)
class ElasticSearchAdapterTest {

    public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

    /**
     * Default index/type name.
     */
    private static final String ZIPS = "zips";
    private static final String ZIPS_ALIAS = "zips_alias";
    private static final int ZIPS_SIZE = 149;

    /**
     * Used to create {@code zips} index and insert zip data in bulk.
     *
     * @throws Exception when instance setup failed
     */
    @BeforeAll
    public static void setupInstance() throws Exception {
        final Map<String, String> mapping =
                ImmutableMap.of("city", "keyword", "state", "keyword", "pop", "long");

        NODE.createIndex(ZIPS, mapping);
        NODE.createAlias(ZIPS, ZIPS_ALIAS);

        // load records from file
        final List<ObjectNode> bulk = new ArrayList<>();
        Resources.readLines(ElasticSearchAdapterTest.class.getResource("/es/zips-mini.json"),
                StandardCharsets.UTF_8, new LineProcessor<Void>() {
                    @Override
                    public boolean processLine(String line) throws IOException {
                        line = line.replace("_id", "id"); // _id is a reserved attribute in ES
                        bulk.add((ObjectNode) NODE.mapper().readTree(line));
                        return true;
                    }

                    @Override
                    public Void getResult() {
                        return null;
                    }
                });

        if (bulk.isEmpty()) {
            throw new IllegalStateException("No records to index. Empty file ?");
        }

        NODE.insertBulk(ZIPS, bulk);
    }

    private static Connection createConnection() throws SQLException {
        final Connection connection =
                DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        final SchemaPlus root =
                connection.unwrap(CalciteConnection.class).getRootSchema();

        root.add("elastic",
                new ElasticsearchSchema(NODE.restClient(), NODE.mapper(), null));

        // add calcite view programmatically
        final String viewSql = "select cast(_MAP['city'] AS varchar(20)) AS \"city\", "
                + " cast(_MAP['loc'][0] AS float) AS \"longitude\",\n"
                + " cast(_MAP['loc'][1] AS float) AS \"latitude\",\n"
                + " cast(_MAP['pop'] AS integer) AS \"pop\", "
                + " cast(_MAP['state'] AS varchar(2)) AS \"state\", "
                + " cast(_MAP['id'] AS varchar(5)) AS \"id\" "
                + "from \"elastic\".\"zips\"";

        root.add("zips",
                ViewTable.viewMacro(root, viewSql,
                        Collections.singletonList("elastic"),
                        Arrays.asList("elastic", "view"), false));

        return connection;
    }

    @Test
    void testDisableSSL() throws SQLException {
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        final SchemaPlus root =
                connection.unwrap(CalciteConnection.class).getRootSchema();

        final CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);

        final ElasticsearchSchemaFactory esSchemaFactory = new ElasticsearchSchemaFactory();
        Map<String, Object> options = new HashMap<>();
        String hosts = "[\"" + NODE.restClient().getNodes()
                .get(0).getHost().toString() + "\"]";
        options.put("username", "user1");
        options.put("password", "password");
        options.put("pathPrefix", "");
        options.put("disableSSLVerification", "true");
        options.put("hosts", hosts);

        final Schema esSchmea =
                esSchemaFactory.create(calciteConnection.getRootSchema(), "es_no_ssl", options);

        assertNotNull(esSchmea);
    }

    @Test
    void view() throws SQLException {
        Connection connection = createConnection();
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from zips where city = 'BROOKLYN'");

        List<Entity> data = ResultSetUtil.toEntityList(resultSet);

        DbUtil.close(statement);
        DbUtil.close(connection);

        assertNotNull(data);
    }


    /**
     * Throws {@code AssertionError} if result set is not sorted by {@code column}.
     * {@code null}s are ignored.
     *
     * @param column    column to be extracted (as comparable object).
     * @param direction ascending / descending
     * @return consumer which throws exception
     */
    private static Consumer<ResultSet> sortedResultSetChecker(String column,
                                                              RelFieldCollation.Direction direction) {
        requireNonNull(column, "column");
        return rset -> {
            try {
                final List<Comparable<?>> states = new ArrayList<>();
                while (rset.next()) {
                    Object object = rset.getObject(column);
                    if (object != null && !(object instanceof Comparable)) {
                        final String message = String.format(Locale.ROOT, "%s is not comparable", object);
                        throw new IllegalStateException(message);
                    }
                    if (object != null) {
                        //noinspection rawtypes
                        states.add((Comparable) object);
                    }
                }
                for (int i = 0; i < states.size() - 1; i++) {
                    //noinspection rawtypes
                    final Comparable current = states.get(i);
                    //noinspection rawtypes
                    final Comparable next = states.get(i + 1);
                    //noinspection unchecked
                    final int cmp = current.compareTo(next);
                    if (direction == RelFieldCollation.Direction.ASCENDING ? cmp > 0 : cmp < 0) {
                        final String message =
                                String.format(Locale.ROOT,
                                        "Column %s NOT sorted (%s): %s (index:%d) > %s (index:%d) count: %d",
                                        column, direction, current, i, next, i + 1, states.size());
                        throw new AssertionError(message);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        };
    }


}
