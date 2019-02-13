/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.mysql.TestingMySqlServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestMySqlIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingMySqlServer mysqlServer;

    public TestMySqlIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingMySqlServer("testuser", "testpass", "tpch", "test_database"));
    }

    public TestMySqlIntegrationSmokeTest(TestingMySqlServer mysqlServer)
    {
        super(() -> createMySqlQueryRunner(mysqlServer, ORDERS));
        this.mysqlServer = mysqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        mysqlServer.close();
    }

    @Override
    public void testDescribeTable()
    {
        // we need specific implementation of this tests due to specific Presto<->Mysql varchar length mapping.
        MaterializedResult actualColumns = computeActual("DESC ORDERS").toTestTypes();

        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(255)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(255)", "", "")
                .row("clerk", "varchar(255)", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar(255)", "", "")
                .build();
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    public void testDropTable()
    {
        assertUpdate("CREATE TABLE test_drop AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_drop"));

        assertUpdate("DROP TABLE test_drop");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop"));
    }

    @Test
    public void testViews()
            throws SQLException
    {
        execute("CREATE OR REPLACE VIEW tpch.test_view AS SELECT * FROM tpch.orders");
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    @Test
    public void testInsert()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_insert (x bigint, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testNameEscaping()
    {
        Session session = testSessionBuilder()
                .setCatalog("mysql")
                .setSchema("test_database")
                .build();

        assertFalse(getQueryRunner().tableExists(session, "test_table"));

        assertUpdate(session, "CREATE TABLE test_table AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(session, "test_table"));

        assertQuery(session, "SELECT * FROM test_table", "SELECT 123");

        assertUpdate(session, "DROP TABLE test_table");
        assertFalse(getQueryRunner().tableExists(session, "test_table"));
    }

    @Test
    public void testMySqlTinyint1()
            throws Exception
    {
        execute("CREATE TABLE tpch.mysql_test_tinyint1 (c_tinyint tinyint(1))");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM mysql_test_tinyint1");
        MaterializedResult expected = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("c_tinyint", "tinyint", "", "")
                .build();

        assertEquals(actual, expected);

        execute("INSERT INTO tpch.mysql_test_tinyint1 VALUES (127), (-128)");
        MaterializedResult materializedRows = computeActual("SELECT * FROM tpch.mysql_test_tinyint1 WHERE c_tinyint = 127");
        assertEquals(materializedRows.getRowCount(), 1);
        MaterializedRow row = getOnlyElement(materializedRows);

        assertEquals(row.getFields().size(), 1);
        assertEquals(row.getField(0), (byte) 127);

        assertUpdate("DROP TABLE mysql_test_tinyint1");
    }

    @Test
    public void testCharTrailingSpace()
            throws Exception
    {
        execute("CREATE TABLE tpch.char_trailing_space (x char(10))");
        assertUpdate("INSERT INTO char_trailing_space VALUES ('test')", 1);

        assertQuery("SELECT * FROM char_trailing_space WHERE x = CHAR 'test'", "VALUES 'test'");
        assertQuery("SELECT * FROM char_trailing_space WHERE x = CHAR 'test  '", "VALUES 'test'");
        assertQuery("SELECT * FROM char_trailing_space WHERE x = CHAR 'test        '", "VALUES 'test'");

        assertEquals(getQueryRunner().execute("SELECT * FROM char_trailing_space WHERE x = CHAR ' test'").getRowCount(), 0);

        Map<String, String> properties = ImmutableMap.of("deprecated.legacy-char-to-varchar-coercion", "true");
        Map<String, String> connectorProperties = ImmutableMap.of("connection-url", mysqlServer.getJdbcUrl());

        try (QueryRunner queryRunner = new DistributedQueryRunner(getSession(), 3, properties);) {
            queryRunner.installPlugin(new MySqlPlugin());
            queryRunner.createCatalog("mysql", "mysql", connectorProperties);

            assertEquals(queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = CHAR 'test'").getRowCount(), 0);
            assertEquals(queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = CHAR 'test  '").getRowCount(), 0);
            assertEquals(queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = CHAR 'test       '").getRowCount(), 0);

            MaterializedResult result = queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = CHAR 'test      '");
            assertEquals(result.getRowCount(), 1);
            assertEquals(result.getMaterializedRows().get(0).getField(0), "test      ");
        }

        assertUpdate("DROP TABLE char_trailing_space");
    }

    @Test
    public void testAggregationPushdown()
            throws SQLException
    {
        execute("CREATE TABLE tpch.test_aggregation (\n" +
                "c1 bigint,\n" +
                "c2 bigint,\n" +
                "c3 varchar(100),\n" +
                "c4 double,\n" +
                "c5 varchar(100))");
        assertUpdate("INSERT INTO test_aggregation VALUES\n" +
                        "(1, 100, 'test', 1.0, 'test'),\n" +
                        "(1, 100, 'test', 3.0, 'test'),\n" +
                        "(1, 100, 'test', 3.0, 'tes'),\n" +
                        "(1, 100, 'test', 3.0, 'tets'),\n" +
                        "(2, 99, 'test', 3.0, 'test'),\n" +
                        "(2, 100, 'test', 2.0, 'test'),\n" +
                        "(3, 10, 'ignored', 2.0, 'test'),\n" +
                        "(101, 10, 'ignored', 2.0, 'test'),\n" +
                        "(101, -10, 'test', 2.0, 'test'),\n" +
                        "(4, 100, 'ignored', 2.0, 'ignored')\n",
                10);
        MaterializedResult actual;
//                actual = computeActual(
//                "SELECT c1+c2,  sum(c4), IF(c1>c2, c3), avg(c4), \n" +
//                        "  count(c3), sum(c4), approx_distinct(c1, 0.26 + 0.01)\n" +
//                        "FROM test_aggregation\n" +
//                        "WHERE c1 > 100 AND c1+c2 > 100 AND c3 IN ('test')\n" +
//                        "GROUP BY 1, 3, c1");
        actual = computeActual(
                "SELECT c1+c2,  sum(c4), IF(c1<c2, c3), \n" +
                        "  COUNT(c3), sum(c3), avg(c4)\n" +
                        "FROM test_aggregation\n" +
                        "WHERE c1 < 100 AND c1+c2 > 100 AND c3 IN ('test') AND c5 LIKE 'tes%'\n" +
                        "GROUP BY 1, 3, c1");
        assertEquals(actual.getRowCount(), 3);
        //Test like pushdown
        //Test avg --> yield Row(sum, count)

//        actual = computeActual(
//                "SELECT c1+c2,  sum(c4), IF(c1>c2, c3), avg(c4), \n" +
//                        "  count(c3), sum(c4) FILTER (WHERE c1 > 1)\n" +
//                        "FROM test_aggregation\n" +
//                        "WHERE c1 > 100 AND c1+c2 > 100 AND c3 IN ('test')\n" +
//                        "GROUP BY 1, 3, c1");

//        actual = computeActual(
//                "SELECT c1+c2,  sum(c4), IF(c1>c2, c3), approx_distinct(c5), \n" +
//                        " array_agg(c4 order by c1), count(c3), sum(c4) FILTER (WHERE c1 > 1)\n" +
//                        "FROM test_aggregation\n" +
//                        "WHERE c1 > 100 AND c1+c2 > 100 AND c3 IN ('test')\n" +
//                        "GROUP BY 1, 3, c1");

//        actual = computeActual(
//                "SELECT sum(c4), approx_distinct(c5), \n" +
//                        " array_agg(c4 order by c1), count(c3), sum(c4) FILTER (WHERE c1 > 1)\n" +
//                        "FROM test_aggregation\n" +
//                        "WHERE c1 > 100 AND c1+c2 > 100 AND c3 IN ('test')\n" +
//                        "GROUP BY \n" +
//                        " GROUPING SETS (\n" +
//                        " (c1), (c1, c2)" +
//                        ")");
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(mysqlServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
