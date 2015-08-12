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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestDistributedQueries
        extends AbstractTestApproximateQueries
{
    protected AbstractTestDistributedQueries(QueryRunner queryRunner)
    {
        super(queryRunner);
    }

    protected AbstractTestDistributedQueries(QueryRunner queryRunner, Session sampledSession)
    {
        super(queryRunner, sampledSession);
    }

    private void assertCreateTable(String table, @Language("SQL") String query, @Language("SQL") String rowCountQuery)
            throws Exception
    {
        assertCreateTable(table, query, query, rowCountQuery);
    }

    private void assertCreateTable(String table, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
            throws Exception
    {
        assertQuery("CREATE TABLE " + table + " AS " + query, rowCountQuery);
        assertQuery("SELECT * FROM " + table, expectedQuery);
        assertQueryTrue("DROP TABLE " + table);

        assertFalse(queryRunner.tableExists(getSession(), table));
    }

    @Test
    public void testSetSession()
            throws Exception
    {
        MaterializedResult result = computeActual("SET SESSION test_string = 'bar'");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("test_string", "bar"));

        result = computeActual("SET SESSION connector.connector_long = 999");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("connector.connector_long", "999"));

        result = computeActual("SET SESSION connector.connector_string = 'baz'");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("connector.connector_string", "baz"));

        result = computeActual("SET SESSION connector.connector_string = 'ban' || 'ana'");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("connector.connector_string", "banana"));

        result = computeActual("SET SESSION connector.connector_long = 444");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("connector.connector_long", "444"));

        result = computeActual("SET SESSION connector.connector_long = 111 + 111");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("connector.connector_long", "222"));

        result = computeActual("SET SESSION connector.connector_boolean = 111 < 3");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("connector.connector_boolean", "false"));

        result = computeActual("SET SESSION connector.connector_double = 11.1");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("connector.connector_double", "11.1"));
    }

    @Test
    public void testResetSession()
            throws Exception
    {
        MaterializedResult result = computeActual(getSession(), "RESET SESSION test_string");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getResetSessionProperties(), ImmutableSet.of("test_string"));

        result = computeActual(getSession(), "RESET SESSION connector.connector_string");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getResetSessionProperties(), ImmutableSet.of("connector.connector_string"));
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        assertQueryTrue("CREATE TABLE test_create (a bigint, b double, c varchar)");
        assertTrue(queryRunner.tableExists(getSession(), "test_create"));
        assertTableColumnNames("test_create", "a", "b", "c");

        assertQueryTrue("DROP TABLE test_create");
        assertFalse(queryRunner.tableExists(getSession(), "test_create"));

        assertQueryTrue("CREATE TABLE test_create_table_if_not_exists (a bigint, b varchar, c double)");
        assertTrue(queryRunner.tableExists(getSession(), "test_create_table_if_not_exists"));
        assertTableColumnNames("test_create_table_if_not_exists", "a", "b", "c");

        assertQueryTrue("CREATE TABLE IF NOT EXISTS test_create_table_if_not_exists (d bigint, e varchar)");
        assertTrue(queryRunner.tableExists(getSession(), "test_create_table_if_not_exists"));
        assertTableColumnNames("test_create_table_if_not_exists", "a", "b", "c");

        assertQueryTrue("DROP TABLE test_create_table_if_not_exists");
        assertFalse(queryRunner.tableExists(getSession(), "test_create_table_if_not_exists"));
    }

    @Test
    public void testCreateTableAsSelect()
            throws Exception
    {
        assertCreateTable(
                "test_select",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTable(
                "test_group",
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");

        assertCreateTable(
                "test_join",
                "SELECT count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT 1");

        assertCreateTable(
                "test_limit",
                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
                "SELECT 10");

        assertCreateTable(
                "test_unicode",
                "SELECT '\u2603' unicode",
                "SELECT 1");
    }

    @Test
    public void testCreateTableAsSelectSampled()
            throws Exception
    {
        assertCreateTable(
                "test_sampled",
                "SELECT orderkey FROM tpch_sampled.tiny.orders ORDER BY orderkey LIMIT 10",
                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
                "SELECT 10");
    }

    @Test
    public void testRenameTable()
            throws Exception
    {
        assertQueryTrue("CREATE TABLE test_rename AS SELECT 123 x");

        assertQueryTrue("ALTER TABLE test_rename RENAME TO test_rename_new");
        MaterializedResult materializedRows = computeActual("SELECT x FROM test_rename_new");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        // provide new table name in uppercase
        assertQueryTrue("ALTER TABLE test_rename_new RENAME TO TEST_RENAME");
        materializedRows = computeActual("SELECT x FROM test_rename");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertQueryTrue("DROP TABLE test_rename");

        assertFalse(queryRunner.tableExists(getSession(), "test_rename"));
        assertFalse(queryRunner.tableExists(getSession(), "test_rename_new"));
    }

    @Test
    public void testRenameColumn()
            throws Exception
    {
        assertQueryTrue("CREATE TABLE test_rename_column AS SELECT 123 x");

        assertQueryTrue("ALTER TABLE test_rename_column RENAME COLUMN x TO y");
        MaterializedResult materializedRows = computeActual("SELECT y FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertQueryTrue("ALTER TABLE test_rename_column RENAME COLUMN y TO Z");
        materializedRows = computeActual("SELECT z FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertQueryTrue("DROP TABLE test_rename_column");
        assertFalse(queryRunner.tableExists(getSession(), "test_rename_column"));
    }

    @Test
    public void testInsert()
            throws Exception
    {
        @Language("SQL") String query = "SELECT orderdate, orderkey FROM orders";

        assertQuery("CREATE TABLE test_insert AS " + query, "SELECT count(*) FROM orders");
        assertQuery("SELECT * FROM test_insert", query);

        assertQuery("INSERT INTO test_insert " + query, "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM test_insert", query + " UNION ALL " + query);

        assertQueryTrue("DROP TABLE test_insert");
    }

    @Test
    public void testDelete()
            throws Exception
    {
        // delete half the table, then delete the rest

        assertQuery("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertQuery("DELETE FROM test_delete WHERE orderkey % 2 = 0", "SELECT count(*) FROM orders WHERE orderkey % 2 = 0");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE orderkey % 2 <> 0");

        assertQuery("DELETE FROM test_delete", "SELECT count(*) FROM orders WHERE orderkey % 2 <> 0");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders LIMIT 0");

        assertQueryTrue("DROP TABLE test_delete");

        // delete successive parts of the table

        assertQuery("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertQuery("DELETE FROM test_delete WHERE custkey <= 100", "SELECT count(*) FROM orders WHERE custkey <= 100");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE custkey > 100");

        assertQuery("DELETE FROM test_delete WHERE custkey <= 300", "SELECT count(*) FROM orders WHERE custkey > 100 AND custkey <= 300");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE custkey > 300");

        assertQuery("DELETE FROM test_delete WHERE custkey <= 500", "SELECT count(*) FROM orders WHERE custkey > 300 AND custkey <= 500");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE custkey > 500");

        assertQueryTrue("DROP TABLE test_delete");

        // delete using a constant property

        assertQuery("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertQuery("DELETE FROM test_delete WHERE orderstatus = 'O'", "SELECT count(*) FROM orders WHERE orderstatus = 'O'");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE orderstatus <> 'O'");

        assertQueryTrue("DROP TABLE test_delete");

        // delete without matching any rows

        assertQuery("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");
        assertQuery("DELETE FROM test_delete WHERE rand() < 0", "SELECT 0");
        assertQueryTrue("DROP TABLE test_delete");
    }

    @Test
    public void testDeleteSemiJoin()
            throws Exception
    {
        // delete using a subquery

        assertQuery("CREATE TABLE test_delete_semi_join AS SELECT * FROM lineitem", "SELECT count(*) FROM lineitem");

        assertQuery(
                "DELETE FROM test_delete_semi_join WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')",
                "SELECT count(*) FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')");
        assertQuery(
                "SELECT * FROM test_delete_semi_join",
                "SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus <> 'F')");

        assertQueryTrue("DROP TABLE test_delete_semi_join");

        // delete with multiple SemiJoin

        assertQuery("CREATE TABLE test_delete_semi_join AS SELECT * FROM lineitem", "SELECT count(*) FROM lineitem");

        assertQuery(
                "DELETE FROM test_delete_semi_join\n" +
                "WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')\n" +
                "  AND orderkey IN (SELECT orderkey FROM orders WHERE custkey % 5 = 0)\n",
                "SELECT count(*) FROM lineitem\n" +
                "WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')\n" +
                "  AND orderkey IN (SELECT orderkey FROM orders WHERE custkey % 5 = 0)");
        assertQuery(
                "SELECT * FROM test_delete_semi_join",
                "SELECT * FROM lineitem\n" +
                "WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus <> 'F')\n" +
                "  OR orderkey IN (SELECT orderkey FROM orders WHERE custkey % 5 <> 0)");

        assertQueryTrue("DROP TABLE test_delete_semi_join");

        // delete with SemiJoin null handling

        assertQuery("CREATE TABLE test_delete_semi_join AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertQuery(
                "DELETE FROM test_delete_semi_join\n" +
                        "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NULL\n",
                "SELECT count(*) FROM orders\n" +
                        "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NULL\n");
        assertQuery(
                "SELECT * FROM test_delete_semi_join",
                "SELECT * FROM orders\n" +
                        "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NOT NULL\n");

        assertQueryTrue("DROP TABLE test_delete_semi_join");
    }

    @Test
    public void testDropTableIfExists()
            throws Exception
    {
        assertFalse(queryRunner.tableExists(getSession(), "test_drop_if_exists"));
        assertQueryTrue("DROP TABLE IF EXISTS test_drop_if_exists");
        assertFalse(queryRunner.tableExists(getSession(), "test_drop_if_exists"));
    }

    @Test
    public void testView()
            throws Exception
    {
        @Language("SQL") String query = "SELECT orderkey, orderstatus, totalprice / 2 half FROM orders";

        assertQueryTrue("CREATE VIEW test_view AS SELECT 123 x");
        assertQueryTrue("CREATE OR REPLACE VIEW test_view AS " + query);

        assertQuery("SELECT * FROM test_view", query);

        assertQuery(
                "SELECT * FROM test_view a JOIN test_view b on a.orderkey = b.orderkey",
                format("SELECT * FROM (%s) a JOIN (%s) b ON a.orderkey = b.orderkey", query, query));

        assertQuery("WITH orders AS (SELECT * FROM orders LIMIT 0) SELECT * FROM test_view", query);

        String name = format("%s.%s.test_view", getSession().getCatalog(), getSession().getSchema());
        assertQuery("SELECT * FROM " + name, query);

        assertQueryTrue("DROP VIEW test_view");
    }

    @Test
    public void testViewMetadata()
            throws Exception
    {
        @Language("SQL") String query = "SELECT 123 x, 'foo' y";
        assertQueryTrue("CREATE VIEW meta_test_view AS " + query);

        // test INFORMATION_SCHEMA.TABLES
        MaterializedResult actual = computeActual(format(
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = '%s'",
                getSession().getSchema()));

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("customer", "BASE TABLE")
                .row("lineitem", "BASE TABLE")
                .row("meta_test_view", "VIEW")
                .row("nation", "BASE TABLE")
                .row("orders", "BASE TABLE")
                .row("part", "BASE TABLE")
                .row("partsupp", "BASE TABLE")
                .row("region", "BASE TABLE")
                .row("supplier", "BASE TABLE")
                .build();

        assertContains(actual, expected);

        // test SHOW TABLES
        actual = computeActual("SHOW TABLES");

        MaterializedResult.Builder builder = resultBuilder(getSession(), actual.getTypes());
        for (MaterializedRow row : expected.getMaterializedRows()) {
            builder.row(row.getField(0));
        }
        expected = builder.build();

        assertContains(actual, expected);

        // test INFORMATION_SCHEMA.VIEWS
        actual = computeActual(format(
                "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = '%s'",
                getSession().getSchema()));

        expected = resultBuilder(getSession(), actual.getTypes())
                .row("meta_test_view", formatSql(new SqlParser().createStatement(query)))
                .build();

        assertContains(actual, expected);

        // test SHOW COLUMNS
        actual = computeActual("SHOW COLUMNS FROM meta_test_view");

        expected = resultBuilder(getSession(), VARCHAR, VARCHAR, BOOLEAN, BOOLEAN, VARCHAR)
                .row("x", "bigint", true, false, "")
                .row("y", "varchar", true, false, "")
                .build();

        assertEquals(actual, expected);

        assertQueryTrue("DROP VIEW meta_test_view");
    }

    @Test
    public void testLargeQuerySuccess()
            throws Exception
    {
        assertQuery("SELECT " + Joiner.on(" AND ").join(nCopies(500, "1 = 1")), "SELECT true");
    }

    @Test
    public void testShowSchemasFromOther()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM tpch");
        ImmutableSet<String> schemaNames = ImmutableSet.copyOf(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertTrue(schemaNames.containsAll(ImmutableSet.of(INFORMATION_SCHEMA, "tiny", "sf1")));
    }

    @Test
    public void testTableSampleSystem()
            throws Exception
    {
        int total = computeActual("SELECT orderkey FROM orders").getMaterializedRows().size();

        boolean sampleSizeFound = false;
        for (int i = 0; i < 100; i++) {
            int sampleSize = computeActual("SELECT orderkey FROM ORDERS TABLESAMPLE SYSTEM (50)").getMaterializedRows().size();
            if (sampleSize > 0 && sampleSize < total) {
                sampleSizeFound = true;
                break;
            }
        }
        assertTrue(sampleSizeFound, "Table sample returned unexpected number of rows");
    }

    @Test
    public void testTableSampleSystemBoundaryValues()
            throws Exception
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (0)");
        MaterializedResult all = computeActual("SELECT orderkey FROM orders");

        assertContains(all, fullSample);
        assertEquals(emptySample.getMaterializedRows().size(), 0);
    }

    @Override
    @Test
    public void testTableSamplePoissonizedRescaled()
            throws Exception
    {
        MaterializedResult sample = computeActual("SELECT * FROM orders TABLESAMPLE POISSONIZED (10) RESCALED");
        MaterializedResult all = computeExpected("SELECT * FROM orders", sample.getTypes());

        assertTrue(!sample.getMaterializedRows().isEmpty());
        assertContains(all, sample);
    }

    @Test
    public void testSymbolAliasing()
            throws Exception
    {
        assertQueryTrue("CREATE TABLE test_symbol_aliasing AS SELECT 1 foo_1, 2 foo_2_4");
        assertQuery("SELECT foo_1, foo_2_4 FROM test_symbol_aliasing", "SELECT 1, 2");
        assertQueryTrue("DROP TABLE test_symbol_aliasing");
    }

    private void assertTableColumnNames(String tableName, String... columnNames)
    {
        MaterializedResult result = computeActual("DESCRIBE " + tableName);
        List<String> expected = ImmutableList.copyOf(columnNames);
        List<String> actual = result.getMaterializedRows().stream()
            .map(row -> (String) row.getField(0))
            .collect(toImmutableList());
        assertEquals(actual, expected);
    }
}
