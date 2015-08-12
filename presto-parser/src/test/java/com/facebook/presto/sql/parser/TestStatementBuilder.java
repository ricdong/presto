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
package com.facebook.presto.sql.parser;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.sql.testing.TreeAssertions.assertFormattedSql;
import static com.google.common.base.Strings.repeat;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertFalse;

public class TestStatementBuilder
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testStatementBuilder()
            throws Exception
    {
        printStatement("select * from foo");
        printStatement("explain select * from foo");
        printStatement("explain (type distributed, format graphviz) select * from foo");

        printStatement("select * from foo /* end */");
        printStatement("/* start */ select * from foo");
        printStatement("/* start */ select * /* middle */ from foo /* end */");
        printStatement("-- start\nselect * -- junk\n-- hi\nfrom foo -- done");

        printStatement("select * from foo a (x, y, z)");

        printStatement("select *, 123, * from foo");

        printStatement("select show from foo");
        printStatement("select extract(day from x), extract(dow from x) from y");

        printStatement("select 1 + 13 || '15' from foo");

        printStatement("select x is distinct from y from foo where a is not distinct from b");

        printStatement("select x[1] from my_table");
        printStatement("select x[1][2] from my_table");
        printStatement("select x[cast(10 * sin(x) as bigint)] from my_table");

        printStatement("select * from unnest(t.my_array)");
        printStatement("select * from unnest(array[1, 2, 3])");
        printStatement("select x from unnest(array[1, 2, 3]) t(x)");
        printStatement("select * from users cross join unnest(friends)");
        printStatement("select id, friend from users cross join unnest(friends) t(friend)");
        printStatement("select * from unnest(t.my_array) with ordinality");
        printStatement("select * from unnest(array[1, 2, 3]) with ordinality");
        printStatement("select x from unnest(array[1, 2, 3]) with ordinality t(x)");
        printStatement("select * from users cross join unnest(friends) with ordinality");
        printStatement("select id, friend from users cross join unnest(friends) with ordinality t(friend)");

        printStatement("" +
                "select depname, empno, salary\n" +
                ", count(*) over ()\n" +
                ", avg(salary) over (partition by depname)\n" +
                ", rank() over (partition by depname order by salary desc)\n" +
                ", sum(salary) over (order by salary rows unbounded preceding)\n" +
                ", sum(salary) over (partition by depname order by salary rows between current row and 3 following)\n" +
                ", sum(salary) over (partition by depname range unbounded preceding)\n" +
                ", sum(salary) over (rows between 2 preceding and unbounded following)\n" +
                "from emp");

        printStatement("" +
                "with a (id) as (with x as (select 123 from z) select * from x) " +
                "   , b (id) as (select 999 from z) " +
                "select * from a join b using (id)");

        printStatement("with recursive t as (select * from x) select * from t");

        printStatement("select * from information_schema.tables");

        printStatement("show catalogs");

        printStatement("show schemas");
        printStatement("show schemas from sys");

        printStatement("show tables");
        printStatement("show tables from information_schema");
        printStatement("show tables like '%'");
        printStatement("show tables from information_schema like '%'");

        printStatement("show partitions from foo");
        printStatement("show partitions from foo where name = 'foo'");
        printStatement("show partitions from foo order by x");
        printStatement("show partitions from foo limit 10");
        printStatement("show partitions from foo order by x desc limit 10");

        printStatement("show functions");

        printStatement("select cast('123' as bigint), try_cast('foo' as bigint)");

        printStatement("select * from a.b.c");
        printStatement("select * from a.b.c.e.f.g");

        printStatement("select \"TOTALPRICE\" \"my price\" from \"ORDERS\"");

        printStatement("select * from foo tablesample system (10+1)");
        printStatement("select * from foo tablesample system (10) join bar tablesample bernoulli (30) on a.id = b.id");

        printStatement("select * from foo tablesample bernoulli (10) stratify on (id)");
        printStatement("select * from foo tablesample system (50) stratify on (id, name)");

        printStatement("select * from foo tablesample poissonized (100)");

        printStatement("select * from foo approximate at 90 confidence");

        printStatement("create table foo as (select * from abc)");
        printStatement("create table foo with (a = 'apple', b = 'banana') as select * from abc");
        printStatement("drop table foo");

        printStatement("insert into foo select * from abc");

        printStatement("delete from foo");
        printStatement("delete from foo where a = b");

        printStatement("values ('a', 1, 2.2), ('b', 2, 3.3)");

        printStatement("table foo");
        printStatement("table foo order by x limit 10");
        printStatement("(table foo)");
        printStatement("(table foo) limit 10");
        printStatement("(table foo limit 5) limit 10");

        printStatement("select * from a union select * from b");
        printStatement("table a union all table b");
        printStatement("(table foo) union select * from foo union (table foo order by x)");

        printStatement("table a union table b intersect table c");
        printStatement("(table a union table b) intersect table c");
        printStatement("table a union table b except table c intersect table d");
        printStatement("(table a union table b except table c) intersect table d");
        printStatement("((table a union table b) except table c) intersect table d");
        printStatement("(table a union (table b except table c)) intersect table d");
        printStatement("table a intersect table b union table c");
        printStatement("table a intersect (table b union table c)");

        printStatement("alter table foo rename to bar");
        printStatement("alter table a.b.c rename to d.e.f");

        printStatement("alter table a.b.c rename column x to y");

        printStatement("create table test (a boolean, b bigint, c double, d varchar, e timestamp)");
        printStatement("create table if not exists baz (a timestamp, b varchar)");
        printStatement("create table test (a boolean, b bigint) with (a = 'apple', b = 'banana')");
        printStatement("drop table test");

        printStatement("create view foo as with a as (select 123) select * from a");
        printStatement("create or replace view foo as select 123 from t");

        printStatement("drop view foo");
    }

    @Test
    public void testStatementBuilderTpch()
            throws Exception
    {
        printTpchQuery(1, 3);
        printTpchQuery(2, 33, "part type like", "region name");
        printTpchQuery(3, "market segment", "2013-03-05");
        printTpchQuery(4, "2013-03-05");
        printTpchQuery(5, "region name", "2013-03-05");
        printTpchQuery(6, "2013-03-05", 33, 44);
        printTpchQuery(7, "nation name 1", "nation name 2");
        printTpchQuery(8, "nation name", "region name", "part type");
        printTpchQuery(9, "part name like");
        printTpchQuery(10, "2013-03-05");
        printTpchQuery(11, "nation name", 33);
        printTpchQuery(12, "ship mode 1", "ship mode 2", "2013-03-05");
        printTpchQuery(13, "comment like 1", "comment like 2");
        printTpchQuery(14, "2013-03-05");
        // query 15: views not supported
        printTpchQuery(16, "part brand", "part type like", 3, 4, 5, 6, 7, 8, 9, 10);
        printTpchQuery(17, "part brand", "part container");
        printTpchQuery(18, 33);
        printTpchQuery(19, "part brand 1", "part brand 2", "part brand 3", 11, 22, 33);
        printTpchQuery(20, "part name like", "2013-03-05", "nation name");
        printTpchQuery(21, "nation name");
        printTpchQuery(22,
                "phone 1",
                "phone 2",
                "phone 3",
                "phone 4",
                "phone 5",
                "phone 6",
                "phone 7");
    }

    private static void printStatement(String sql)
    {
        println(sql.trim());
        println("");

        Statement statement = SQL_PARSER.createStatement(sql);
        println(statement.toString());
        println("");

        println(SqlFormatter.formatSql(statement));
        println("");
        assertFormattedSql(SQL_PARSER, statement);

        println(repeat("=", 60));
        println("");
    }

    private static void println(String s)
    {
        if (Boolean.parseBoolean(System.getProperty("printParse"))) {
            System.out.println(s);
        }
    }

    private static String getTpchQuery(int q)
            throws IOException
    {
        return readResource("tpch/queries/" + q + ".sql");
    }

    private static void printTpchQuery(int query, Object... values)
            throws IOException
    {
        String sql = getTpchQuery(query);

        for (int i = values.length - 1; i >= 0; i--) {
            sql = sql.replaceAll(format(":%s", i + 1), String.valueOf(values[i]));
        }

        assertFalse(sql.matches("(?s).*:[0-9].*"), "Not all bind parameters were replaced: " + sql);

        sql = fixTpchQuery(sql);
        printStatement(sql);
    }

    private static String readResource(String name)
            throws IOException
    {
        return Resources.toString(Resources.getResource(name), UTF_8);
    }

    private static String fixTpchQuery(String s)
    {
        s = s.replaceFirst("(?m);$", "");
        s = s.replaceAll("(?m)^:[xo]$", "");
        s = s.replaceAll("(?m)^:n -1$", "");
        s = s.replaceAll("(?m)^:n ([0-9]+)$", "LIMIT $1");
        s = s.replace("day (3)", "day"); // for query 1
        return s;
    }
}
