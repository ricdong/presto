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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.TestingMetadata;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.metadata.ViewDefinition.ViewColumn;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_NAME_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_COLUMN_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_COLUMN_ALIASES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_SET_COLUMN_TYPES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_AGGREGATE_OR_GROUP_BY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_AGGREGATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_WINDOW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NON_NUMERIC_SAMPLE_PERCENTAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_SELECT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_IS_STALE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WILDCARD_WITHOUT_FROM;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WINDOW_REQUIRES_OVER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestAnalyzer
{
    public static final Session SESSION = testSessionBuilder()
            .setCatalog("default")
            .setSchema("default")
            .build();

    private static final SqlParser SQL_PARSER = new SqlParser();

    private Analyzer analyzer;
    private Analyzer approximateDisabledAnalyzer;

    @Test
    public void testDuplicateRelation()
            throws Exception
    {
        assertFails(DUPLICATE_RELATION, "SELECT * FROM t1 JOIN t1 USING (a)");
        assertFails(DUPLICATE_RELATION, "SELECT * FROM t1 x JOIN t2 x USING (a)");
    }

    @Test
    public void testNonComparableGroupBy()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM (SELECT approx_set(1)) GROUP BY 1");
    }

    @Test
    public void testNonComparableWindowPartition()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT row_number() OVER (PARTITION BY t.x) FROM (VALUES(CAST (NULL AS HyperLogLog))) AS t(x)");
    }

    @Test
    public void testNonComparableWindowOrder()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT row_number() OVER (ORDER BY t.x) FROM (VALUES(color('red'))) AS t(x)");
    }

    @Test
    public void testNonComparableDistinctAggregation()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT count(DISTINCT x) FROM (SELECT approx_set(1) x)");
    }

    @Test
    public void testNonComparableDistinct()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT DISTINCT * FROM (SELECT approx_set(1) x)");
        assertFails(TYPE_MISMATCH, "SELECT DISTINCT x FROM (SELECT approx_set(1) x)");
    }

    @Test
    public void testInSubqueryTypes()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES ('a')) t(y) WHERE y IN (VALUES (1))");
    }

    @Test
    public void testScalarSubQueryException()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT 'a', (VALUES (1)) GROUP BY 1");
        assertFails(NOT_SUPPORTED, "SELECT 'a', (SELECT (1))");
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 WHERE (VALUES 1) = 2");
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 WHERE (VALUES 1) IN (VALUES 1)");
        analyze("SELECT * FROM (SELECT 1) t1(x) WHERE x IN (SELECT 1)");
    }

    @Test
    public void testHavingReferencesOutputAlias()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT sum(a) x FROM t1 HAVING x > 5");
    }

    @Test
    public void testWildcardWithInvalidPrefix()
            throws Exception
    {
        assertFails(MISSING_TABLE, "SELECT foo.* FROM t1");
    }

    @Test
    public void testGroupByWithWildcard()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT * FROM t1 GROUP BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT u1.*, u2.* FROM (select a, b + 1 from t1) u1 JOIN (select a, b + 2 from t1) u2 USING (a) GROUP BY u1.a, u2.a, 3");
    }

    @Test
    public void testGroupByInvalidOrdinal()
            throws Exception
    {
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 GROUP BY 10");
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 GROUP BY 0");
    }

    @Test
    public void testOrderByInvalidOrdinal()
            throws Exception
    {
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 ORDER BY 10");
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 ORDER BY 0");
    }

    @Test
    public void testOrderByNonComparable()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT x FROM (SELECT approx_set(1) x) ORDER BY 1");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (SELECT approx_set(1) x) ORDER BY 1");
        assertFails(TYPE_MISMATCH, "SELECT x FROM (SELECT approx_set(1) x) ORDER BY x");
    }

    @Test
    public void testNestedAggregation()
            throws Exception
    {
        assertFails(NESTED_AGGREGATION, "SELECT sum(count(*)) FROM t1");
    }

    @Test
    public void testAggregationsNotAllowed()
            throws Exception
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 WHERE sum(a) > 1");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 GROUP BY sum(a)");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 JOIN t2 ON sum(t1.a) = t2.a");
    }

    @Test
    public void testWindowsNotAllowed()
            throws Exception
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 WHERE foo() over () > 1");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 GROUP BY rank() over ()");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 JOIN t2 ON sum(t1.a) over () = t2.a");
    }

    @Test
    public void testInvalidTable()
            throws Exception
    {
        assertFails(MISSING_CATALOG, "SELECT * FROM foo.default.t");
        assertFails(MISSING_SCHEMA, "SELECT * FROM foo.t");
        assertFails(MISSING_TABLE, "SELECT * FROM foo");
    }

    @Test
    public void testNonAggregate()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT 'a', array[b][1] FROM t1 GROUP BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a, sum(b) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) / a FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) / a FROM t1 GROUP BY c");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) FROM t1 ORDER BY a + 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a, sum(b) FROM t1 GROUP BY a HAVING c > 5");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (PARTITION BY a) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY a) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY count(*) ROWS a PRECEDING) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY count(*) ROWS BETWEEN b PRECEDING AND a PRECEDING) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY count(*) ROWS BETWEEN a PRECEDING AND UNBOUNDED PRECEDING) FROM t1 GROUP BY b");
    }

    @Test
    public void testInvalidAttribute()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT f FROM t1");
        assertFails(MISSING_ATTRIBUTE, "SELECT * FROM t1 ORDER BY f");
        assertFails(MISSING_ATTRIBUTE, "SELECT count(*) FROM t1 GROUP BY f");
        assertFails(MISSING_ATTRIBUTE, "SELECT * FROM t1 WHERE f > 1");
    }

    @Test
    public void testOrderByMustAppearInSelectWithDistinct()
            throws Exception
    {
        assertFails(ORDER_BY_MUST_BE_IN_SELECT, "SELECT DISTINCT a FROM t1 ORDER BY b");
    }

    @Test
    public void testNonBooleanWhereClause()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE a");
    }

    @Test
    public void testApproximateNotEnabled()
            throws Exception
    {
        try {
            Statement statement = SQL_PARSER.createStatement("SELECT AVG(a) FROM t1 APPROXIMATE AT 99.0 CONFIDENCE");
            approximateDisabledAnalyzer.analyze(statement);
            fail(format("Expected error %s, but analysis succeeded", NOT_SUPPORTED));
        }
        catch (SemanticException e) {
            if (e.getCode() != NOT_SUPPORTED) {
                fail(format("Expected error %s, but found %s: %s", NOT_SUPPORTED, e.getCode(), e.getMessage()), e);
            }
        }
    }

    @Test
    public void testApproximateQuery()
            throws Exception
    {
        analyze("SELECT AVG(a) FROM t1 APPROXIMATE AT 99.0 CONFIDENCE");
    }

    @Test
    public void testDistinctAggregations()
            throws Exception
    {
        analyze("SELECT COUNT(DISTINCT a), SUM(a) FROM t1");
    }

    @Test
    public void testMultipleDistinctAggregations()
            throws Exception
    {
        analyze("SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t1");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT a x FROM t1 ORDER BY x + 1");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn2()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a x FROM t1 ORDER BY a + 1");
    }

    @Test
    public void testOrderByWithWildcard()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a, t1.* FROM t1 ORDER BY a");
    }

    @Test
    public void testMismatchedColumnAliasCount()
            throws Exception
    {
        assertFails(MISMATCHED_COLUMN_ALIASES, "SELECT * FROM t1 u (x, y)");
    }

    @Test
    public void testJoinOnConstantExpression()
            throws Exception
    {
        analyze("SELECT * FROM t1 JOIN t2 ON 1 = 1");
    }

    @Test
    public void testJoinOnNonBooleanExpression()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 JOIN t2 ON 5");
    }

    @Test
    public void testJoinOnAmbiguousName()
            throws Exception
    {
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT * FROM t1 JOIN t2 ON a = a");
    }

    @Test
    public void testNonEquiJoin()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 JOIN t2 ON t1.a + t2.a = 1");
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a OR t1.b = t2.b");
    }

    @Test
    public void testNonBooleanHaving()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT sum(a) FROM t1 HAVING sum(a)");
    }

    @Test
    public void testAmbiguousReferenceInOrderBy()
            throws Exception
    {
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT a x, b x FROM t1 ORDER BY x");
    }

    @Test
    public void testImplicitCrossJoin()
    {
        // TODO: validate output
        analyze("SELECT * FROM t1, t2");
    }

    @Test
    public void testNaturalJoinNotSupported()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 NATURAL JOIN t2");
    }

    @Test
    public void testNestedWindowFunctions()
            throws Exception
    {
        assertFails(NESTED_WINDOW, "SELECT avg(sum(a) OVER ()) FROM t1");
        assertFails(NESTED_WINDOW, "SELECT sum(sum(a) OVER ()) OVER () FROM t1");
        assertFails(NESTED_WINDOW, "SELECT avg(a) OVER (PARTITION BY sum(b) OVER ()) FROM t1");
        assertFails(NESTED_WINDOW, "SELECT avg(a) OVER (ORDER BY sum(b) OVER ()) FROM t1");
    }

    @Test
    public void testWindowFunctionWithoutOverClause()
    {
        assertFails(WINDOW_REQUIRES_OVER, "SELECT row_number()");
        assertFails(WINDOW_REQUIRES_OVER, "SELECT coalesce(lead(a), 0) from (values(0)) t(a)");
    }

    @Test
    public void testInvalidWindowFrame()
            throws Exception
    {
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS UNBOUNDED FOLLOWING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS 2 FOLLOWING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 5 PRECEDING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN 2 FOLLOWING AND 5 PRECEDING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN 2 FOLLOWING AND CURRENT ROW)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (RANGE 2 PRECEDING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (RANGE BETWEEN 2 PRECEDING AND 5 FOLLOWING)");

        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS 0.5 PRECEDING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS 'foo' PRECEDING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 0.5 FOLLOWING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 'foo' FOLLOWING)");
    }

    @Test
    public void testDistinctInWindowFunctionParameter()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT a, count(DISTINCT b) OVER () FROM t1");
    }

    @Test
    public void testGroupByOrdinalsWithWildcard()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT t1.*, a FROM t1 GROUP BY 1,2,c,d");
    }

    @Test
    public void testGroupByWithQualifiedName()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT a FROM t1 GROUP BY t1.a");
    }

    @Test
    public void testGroupByWithQualifiedName2()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT t1.a FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByWithQualifiedName3()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT * FROM t1 GROUP BY t1.a, t1.b, t1.c, t1.d");
    }

    @Test
    public void testGroupByWithRowExpression()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT (a, b) FROM t1 GROUP BY a, b");
    }

    @Test
    public void testHaving()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT sum(a) FROM t1 HAVING avg(a) - avg(b) > 10");
    }

    @Test
    public void testWithCaseInsensitiveResolution()
            throws Exception
    {
        // TODO: verify output
        analyze("WITH AB AS (SELECT * FROM t1) SELECT * FROM ab");
    }

    @Test
    public void testInsert()
            throws Exception
    {
        analyze("INSERT INTO t1 SELECT * FROM t1");
        analyze("INSERT INTO t3 SELECT * FROM t3");
        analyze("INSERT INTO t3 SELECT a, b FROM t3");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 VALUES (1, 2)");

        // ignore t5 hidden column
        analyze("INSERT INTO t5 VALUES (1)");

        // fail if hidden column provided
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t5 VALUES (1, 2)");
    }

    @Test
    public void testDuplicateWithQuery()
            throws Exception
    {
        assertFails(DUPLICATE_RELATION,
                "WITH a AS (SELECT * FROM t1)," +
                        "     a AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testCaseInsensitiveDuplicateWithQuery()
            throws Exception
    {
        assertFails(DUPLICATE_RELATION,
                "WITH a AS (SELECT * FROM t1)," +
                        "     A AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testWithForwardReference()
            throws Exception
    {
        assertFails(MISSING_TABLE,
                "WITH a AS (SELECT * FROM b)," +
                        "     b AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testExpressions()
            throws Exception
    {
        // logical not
        assertFails(TYPE_MISMATCH, "SELECT NOT 1 FROM t1");

        // logical and/or
        assertFails(TYPE_MISMATCH, "SELECT 1 AND TRUE FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT TRUE AND 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 OR TRUE FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT TRUE OR 1 FROM t1");

        // comparison
        assertFails(TYPE_MISMATCH, "SELECT 1 = 'a' FROM t1");

        // nullif
        assertFails(TYPE_MISMATCH, "SELECT NULLIF(1, 'a') FROM t1");

        // case
        assertFails(TYPE_MISMATCH, "SELECT CASE WHEN TRUE THEN 'a' ELSE 1 END FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT CASE WHEN '1' THEN 1 ELSE 2 END FROM t1");

        assertFails(TYPE_MISMATCH, "SELECT CASE 1 WHEN 'a' THEN 2 END FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT CASE 1 WHEN 1 THEN 2 ELSE 'a' END FROM t1");

        // coalesce
        assertFails(TYPE_MISMATCH, "SELECT COALESCE(1, 'a') FROM t1");

        // cast
        assertFails(TYPE_MISMATCH, "SELECT CAST(date '2014-01-01' AS bigint)");
        assertFails(TYPE_MISMATCH, "SELECT TRY_CAST(date '2014-01-01' AS bigint)");
        assertFails(TYPE_MISMATCH, "SELECT CAST(null AS UNKNOWN)");

        // arithmetic unary
        assertFails(TYPE_MISMATCH, "SELECT -'a' FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT +'a' FROM t1");

        // arithmetic addition/subtraction
        assertFails(TYPE_MISMATCH, "SELECT 'a' + 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 + 'a'  FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' - 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 - 'a' FROM t1");

        // like
        assertFails(TYPE_MISMATCH, "SELECT 1 LIKE 'a' FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' LIKE 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' LIKE 'b' ESCAPE 1 FROM t1");

        // extract
        assertFails(TYPE_MISMATCH, "SELECT EXTRACT(DAY FROM 'a') FROM t1");

        // between
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 'a' AND 2 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 0 AND 'b' FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 'a' AND 'b' FROM t1");

        // in
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 1 IN ('a')");
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 'a' IN (1)");
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 'a' IN (1, 'b')");
    }

    @Test(enabled = false) // TODO: need to support widening conversion for numbers
    public void testInWithNumericTypes()
            throws Exception
    {
        analyze("SELECT * FROM t1 WHERE 1 IN (1, 2, 3.5)");
    }

    @Test
    public void testWildcardWithoutFrom()
            throws Exception
    {
        assertFails(WILDCARD_WITHOUT_FROM, "SELECT *");
    }

    @Test
    public void testReferenceWithoutFrom()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT dummy");
    }

    @Test
    public void testGroupBy()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY a");
    }

    @Test
    public void testAggregateWithWildcard()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT * FROM (SELECT a + 1, b FROM t1) t GROUP BY b ORDER BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT * FROM (SELECT a, b FROM t1) t GROUP BY b ORDER BY 1");

        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT * FROM (SELECT a, b FROM t1) GROUP BY b ORDER BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT * FROM (SELECT a + 1, b FROM t1) GROUP BY b ORDER BY 1");
    }

    @Test
    public void testGroupByCase()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE a WHEN 1 THEN 'a' ELSE 'b' END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE 1 WHEN 2 THEN a ELSE 0 END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE 1 WHEN 2 THEN 0 ELSE a END, count(*) FROM t1");

        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN a = 1 THEN 'a' ELSE 'b' END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN true THEN a ELSE 0 END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN true THEN 0 ELSE a END, count(*) FROM t1");
    }

    @Test
    public void testMismatchedUnionQueries()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT 1 UNION SELECT 'a'");
        assertFails(TYPE_MISMATCH, "SELECT a FROM t1 UNION SELECT 'a'");
        assertFails(TYPE_MISMATCH, "(SELECT 1) UNION SELECT 'a'");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "SELECT 1, 2 UNION SELECT 1");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "SELECT 'a' UNION SELECT 'b', 'c'");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "TABLE t2 UNION SELECT 'a'");
    }

    @Test
    public void testUnionUnmatchedOrderByAttribute()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "TABLE t2 UNION ALL SELECT c, d FROM t1 ORDER BY c");
    }

    @Test
    public void testGroupByComplexExpressions()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(a IS NULL, 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(a IS NOT NULL, 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(CAST(a AS VARCHAR) LIKE 'a', 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a IN (1, 2, 3) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT 1 IN (a, 2, 3) FROM t1 GROUP BY b");
    }

    @Test
    public void testNonNumericTableSamplePercentage()
            throws Exception
    {
        assertFails(NON_NUMERIC_SAMPLE_PERCENTAGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI ('a')");
        assertFails(NON_NUMERIC_SAMPLE_PERCENTAGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (a + 1)");
    }

    @Test
    public void testTableSampleOutOfRange()
            throws Exception
    {
        assertFails(SAMPLE_PERCENTAGE_OUT_OF_RANGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (-1)");
        assertFails(SAMPLE_PERCENTAGE_OUT_OF_RANGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (-101)");
    }

    @Test
    public void testCreateViewColumns()
            throws Exception
    {
        assertFails(COLUMN_NAME_NOT_SPECIFIED, "CREATE VIEW test AS SELECT 123");
        assertFails(DUPLICATE_COLUMN_NAME, "CREATE VIEW test AS SELECT 1 a, 2 a");
    }

    @Test
    public void testStaleView()
            throws Exception
    {
        assertFails(VIEW_IS_STALE, "SELECT * FROM v2");
    }

    @Test
    public void testStoredViewAnalysisScoping()
            throws Exception
    {
        // the view must not be analyzed using the query context
        analyze("WITH t1 AS (SELECT 123 x) SELECT * FROM v1");
    }

    @Test
    public void testStoredViewResolution()
            throws Exception
    {
        // the view must be analyzed relative to its own catalog/schema
        analyze("SELECT * FROM c3.s3.v3");
    }

    @Test
    public void testUse()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "USE default");
    }

    @Test
    public void testNotNullInJoinClause()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT * FROM (VALUES (1)) a (x) JOIN (VALUES (2)) b ON a.x IS NOT NULL");
    }

    @Test
    public void testIfInJoinClause()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT * FROM (VALUES (1)) a (x) JOIN (VALUES (2)) b ON IF(a.x = 1, true, false)");
    }

    @Test
    public void testLiteral()
            throws Exception
    {
        assertFails(INVALID_LITERAL, "SELECT TIMESTAMP '2012-10-31 01:00:00 PT'");
    }

    @BeforeMethod(alwaysRun = true)
    public void setup()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        MetadataManager metadata = new MetadataManager(
                new FeaturesConfig().setExperimentalSyntaxEnabled(true),
                typeManager,
                new SplitManager(),
                new BlockEncodingManager(typeManager),
                new SessionPropertyManager(),
                new TablePropertyManager());
        metadata.addConnectorMetadata("tpch", "tpch", new TestingMetadata());
        metadata.addConnectorMetadata("c2", "c2", new TestingMetadata());
        metadata.addConnectorMetadata("c3", "c3", new TestingMetadata());

        SchemaTableName table1 = new SchemaTableName("default", "t1");
        metadata.createTable(SESSION, "tpch", new TableMetadata("tpch", new ConnectorTableMetadata(table1,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", BIGINT, false),
                        new ColumnMetadata("b", BIGINT, false),
                        new ColumnMetadata("c", BIGINT, false),
                        new ColumnMetadata("d", BIGINT, false)))));

        SchemaTableName table2 = new SchemaTableName("default", "t2");
        metadata.createTable(SESSION, "tpch", new TableMetadata("tpch", new ConnectorTableMetadata(table2,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", BIGINT, false),
                        new ColumnMetadata("b", BIGINT, false)))));

        SchemaTableName table3 = new SchemaTableName("default", "t3");
        metadata.createTable(SESSION, "tpch", new TableMetadata("tpch", new ConnectorTableMetadata(table3,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", BIGINT, false),
                        new ColumnMetadata("b", BIGINT, false),
                        new ColumnMetadata("x", BIGINT, false, null, true)))));

        // table in different catalog
        SchemaTableName table4 = new SchemaTableName("s2", "t4");
        metadata.createTable(SESSION, "c2", new TableMetadata("tpch", new ConnectorTableMetadata(table4,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", BIGINT, false)))));

        // table with a hidden column
        SchemaTableName table5 = new SchemaTableName("default", "t5");
        metadata.createTable(SESSION, "tpch", new TableMetadata("tpch", new ConnectorTableMetadata(table5,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", BIGINT, false),
                        new ColumnMetadata("b", BIGINT, false, null, true)))));

        // valid view referencing table in same schema
        String viewData1 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition("select a from t1", "tpch", "default", ImmutableList.of(
                        new ViewColumn("a", BIGINT))));
        metadata.createView(SESSION, new QualifiedTableName("tpch", "default", "v1"), viewData1, false);

        // stale view (different column type)
        String viewData2 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition("select a from t1", "tpch", "default", ImmutableList.of(
                        new ViewColumn("a", VARCHAR))));
        metadata.createView(SESSION, new QualifiedTableName("tpch", "default", "v2"), viewData2, false);

        // view referencing table in different schema from itself and session
        String viewData3 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition("select a from t4", "c2", "s2", ImmutableList.of(
                        new ViewColumn("a", BIGINT))));
        metadata.createView(SESSION, new QualifiedTableName("c3", "s3", "v3"), viewData3, false);

        analyzer = new Analyzer(
                testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema("default")
                        .build(),
                metadata,
                SQL_PARSER,
                Optional.empty(),
                true);

        approximateDisabledAnalyzer = new Analyzer(
                testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema("default")
                        .build(),
                metadata,
                SQL_PARSER,
                Optional.empty(),
                false);
    }

    private void analyze(@Language("SQL") String query)
    {
        Statement statement = SQL_PARSER.createStatement(query);
        analyzer.analyze(statement);
    }

    private void assertFails(SemanticErrorCode error, @Language("SQL") String query)
    {
        try {
            Statement statement = SQL_PARSER.createStatement(query);
            analyzer.analyze(statement);
            fail(format("Expected error %s, but analysis succeeded", error));
        }
        catch (SemanticException e) {
            if (e.getCode() != error) {
                fail(format("Expected error %s, but found %s: %s", error, e.getCode(), e.getMessage()), e);
            }
        }
    }
}
