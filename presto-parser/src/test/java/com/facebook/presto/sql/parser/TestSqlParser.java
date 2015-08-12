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

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Approximate;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IntervalLiteral.IntervalField;
import com.facebook.presto.sql.tree.IntervalLiteral.Sign;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.query;
import static com.facebook.presto.sql.QueryUtil.row;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.subquery;
import static com.facebook.presto.sql.QueryUtil.table;
import static com.facebook.presto.sql.QueryUtil.values;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.tree.ArithmeticUnaryExpression.negative;
import static com.facebook.presto.sql.tree.ArithmeticUnaryExpression.positive;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestSqlParser
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testPossibleExponentialBacktracking()
            throws Exception
    {
        SQL_PARSER.createExpression("(((((((((((((((((((((((((((true)))))))))))))))))))))))))))");
    }

    @Test
    public void testGenericLiteral()
            throws Exception
    {
        assertGenericLiteral("VARCHAR");
        assertGenericLiteral("BIGINT");
        assertGenericLiteral("DOUBLE");
        assertGenericLiteral("BOOLEAN");
        assertGenericLiteral("DATE");
        assertGenericLiteral("foo");
    }

    public static void assertGenericLiteral(String type)
    {
        assertExpression(type + " 'abc'", new GenericLiteral(type, "abc"));
    }

    @Test
    public void testLiterals()
            throws Exception
    {
        assertExpression("TIME" + " 'abc'", new TimeLiteral("abc"));
        assertExpression("TIMESTAMP" + " 'abc'", new TimestampLiteral("abc"));
        assertExpression("INTERVAL '33' day", new IntervalLiteral("33", Sign.POSITIVE, IntervalField.DAY, Optional.empty()));
        assertExpression("INTERVAL '33' day to second", new IntervalLiteral("33", Sign.POSITIVE, IntervalField.DAY, Optional.of(IntervalField.SECOND)));
    }

    @Test
    public void testArrayConstructor()
            throws Exception
    {
        assertExpression("ARRAY []", new ArrayConstructor(ImmutableList.<Expression>of()));
        assertExpression("ARRAY [1, 2]", new ArrayConstructor(ImmutableList.<Expression>of(new LongLiteral("1"), new LongLiteral("2"))));
        assertExpression("ARRAY [1.0, 2.5]", new ArrayConstructor(ImmutableList.<Expression>of(new DoubleLiteral("1.0"), new DoubleLiteral("2.5"))));
        assertExpression("ARRAY ['hi']", new ArrayConstructor(ImmutableList.<Expression>of(new StringLiteral("hi"))));
        assertExpression("ARRAY ['hi', 'hello']", new ArrayConstructor(ImmutableList.<Expression>of(new StringLiteral("hi"), new StringLiteral("hello"))));
    }

    @Test
    public void testArraySubscript()
            throws Exception
    {
        assertExpression("ARRAY [1, 2][1]", new SubscriptExpression(
                        new ArrayConstructor(ImmutableList.<Expression>of(new LongLiteral("1"), new LongLiteral("2"))),
                        new LongLiteral("1"))
        );
        try {
            assertExpression("CASE WHEN TRUE THEN ARRAY[1,2] END[1]", null);
            fail();
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testDouble()
            throws Exception
    {
        assertExpression("123.", new DoubleLiteral("123"));
        assertExpression("123.0", new DoubleLiteral("123"));
        assertExpression(".5", new DoubleLiteral(".5"));
        assertExpression("123.5", new DoubleLiteral("123.5"));

        assertExpression("123E7", new DoubleLiteral("123E7"));
        assertExpression("123.E7", new DoubleLiteral("123E7"));
        assertExpression("123.0E7", new DoubleLiteral("123E7"));
        assertExpression("123E+7", new DoubleLiteral("123E7"));
        assertExpression("123E-7", new DoubleLiteral("123E-7"));

        assertExpression("123.456E7", new DoubleLiteral("123.456E7"));
        assertExpression("123.456E+7", new DoubleLiteral("123.456E7"));
        assertExpression("123.456E-7", new DoubleLiteral("123.456E-7"));

        assertExpression(".4E42", new DoubleLiteral(".4E42"));
        assertExpression(".4E+42", new DoubleLiteral(".4E42"));
        assertExpression(".4E-42", new DoubleLiteral(".4E-42"));
    }

    @Test
    public void testCast()
            throws Exception
    {
        assertCast("varchar");
        assertCast("bigint");
        assertCast("BIGINT");
        assertCast("double");
        assertCast("DOUBLE");
        assertCast("boolean");
        assertCast("date");
        assertCast("time");
        assertCast("timestamp");
        assertCast("time with time zone");
        assertCast("timestamp with time zone");
        assertCast("foo");
        assertCast("FOO");

        assertCast("ARRAY<bigint>");
        assertCast("ARRAY<BIGINT>");
        assertCast("array<bigint>");
        assertCast("array < bigint  >", "ARRAY<bigint>");
        assertCast("array<array<bigint>>");
        assertCast("foo ARRAY", "ARRAY<foo>");
        assertCast("boolean array  array ARRAY", "ARRAY<ARRAY<ARRAY<boolean>>>");
        assertCast("boolean ARRAY ARRAY ARRAY", "ARRAY<ARRAY<ARRAY<boolean>>>");
        assertCast("ARRAY<boolean> ARRAY ARRAY", "ARRAY<ARRAY<ARRAY<boolean>>>");
    }

    @Test
    public void testArithmeticUnary()
    {
        assertExpression("9", new LongLiteral("9"));

        assertExpression("+9", positive(new LongLiteral("9")));
        assertExpression("+ 9", positive(new LongLiteral("9")));

        assertExpression("++9", positive(positive(new LongLiteral("9"))));
        assertExpression("+ +9", positive(positive(new LongLiteral("9"))));
        assertExpression("+ + 9", positive(positive(new LongLiteral("9"))));

        assertExpression("+++9", positive(positive(positive(new LongLiteral("9")))));
        assertExpression("+ + +9", positive(positive(positive(new LongLiteral("9")))));
        assertExpression("+ + + 9", positive(positive(positive(new LongLiteral("9")))));

        assertExpression("-9", negative(new LongLiteral("9")));
        assertExpression("- 9", negative(new LongLiteral("9")));

        assertExpression("- + 9", negative(positive(new LongLiteral("9"))));
        assertExpression("-+9", negative(positive(new LongLiteral("9"))));

        assertExpression("+ - + 9", positive(negative(positive(new LongLiteral("9")))));
        assertExpression("+-+9", positive(negative(positive(new LongLiteral("9")))));

        assertExpression("- -9", negative(negative(new LongLiteral("9"))));
        assertExpression("- - 9", negative(negative(new LongLiteral("9"))));

        assertExpression("- + - + 9", negative(positive(negative(positive(new LongLiteral("9"))))));
        assertExpression("-+-+9", negative(positive(negative(positive(new LongLiteral("9"))))));

        assertExpression("+ - + - + 9", positive(negative(positive(negative(positive(new LongLiteral("9")))))));
        assertExpression("+-+-+9", positive(negative(positive(negative(positive(new LongLiteral("9")))))));

        assertExpression("- - -9", negative(negative(negative(new LongLiteral("9")))));
        assertExpression("- - - 9", negative(negative(negative(new LongLiteral("9")))));
    }

    @Test
    public void testDoubleInQuery()
    {
        assertStatement("SELECT 123.456E7 FROM DUAL",
                simpleQuery(
                        selectList(new DoubleLiteral("123.456E7")),
                        table(QualifiedName.of("DUAL"))));
    }

    @Test
    public void testIntersect()
    {
        assertStatement("SELECT 123 INTERSECT DISTINCT SELECT 123 INTERSECT ALL SELECT 123",
                new Query(
                        Optional.empty(),
                        new Intersect(ImmutableList.of(
                                new Intersect(ImmutableList.of(createSelect123(), createSelect123()), true),
                                createSelect123()
                        ), false),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testUnion()
    {
        assertStatement("SELECT 123 UNION DISTINCT SELECT 123 UNION ALL SELECT 123",
                new Query(
                        Optional.empty(),
                        new Union(ImmutableList.of(
                                new Union(ImmutableList.of(createSelect123(), createSelect123()), true),
                                createSelect123()
                        ), false),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));
    }

    private static QuerySpecification createSelect123()
    {
        return new QuerySpecification(
                selectList(new LongLiteral("123")),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty()
        );
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertExpression("1 BETWEEN 2 AND 3", new BetweenPredicate(new LongLiteral("1"), new LongLiteral("2"), new LongLiteral("3")));
        assertExpression("1 NOT BETWEEN 2 AND 3", new NotExpression(new BetweenPredicate(new LongLiteral("1"), new LongLiteral("2"), new LongLiteral("3"))));
    }

    @Test
    public void testValues()
    {
        Query valuesQuery = query(values(
                row(new StringLiteral("a"), new LongLiteral("1"), new DoubleLiteral("2.2")),
                row(new StringLiteral("b"), new LongLiteral("2"), new DoubleLiteral("3.3"))));

        assertStatement("VALUES ('a', 1, 2.2), ('b', 2, 3.3)", valuesQuery);

        assertStatement("SELECT * FROM (VALUES ('a', 1, 2.2), ('b', 2, 3.3))",
                simpleQuery(
                        selectList(new AllColumns()),
                        subquery(valuesQuery)));
    }

    @Test
    public void testPrecedenceAndAssociativity()
            throws Exception
    {
        assertExpression("1 AND 2 OR 3", new LogicalBinaryExpression(LogicalBinaryExpression.Type.OR,
                new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertExpression("1 OR 2 AND 3", new LogicalBinaryExpression(LogicalBinaryExpression.Type.OR,
                new LongLiteral("1"),
                new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
                        new LongLiteral("2"),
                        new LongLiteral("3"))));

        assertExpression("NOT 1 AND 2", new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
                new NotExpression(new LongLiteral("1")),
                new LongLiteral("2")));

        assertExpression("NOT 1 OR 2", new LogicalBinaryExpression(LogicalBinaryExpression.Type.OR,
                new NotExpression(new LongLiteral("1")),
                new LongLiteral("2")));

        assertExpression("-1 + 2", new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.ADD,
                negative(new LongLiteral("1")),
                new LongLiteral("2")));

        assertExpression("1 - 2 - 3", new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.SUBTRACT,
                new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.SUBTRACT,
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertExpression("1 / 2 / 3", new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.DIVIDE,
                new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.DIVIDE,
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertExpression("1 + 2 * 3", new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.ADD,
                new LongLiteral("1"),
                new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.MULTIPLY,
                        new LongLiteral("2"),
                        new LongLiteral("3"))));
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: no viable alternative at input '<EOF>'")
    public void testEmptyExpression()
    {
        SQL_PARSER.createExpression("");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: no viable alternative at input '<EOF>'")
    public void testEmptyStatement()
    {
        SQL_PARSER.createStatement("");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:7: extraneous input 'x' expecting\\E.*")
    public void testExpressionWithTrailingJunk()
    {
        SQL_PARSER.createExpression("1 + 1 x");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: no viable alternative at input '@'")
    public void testTokenizeErrorStartOfLine()
    {
        SQL_PARSER.createStatement("@select");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:25: no viable alternative at input '@'")
    public void testTokenizeErrorMiddleOfLine()
    {
        SQL_PARSER.createStatement("select * from foo where @what");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:15: no viable alternative at input\\E.*")
    public void testTokenizeErrorIncompleteToken()
    {
        SQL_PARSER.createStatement("select * from 'oops");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 3:1: extraneous input 'from' expecting\\E.*")
    public void testParseErrorStartOfLine()
    {
        SQL_PARSER.createStatement("select *\nfrom x\nfrom");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 3:7: no viable alternative at input 'from'")
    public void testParseErrorMiddleOfLine()
    {
        SQL_PARSER.createStatement("select *\nfrom x\nwhere from");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:14: no viable alternative at input '<EOF>'")
    public void testParseErrorEndOfInput()
    {
        SQL_PARSER.createStatement("select * from");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:16: no viable alternative at input '<EOF>'")
    public void testParseErrorEndOfInputWhitespace()
    {
        SQL_PARSER.createStatement("select * from  ");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:15: backquoted identifiers are not supported; use double quotes to quote identifiers")
    public void testParseErrorBackquotes()
    {
        SQL_PARSER.createStatement("select * from `foo`");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:19: backquoted identifiers are not supported; use double quotes to quote identifiers")
    public void testParseErrorBackquotesEndOfInput()
    {
        SQL_PARSER.createStatement("select * from foo `bar`");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:8: identifiers must not start with a digit; surround the identifier with double quotes")
    public void testParseErrorDigitIdentifiers()
    {
        SQL_PARSER.createStatement("select 1x from dual");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:15: identifiers must not contain '@'")
    public void testIdentifierWithAtSign()
    {
        SQL_PARSER.createStatement("select * from foo@bar");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:15: identifiers must not contain ':'")
    public void testIdentifierWithColon()
    {
        SQL_PARSER.createStatement("select * from foo:bar");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:35: mismatched input 'order' expecting .*")
    public void testParseErrorDualOrderBy()
    {
        SQL_PARSER.createStatement("select fuu from dual order by fuu order by fuu");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:31: mismatched input 'order' expecting <EOF>")
    public void testParseErrorReverseOrderByLimit()
    {
        SQL_PARSER.createStatement("select fuu from dual limit 10 order by fuu");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: Invalid numeric literal: 12223222232535343423232435343")
    public void testParseErrorInvalidPositiveLongCast()
    {
        SQL_PARSER.createStatement("select CAST(12223222232535343423232435343 AS BIGINT)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: Invalid numeric literal: 12223222232535343423232435343")
    public void testParseErrorInvalidNegativeLongCast()
    {
        SQL_PARSER.createStatement("select CAST(-12223222232535343423232435343 AS BIGINT)");
    }

    @Test
    public void testParsingExceptionPositionInfo()
    {
        try {
            SQL_PARSER.createStatement("select *\nfrom x\nwhere from");
            fail("expected exception");
        }
        catch (ParsingException e) {
            assertEquals(e.getMessage(), "line 3:7: no viable alternative at input 'from'");
            assertEquals(e.getErrorMessage(), "no viable alternative at input 'from'");
            assertEquals(e.getLineNumber(), 3);
            assertEquals(e.getColumnNumber(), 7);
        }
    }

    @Test
    public void testAllowIdentifierColon()
    {
        SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON));
        sqlParser.createStatement("select * from foo:bar");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:12: no viable alternative at input\\E.*")
    public void testInvalidArguments()
    {
        SQL_PARSER.createStatement("select foo(,1)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:20: no viable alternative at input\\E.*")
    public void testInvalidArguments2()
    {
        SQL_PARSER.createStatement("select foo(DISTINCT)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:21: no viable alternative at input\\E.*")
    public void testInvalidArguments3()
    {
        SQL_PARSER.createStatement("select foo(DISTINCT ,1)");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testAllowIdentifierAtSign()
    {
        SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(AT_SIGN));
        sqlParser.createStatement("select * from foo@bar");
    }

    @Test
    public void testInterval()
            throws Exception
    {
        assertExpression("INTERVAL '123' YEAR", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.YEAR));
        assertExpression("INTERVAL '123-3' YEAR TO MONTH", new IntervalLiteral("123-3", Sign.POSITIVE, IntervalField.YEAR, Optional.of(IntervalField.MONTH)));
        assertExpression("INTERVAL '123' MONTH", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.MONTH));
        assertExpression("INTERVAL '123' DAY", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.DAY));
        assertExpression("INTERVAL '123 23:58:53.456' DAY TO SECOND", new IntervalLiteral("123 23:58:53.456", Sign.POSITIVE, IntervalField.DAY, Optional.of(IntervalField.SECOND)));
        assertExpression("INTERVAL '123' HOUR", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.HOUR));
        assertExpression("INTERVAL '23:59' HOUR TO MINUTE", new IntervalLiteral("23:59", Sign.POSITIVE, IntervalField.HOUR, Optional.of(IntervalField.MINUTE)));
        assertExpression("INTERVAL '123' MINUTE", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.MINUTE));
        assertExpression("INTERVAL '123' SECOND", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.SECOND));
    }

    @Test
    public void testTime()
            throws Exception
    {
        assertExpression("TIME '03:04:05'", new TimeLiteral("03:04:05"));
    }

    @Test
    public void testCurrentTimestamp()
            throws Exception
    {
        assertExpression("CURRENT_TIMESTAMP", new CurrentTime(CurrentTime.Type.TIMESTAMP));
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: expression is too large \\(stack overflow while parsing\\)")
    public void testStackOverflowExpression()
    {
        for (int size = 3000; size <= 100_000; size *= 2) {
            SQL_PARSER.createExpression(Joiner.on(" OR ").join(nCopies(size, "x = y")));
        }
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: statement is too large \\(stack overflow while parsing\\)")
    public void testStackOverflowStatement()
    {
        for (int size = 6000; size <= 100_000; size *= 2) {
            SQL_PARSER.createStatement("SELECT " + Joiner.on(" OR ").join(nCopies(size, "x = y")));
        }
    }

    @Test
    public void testSetSession()
            throws Exception
    {
        assertStatement("SET SESSION foo = 'bar'", new SetSession(QualifiedName.of("foo"), new StringLiteral("bar")));
        assertStatement("SET SESSION foo.bar = 'baz'", new SetSession(QualifiedName.of("foo", "bar"), new StringLiteral("baz")));
        assertStatement("SET SESSION foo.bar.boo = 'baz'", new SetSession(QualifiedName.of("foo", "bar", "boo"), new StringLiteral("baz")));

        assertStatement("SET SESSION foo.bar = 'ban' || 'ana'", new SetSession(
                QualifiedName.of("foo", "bar"),
                new FunctionCall(new QualifiedName("concat"), ImmutableList.of(
                        new StringLiteral("ban"),
                        new StringLiteral("ana")))));
    }

    @Test
    public void testResetSession()
            throws Exception
    {
        assertStatement("RESET SESSION foo.bar", new ResetSession(QualifiedName.of("foo", "bar")));
        assertStatement("RESET SESSION foo", new ResetSession(QualifiedName.of("foo")));
    }

    @Test
    public void testShowSession()
            throws Exception
    {
        assertStatement("SHOW SESSION", new ShowSession());
    }

    @Test
    public void testShowCatalogs()
            throws Exception
    {
        assertStatement("SHOW CATALOGS", new ShowCatalogs());
    }

    @Test
    public void testShowSchemas()
            throws Exception
    {
        assertStatement("SHOW SCHEMAS", new ShowSchemas(Optional.<String>empty()));
        assertStatement("SHOW SCHEMAS FROM foo", new ShowSchemas(Optional.of("foo")));
        assertStatement("SHOW SCHEMAS IN foo", new ShowSchemas(Optional.of("foo")));
    }

    @Test
    public void testShowTables()
            throws Exception
    {
        assertStatement("SHOW TABLES", new ShowTables(Optional.empty(), Optional.empty()));
        assertStatement("SHOW TABLES FROM a", new ShowTables(Optional.of(QualifiedName.of("a")), Optional.empty()));
        assertStatement("SHOW TABLES IN a LIKE '%'", new ShowTables(Optional.of(QualifiedName.of("a")), Optional.of("%")));
    }

    @Test
    public void testShowPartitions()
    {
        assertStatement("SHOW PARTITIONS FROM t", new ShowPartitions(QualifiedName.of("t"), Optional.empty(), ImmutableList.of(), Optional.empty()));

        assertStatement("SHOW PARTITIONS FROM t WHERE x = 1",
                new ShowPartitions(
                        QualifiedName.of("t"),
                        Optional.of(new ComparisonExpression(ComparisonExpression.Type.EQUAL, new QualifiedNameReference(QualifiedName.of("x")), new LongLiteral("1"))),
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("SHOW PARTITIONS FROM t WHERE x = 1 ORDER BY y",
                new ShowPartitions(
                        QualifiedName.of("t"),
                        Optional.of(new ComparisonExpression(ComparisonExpression.Type.EQUAL, new QualifiedNameReference(QualifiedName.of("x")), new LongLiteral("1"))),
                        ImmutableList.of(new SortItem(new QualifiedNameReference(QualifiedName.of("y")), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                        Optional.empty()));

        assertStatement("SHOW PARTITIONS FROM t WHERE x = 1 ORDER BY y LIMIT 10",
                new ShowPartitions(
                        QualifiedName.of("t"),
                        Optional.of(new ComparisonExpression(ComparisonExpression.Type.EQUAL, new QualifiedNameReference(QualifiedName.of("x")), new LongLiteral("1"))),
                        ImmutableList.of(new SortItem(new QualifiedNameReference(QualifiedName.of("y")), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                        Optional.of("10")));
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(new TableElement("a", "VARCHAR"), new TableElement("b", "BIGINT")),
                        false,
                        ImmutableMap.of()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(new TableElement("c", "TIMESTAMP")),
                        true,
                        ImmutableMap.of()));
    }

    @Test
    public void testCreateTableAsSelect()
            throws Exception
    {
        assertStatement("CREATE TABLE foo AS SELECT * FROM t",
                new CreateTableAsSelect(QualifiedName.of("foo"),
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        ImmutableMap.of()));

        assertStatement("CREATE TABLE foo " +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana' ) " +
                        "AS " +
                        "SELECT * " +
                        "FROM t",
                new CreateTableAsSelect(QualifiedName.of("foo"),
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        ImmutableMap.<String, Expression>builder()
                                .put("string", new StringLiteral("bar"))
                                .put("long", new LongLiteral("42"))
                                .put("computed", new FunctionCall(new QualifiedName("concat"), ImmutableList.of(
                                        new StringLiteral("ban"),
                                        new StringLiteral("ana"))))
                                .build()));
    }

    @Test
    public void testDropTable()
            throws Exception
    {
        assertStatement("DROP TABLE a", new DropTable(QualifiedName.of("a"), false));
        assertStatement("DROP TABLE a.b", new DropTable(QualifiedName.of("a", "b"), false));
        assertStatement("DROP TABLE a.b.c", new DropTable(QualifiedName.of("a", "b", "c"), false));

        assertStatement("DROP TABLE IF EXISTS a", new DropTable(QualifiedName.of("a"), true));
        assertStatement("DROP TABLE IF EXISTS a.b", new DropTable(QualifiedName.of("a", "b"), true));
        assertStatement("DROP TABLE IF EXISTS a.b.c", new DropTable(QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testDropView()
            throws Exception
    {
        assertStatement("DROP VIEW a", new DropView(QualifiedName.of("a"), false));
        assertStatement("DROP VIEW a.b", new DropView(QualifiedName.of("a", "b"), false));
        assertStatement("DROP VIEW a.b.c", new DropView(QualifiedName.of("a", "b", "c"), false));

        assertStatement("DROP VIEW IF EXISTS a", new DropView(QualifiedName.of("a"), true));
        assertStatement("DROP VIEW IF EXISTS a.b", new DropView(QualifiedName.of("a", "b"), true));
        assertStatement("DROP VIEW IF EXISTS a.b.c", new DropView(QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testInsertInto()
            throws Exception
    {
        assertStatement("INSERT INTO a SELECT * FROM t",
                new Insert(QualifiedName.of("a"), simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")))));
    }

    @Test
    public void testDelete()
    {
        assertStatement("DELETE FROM t", new Delete(table(QualifiedName.of("t")), Optional.empty()));

        assertStatement("DELETE FROM t WHERE a = b", new Delete(table(QualifiedName.of("t")), Optional.of(
                new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                        new QualifiedNameReference(QualifiedName.of("a")),
                        new QualifiedNameReference(QualifiedName.of("b"))))));
    }

    @Test
    public void testRenameTable()
            throws Exception
    {
        assertStatement("ALTER TABLE a RENAME TO b", new RenameTable(QualifiedName.of("a"), QualifiedName.of("b")));
    }

    @Test
    public void testRenameColumn()
            throws Exception
    {
        assertStatement("ALTER TABLE foo.t RENAME COLUMN a TO b", new RenameColumn(QualifiedName.of("foo", "t"), "a", "b"));
    }

    @Test
    public void testCreateView()
            throws Exception
    {
        assertStatement("CREATE VIEW a AS SELECT * FROM t", new CreateView(
                QualifiedName.of("a"),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                false));

        assertStatement("CREATE OR REPLACE VIEW a AS SELECT * FROM t", new CreateView(
                QualifiedName.of("a"),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                true));
    }

    @Test
    public void testWith()
            throws Exception
    {
        assertStatement("WITH a (t, u) AS (SELECT * FROM x), b AS (SELECT * FROM y) TABLE z",
                new Query(Optional.of(new With(false, ImmutableList.of(
                        new WithQuery("a", simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("x"))), ImmutableList.of("t", "u")),
                        new WithQuery("b", simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("y"))), null)))),
                        new Table(QualifiedName.of("z")),
                        ImmutableList.of(),
                        Optional.<String>empty(),
                        Optional.<Approximate>empty()));

        assertStatement("WITH RECURSIVE a AS (SELECT * FROM x) TABLE y",
                new Query(Optional.of(new With(true, ImmutableList.of(
                        new WithQuery("a", simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("x"))), null)))),
                        new Table(QualifiedName.of("y")),
                        ImmutableList.of(),
                        Optional.<String>empty(),
                        Optional.<Approximate>empty()));
    }

    @Test
    public void testImplicitJoin()
            throws Exception
    {
        assertStatement("SELECT * FROM a, b",
                simpleQuery(selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(QualifiedName.of("a")),
                                new Table(QualifiedName.of("b")),
                                Optional.<JoinCriteria>empty())));
    }

    @Test
    public void testExplain()
            throws Exception
    {
        assertStatement("EXPLAIN SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), ImmutableList.of()));
        assertStatement("EXPLAIN (TYPE LOGICAL) SELECT * FROM t",
                new Explain(
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        ImmutableList.of(new ExplainType(ExplainType.Type.LOGICAL))));
        assertStatement("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) SELECT * FROM t",
                new Explain(
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        ImmutableList.of(
                                new ExplainType(ExplainType.Type.LOGICAL),
                                new ExplainFormat(ExplainFormat.Type.TEXT))));
    }

    @Test
    public void testJoinPrecedence()
    {
        assertStatement("SELECT * FROM a CROSS JOIN b LEFT JOIN c ON true",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.LEFT,
                                new Join(
                                        Join.Type.CROSS,
                                        new Table(QualifiedName.of("a")),
                                        new Table(QualifiedName.of("b")),
                                        Optional.empty()
                                ),
                                new Table(QualifiedName.of("c")),
                                Optional.of(new JoinOn(BooleanLiteral.TRUE_LITERAL)))));
        assertStatement("SELECT * FROM a CROSS JOIN b NATURAL JOIN c CROSS JOIN d NATURAL JOIN e",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.INNER,
                                new Join(
                                        Join.Type.CROSS,
                                        new Join(
                                                Join.Type.INNER,
                                                new Join(
                                                        Join.Type.CROSS,
                                                        new Table(QualifiedName.of("a")),
                                                        new Table(QualifiedName.of("b")),
                                                        Optional.empty()
                                                ),
                                                new Table(QualifiedName.of("c")),
                                                Optional.of(new NaturalJoin())),
                                        new Table(QualifiedName.of("d")),
                                        Optional.empty()
                                ),
                                new Table(QualifiedName.of("e")),
                                Optional.of(new NaturalJoin()))));
    }

    @Test
    public void testUnnest()
            throws Exception
    {
        assertStatement("SELECT * FROM t CROSS JOIN UNNEST(a)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.CROSS,
                                new Table(QualifiedName.of("t")),
                                new Unnest(ImmutableList.of(new QualifiedNameReference(QualifiedName.of("a"))), false),
                                Optional.empty())));
        assertStatement("SELECT * FROM t CROSS JOIN UNNEST(a) WITH ORDINALITY",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.CROSS,
                                new Table(QualifiedName.of("t")),
                                new Unnest(ImmutableList.of(new QualifiedNameReference(QualifiedName.of("a"))), true),
                                Optional.empty())));
    }

    private static void assertCast(String type)
    {
        assertCast(type, type);
    }

    private static void assertCast(String type, String expected)
    {
        assertExpression("CAST(null AS " + type + ")", new Cast(new NullLiteral(), expected));
    }

    private static void assertStatement(String query, Statement expected)
    {
        assertParsed(query, expected, SQL_PARSER.createStatement(query));
    }

    private static void assertExpression(String expression, Expression expected)
    {
        assertParsed(expression, expected, SQL_PARSER.createExpression(expression));
    }

    private static void assertParsed(String input, Node expected, Node parsed)
    {
        if (!parsed.equals(expected)) {
            fail(format("expected\n\n%s\n\nto parse as\n\n%s\n\nbut was\n\n%s\n",
                    indent(input),
                    indent(formatSql(expected)),
                    indent(formatSql(parsed))));
        }
    }

    private static String indent(String value)
    {
        String indent = "    ";
        return indent + value.trim().replaceAll("\n", "\n" + indent);
    }
}
