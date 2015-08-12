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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.TestingRowConstructor;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockSerdeUtil.writeBlock;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.ArrayType.toStackRepresentation;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestArrayOperators
        extends AbstractTestFunctions
{
    public TestArrayOperators()
    {
        registerScalar(TestingRowConstructor.class);
    }

    @Test
    public void testStackRepresentation()
            throws Exception
    {
        Block actualBlock = toStackRepresentation(ImmutableList.of(
                toStackRepresentation(ImmutableList.of(1L, 2L), BIGINT),
                toStackRepresentation(ImmutableList.of(3L), BIGINT)), new ArrayType(BIGINT));
        DynamicSliceOutput actualSliceOutput = new DynamicSliceOutput(100);
        writeBlock(actualSliceOutput, actualBlock);

        Block expectedBlock = new ArrayType(BIGINT)
                .createBlockBuilder(new BlockBuilderStatus(), 3)
                .writeObject(BIGINT.createBlockBuilder(new BlockBuilderStatus(), 2).writeLong(1).closeEntry().writeLong(2).closeEntry().build())
                .closeEntry()
                .writeObject(BIGINT.createBlockBuilder(new BlockBuilderStatus(), 1).writeLong(3).closeEntry().build())
                .closeEntry()
                .build();
        DynamicSliceOutput expectedSliceOutput = new DynamicSliceOutput(100);
        writeBlock(expectedSliceOutput, expectedBlock);

        assertEquals(actualSliceOutput.slice(), expectedSliceOutput.slice());
    }

    @Test
    public void testArrayElements()
            throws Exception
    {
        assertFunction("CAST(ARRAY [1, 2, 3] AS ARRAY<BIGINT>)", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L));
        assertFunction("CAST(ARRAY [1, null, 3] AS ARRAY<BIGINT>)", new ArrayType(BIGINT), asList(1L, null, 3L));

        assertFunction("CAST(ARRAY [1, 2, 3] AS ARRAY<DOUBLE>)", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0, 3.0));
        assertFunction("CAST(ARRAY [1, null, 3] AS ARRAY<DOUBLE>)", new ArrayType(DOUBLE), asList(1.0, null, 3.0));

        assertFunction("CAST(ARRAY ['1', '2'] AS ARRAY<VARCHAR>)", new ArrayType(VARCHAR), ImmutableList.of("1", "2"));
        assertFunction("CAST(ARRAY ['1', '2'] AS ARRAY<DOUBLE>)", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0));

        assertFunction("CAST(ARRAY [true, false] AS ARRAY<BOOLEAN>)", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("CAST(ARRAY [true, false] AS ARRAY<VARCHAR>)", new ArrayType(VARCHAR), ImmutableList.of("true", "false"));
        assertFunction("CAST(ARRAY [1, 0] AS ARRAY<BOOLEAN>)", new ArrayType(BOOLEAN), ImmutableList.of(true, false));

        assertFunction("CAST(ARRAY [ARRAY[1], ARRAY[2, 3]] AS ARRAY<ARRAY<DOUBLE>>)", new ArrayType(new ArrayType(DOUBLE)), asList(asList(1.0), asList(2.0, 3.0)));

        assertInvalidFunction("CAST(ARRAY [1, null, 3] AS ARRAY<TIMESTAMP>)", FUNCTION_NOT_FOUND);
        assertInvalidFunction("CAST(ARRAY [1, null, 3] AS ARRAY<ARRAY<TIMESTAMP>>)", FUNCTION_NOT_FOUND);
        assertInvalidFunction("CAST(ARRAY ['puppies', 'kittens'] AS ARRAY<BIGINT>)", INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testArrayToJson()
            throws Exception
    {
        assertFunction("CAST(ARRAY [1, 2, 3] AS JSON)", JSON, "[1,2,3]");
        assertFunction("CAST(ARRAY [1, NULL, 3] AS JSON)", JSON, "[1,null,3]");
        assertFunction("CAST(ARRAY [1, 2.0, 3] AS JSON)", JSON, "[1.0,2.0,3.0]");
        assertFunction("CAST(ARRAY [1.0, 2.5, 3.0] AS JSON)", JSON, "[1.0,2.5,3.0]");
        assertFunction("CAST(ARRAY ['puppies', 'kittens'] AS JSON)", JSON, "[\"puppies\",\"kittens\"]");
        assertFunction("CAST(ARRAY [TRUE, FALSE] AS JSON)", JSON, "[true,false]");
        assertFunction("CAST(ARRAY [from_unixtime(1)] AS JSON)", JSON, "[\"" + sqlTimestamp(1000) + "\"]");
        assertFunction("CAST(ARRAY [ARRAY [1], ARRAY [2, 3]] AS JSON)", JSON, "[[1],[2,3]]");
    }

    @Test
    public void testJsonToArray()
            throws Exception
    {
        assertFunction("CAST(CAST('[1, 2, 3]' AS JSON) AS ARRAY<BIGINT>)", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L));
        assertFunction("CAST(CAST('[1, null, 3]' AS JSON) AS ARRAY<BIGINT>)", new ArrayType(BIGINT), asList(1L, null, 3L));
        assertFunction("CAST(CAST('[1, 2.0, 3]' AS JSON) AS ARRAY<DOUBLE>)", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0, 3.0));
        assertFunction("CAST(CAST('[1.0, 2.5, 3.0]' AS JSON) AS ARRAY<DOUBLE>)", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.5, 3.0));
        assertFunction("CAST(CAST('[\"puppies\", \"kittens\"]' AS JSON) AS ARRAY<VARCHAR>)", new ArrayType(VARCHAR), ImmutableList.of("puppies", "kittens"));
        assertFunction("CAST(CAST('[true, false]' AS JSON) AS ARRAY<BOOLEAN>)", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("CAST(CAST('[[1], [null]]' AS JSON) AS ARRAY<ARRAY<BIGINT>>)", new ArrayType(new ArrayType(BIGINT)), asList(asList(1L), asList((Long) null)));
        assertFunction("CAST(CAST('null' AS JSON) AS ARRAY<BIGINT>)", new ArrayType(BIGINT), null);
        assertFunction("CAST(CAST('[5, [1, 2, 3], \"e\", {\"a\": \"b\"}, null, \"null\", [null]]' AS JSON) AS ARRAY<JSON>)", new ArrayType(JSON), ImmutableList.of("5", "[1,2,3]", "\"e\"", "{\"a\":\"b\"}", "null", "\"null\"", "[null]"));
        assertInvalidCast("CAST(CAST('[1, null, 3]' AS JSON) AS ARRAY<TIMESTAMP>)");
        assertInvalidCast("CAST(CAST('[1, null, 3]' AS JSON) AS ARRAY<ARRAY<TIMESTAMP>>)");
        assertInvalidCast("CAST(CAST('[1, 2, 3]' AS JSON) AS ARRAY<BOOLEAN>)");
        assertInvalidCast("CAST(CAST('[\"puppies\", \"kittens\"]' AS JSON) AS ARRAY<BIGINT>)");
    }

    @Test
    public void testConstructor()
            throws Exception
    {
        assertFunction("ARRAY []", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("ARRAY [NULL]", new ArrayType(UNKNOWN), Lists.newArrayList((Object) null));
        assertFunction("ARRAY [1, 2, 3]", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L));
        assertFunction("ARRAY [1, NULL, 3]", new ArrayType(BIGINT), Lists.newArrayList(1L, null, 3L));
        assertFunction("ARRAY [NULL, 2, 3]", new ArrayType(BIGINT), Lists.newArrayList(null, 2L, 3L));
        assertFunction("ARRAY [1, 2.0, 3]", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0, 3.0));
        assertFunction("ARRAY [ARRAY[1, 2], ARRAY[3]]", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(3L)));
        assertFunction("ARRAY [ARRAY[1, 2], NULL, ARRAY[3]]", new ArrayType(new ArrayType(BIGINT)), Lists.newArrayList(ImmutableList.of(1L, 2L), null, ImmutableList.of(3L)));
        assertFunction("ARRAY [1.0, 2.5, 3.0]", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.5, 3.0));
        assertFunction("ARRAY [1, 2.5, 3]", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.5, 3.0));
        assertFunction("ARRAY ['puppies', 'kittens']", new ArrayType(VARCHAR), ImmutableList.of("puppies", "kittens"));
        assertFunction("ARRAY [TRUE, FALSE]", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("ARRAY [from_unixtime(1), from_unixtime(100)]", new ArrayType(TIMESTAMP), ImmutableList.of(
                sqlTimestamp(1000), sqlTimestamp(100_000)));
        assertFunction("ARRAY [sqrt(-1)]", new ArrayType(DOUBLE), ImmutableList.of(NaN));
        assertFunction("ARRAY [pow(infinity(), 2)]", new ArrayType(DOUBLE), ImmutableList.of(POSITIVE_INFINITY));
        assertFunction("ARRAY [pow(-infinity(), 1)]", new ArrayType(DOUBLE), ImmutableList.of(NEGATIVE_INFINITY));
        assertFunction("ARRAY [ARRAY [], NULL]", new ArrayType(new ArrayType(UNKNOWN)), asList(ImmutableList.of(), null));
    }

    @Test
    public void testArrayToArrayConcat()
            throws Exception
    {
        assertFunction("ARRAY [1, NULL] || ARRAY [3]", new ArrayType(BIGINT), Lists.newArrayList(1L, null, 3L));
        assertFunction("ARRAY [1, 2] || ARRAY[3, 4]", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L, 4L));
        assertFunction("ARRAY [NULL] || ARRAY[NULL]", new ArrayType(UNKNOWN), Lists.newArrayList(null, null));
        assertFunction("ARRAY ['puppies'] || ARRAY ['kittens']", new ArrayType(VARCHAR), ImmutableList.of("puppies", "kittens"));
        assertFunction("ARRAY [TRUE] || ARRAY [FALSE]", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("concat(ARRAY [1] , ARRAY[2,3])", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L));
        assertFunction("ARRAY [from_unixtime(1)] || ARRAY[from_unixtime(100)]", new ArrayType(TIMESTAMP), ImmutableList.of(
                sqlTimestamp(1000), sqlTimestamp(100_000)));
        assertFunction("ARRAY [ARRAY[ARRAY[1]]] || ARRAY [ARRAY[ARRAY[2]]]",
                new ArrayType(new ArrayType(new ArrayType(BIGINT))),
                asList(singletonList(Longs.asList(1)), singletonList(Longs.asList(2))));
        assertFunction("ARRAY [] || ARRAY []", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("ARRAY [TRUE] || ARRAY [FALSE] || ARRAY [TRUE]", new ArrayType(BOOLEAN), ImmutableList.of(true, false, true));
        assertFunction("ARRAY [1] || ARRAY [2] || ARRAY [3] || ARRAY [4]", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L, 4L));
        assertFunction("ARRAY [1] || ARRAY [2.0] || ARRAY [3] || ARRAY [4.0]", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0, 3.0, 4.0));
        assertFunction("ARRAY [ARRAY [1], ARRAY [2, 8]] || ARRAY [ARRAY [3, 6], ARRAY [4]]", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(2L, 8L), ImmutableList.of(3L, 6L), ImmutableList.of(4L)));

        assertInvalidFunction("ARRAY [ARRAY[1]] || ARRAY[ARRAY[true], ARRAY[false]]", FUNCTION_NOT_FOUND.toErrorCode());

        try {
            assertFunction("ARRAY [ARRAY[1]] || ARRAY[NULL]", new ArrayType(new ArrayType(BIGINT)), null);
            fail("arrays must be of the same type");
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testElementArrayConcat()
            throws Exception
    {
        assertFunction("CAST (ARRAY [DATE '2001-08-22'] || DATE '2001-08-23' AS JSON)", JSON, "[\"2001-08-22\",\"2001-08-23\"]");
        assertFunction("CAST (DATE '2001-08-23' || ARRAY [DATE '2001-08-22'] AS JSON)", JSON, "[\"2001-08-23\",\"2001-08-22\"]");
        assertFunction("1 || ARRAY [2]", new ArrayType(BIGINT), Lists.newArrayList(1L, 2L));
        assertFunction("ARRAY [2] || 1", new ArrayType(BIGINT), Lists.newArrayList(2L, 1L));
        assertFunction("TRUE || ARRAY [FALSE]", new ArrayType(BOOLEAN), Lists.newArrayList(true, false));
        assertFunction("ARRAY [FALSE] || TRUE", new ArrayType(BOOLEAN), Lists.newArrayList(false, true));
        assertFunction("1.0 || ARRAY [2.0]", new ArrayType(DOUBLE), Lists.newArrayList(1.0, 2.0));
        assertFunction("ARRAY [2.0] || 1.0", new ArrayType(DOUBLE), Lists.newArrayList(2.0, 1.0));
        assertFunction("'puppies' || ARRAY ['kittens']", new ArrayType(VARCHAR), Lists.newArrayList("puppies", "kittens"));
        assertFunction("ARRAY ['kittens'] || 'puppies'", new ArrayType(VARCHAR), Lists.newArrayList("kittens", "puppies"));
        assertFunction("ARRAY [from_unixtime(1)] || from_unixtime(100)", new ArrayType(TIMESTAMP), ImmutableList.of(
                sqlTimestamp(1000), sqlTimestamp(100_000)));
        assertFunction("from_unixtime(100) || ARRAY [from_unixtime(1)]", new ArrayType(TIMESTAMP), ImmutableList.of(
                sqlTimestamp(100_000), sqlTimestamp(1000)));
        assertFunction("ARRAY [2, 8] || ARRAY[ARRAY[3, 6], ARRAY[4]]", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(2L, 8L), ImmutableList.of(3L, 6L), ImmutableList.of(4L)));
        assertFunction("ARRAY [ARRAY [1], ARRAY [2, 8]] || ARRAY [3, 6]", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(2L, 8L), ImmutableList.of(3L, 6L)));
    }

    @Test
    public void testArrayContains()
            throws Exception
    {
        assertFunction("CONTAINS(ARRAY [1, 2, 3], 2)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [1, 2, 3], 5)", BOOLEAN, false);
        assertFunction("CONTAINS(ARRAY [1, NULL, 3], 1)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [NULL, 2, 3], 1)", BOOLEAN, null);
        assertFunction("CONTAINS(ARRAY [1, 2.0, 3], 3.0)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [1.0, 2.5, 3.0], 2.2)", BOOLEAN, false);
        assertFunction("CONTAINS(ARRAY ['puppies', 'kittens'], 'kittens')", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY ['puppies', 'kittens'], 'lizards')", BOOLEAN, false);
        assertFunction("CONTAINS(ARRAY [TRUE, FALSE], TRUE)", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [FALSE], TRUE)", BOOLEAN, false);
        assertFunction("CONTAINS(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [3, 4])", BOOLEAN, true);
        assertFunction("CONTAINS(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [3])", BOOLEAN, false);
    }

    @Test
    public void testArrayJoin()
            throws Exception
    {
        assertFunction("array_join(ARRAY[1, NULL, 2], ',')", VARCHAR, "1,2");
        assertFunction("ARRAY_JOIN(ARRAY [1, 2, 3], ';', 'N/A')", VARCHAR, "1;2;3");
        assertFunction("ARRAY_JOIN(ARRAY [1, 2, null], ';', 'N/A')", VARCHAR, "1;2;N/A");
        assertFunction("ARRAY_JOIN(ARRAY [1, 2, 3], 'x')", VARCHAR, "1x2x3");
        assertFunction("ARRAY_JOIN(ARRAY [null], '=')", VARCHAR, "");
        assertFunction("ARRAY_JOIN(ARRAY [null,null], '=')", VARCHAR, "");
        assertFunction("ARRAY_JOIN(ARRAY [], 'S')", VARCHAR, "");
        assertFunction("ARRAY_JOIN(ARRAY [''], '', '')", VARCHAR, "");
        assertFunction("ARRAY_JOIN(ARRAY [1, 2, 3, null, 5], ',', '*')", VARCHAR, "1,2,3,*,5");
        assertFunction("ARRAY_JOIN(ARRAY ['a', 'b', 'c', null, null, 'd'], '-', 'N/A')", VARCHAR, "a-b-c-N/A-N/A-d");
        assertFunction("ARRAY_JOIN(ARRAY ['a', 'b', 'c', null, null, 'd'], '-')", VARCHAR, "a-b-c-d");
        assertFunction("ARRAY_JOIN(ARRAY [null, null, null, null], 'X')", VARCHAR, "");
        assertFunction("ARRAY_JOIN(ARRAY [true, false], 'XX')", VARCHAR, "trueXXfalse");
        assertFunction("ARRAY_JOIN(ARRAY [sqrt(-1), infinity()], ',')", VARCHAR, "NaN,Infinity");
        assertFunction("ARRAY_JOIN(ARRAY [from_unixtime(1), from_unixtime(10)], '|')", VARCHAR, sqlTimestamp(1000).toString() + "|" + sqlTimestamp(10_000).toString());
        assertFunction("ARRAY_JOIN(ARRAY [null, from_unixtime(10)], '|')", VARCHAR, sqlTimestamp(10_000).toString());
        assertFunction("ARRAY_JOIN(ARRAY [null, from_unixtime(10)], '|', 'XYZ')", VARCHAR, "XYZ|" + sqlTimestamp(10_000).toString());

        assertInvalidFunction("ARRAY_JOIN(ARRAY [ARRAY [1], ARRAY [2]], '-')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("ARRAY_JOIN(ARRAY [MAP(ARRAY [1], ARRAY [2])], '-')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("ARRAY_JOIN(ARRAY [test_row(1, 2)], '-')", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testCardinality()
            throws Exception
    {
        assertFunction("CARDINALITY(ARRAY [])", BIGINT, 0);
        assertFunction("CARDINALITY(ARRAY [NULL])", BIGINT, 1);
        assertFunction("CARDINALITY(ARRAY [1, 2, 3])", BIGINT, 3);
        assertFunction("CARDINALITY(ARRAY [1, NULL, 3])", BIGINT, 3);
        assertFunction("CARDINALITY(ARRAY [1, 2.0, 3])", BIGINT, 3);
        assertFunction("CARDINALITY(ARRAY [ARRAY[1, 2], ARRAY[3]])", BIGINT, 2);
        assertFunction("CARDINALITY(ARRAY [1.0, 2.5, 3.0])", BIGINT, 3);
        assertFunction("CARDINALITY(ARRAY ['puppies', 'kittens'])", BIGINT, 2);
        assertFunction("CARDINALITY(ARRAY [TRUE, FALSE])", BIGINT, 2);
    }

    @Test
    public void testArrayMin()
            throws Exception
    {
        assertFunction("ARRAY_MIN(ARRAY [])", UNKNOWN, null);
        assertFunction("ARRAY_MIN(ARRAY [NULL])", UNKNOWN, null);
        assertFunction("ARRAY_MIN(ARRAY [NULL, NULL, NULL])", UNKNOWN, null);
        assertFunction("ARRAY_MIN(ARRAY [NULL, 2, 3])", BIGINT, null);
        assertFunction("ARRAY_MIN(ARRAY [1.0, NULL, 3])", DOUBLE, null);
        assertFunction("ARRAY_MIN(ARRAY ['1', '2', NULL])", VARCHAR, null);
        assertFunction("ARRAY_MIN(ARRAY [3, 2, 1])", BIGINT, 1);
        assertFunction("ARRAY_MIN(ARRAY [1, 2, 3])", BIGINT, 1);
        assertFunction("ARRAY_MIN(ARRAY [1, 2.0, 3])", DOUBLE, 1.0);
        assertFunction("ARRAY_MIN(ARRAY [ARRAY[1, 2], ARRAY[3]])", new ArrayType(BIGINT), ImmutableList.of(1L, 2L));
        assertFunction("ARRAY_MIN(ARRAY [1.0, 2.5, 3.0])", DOUBLE, 1.0);
        assertFunction("ARRAY_MIN(ARRAY ['puppies', 'kittens'])", VARCHAR, "kittens");
        assertFunction("ARRAY_MIN(ARRAY [TRUE, FALSE])", BOOLEAN, false);
        assertFunction("ARRAY_MIN(ARRAY [NULL, FALSE])", BOOLEAN, null);
    }

    @Test
    public void testArrayMax()
            throws Exception
    {
        assertFunction("ARRAY_MAX(ARRAY [])", UNKNOWN, null);
        assertFunction("ARRAY_MAX(ARRAY [NULL])", UNKNOWN, null);
        assertFunction("ARRAY_MAX(ARRAY [NULL, NULL, NULL])", UNKNOWN, null);
        assertFunction("ARRAY_MAX(ARRAY [NULL, 2, 3])", BIGINT, null);
        assertFunction("ARRAY_MAX(ARRAY [1.0, NULL, 3])", DOUBLE, null);
        assertFunction("ARRAY_MAX(ARRAY ['1', '2', NULL])", VARCHAR, null);
        assertFunction("ARRAY_MAX(ARRAY [3, 2, 1])", BIGINT, 3);
        assertFunction("ARRAY_MAX(ARRAY [1, 2, 3])", BIGINT, 3);
        assertFunction("ARRAY_MAX(ARRAY [1, 2.0, 3])", DOUBLE, 3.0);
        assertFunction("ARRAY_MAX(ARRAY [ARRAY[1, 2], ARRAY[3]])", new ArrayType(BIGINT), ImmutableList.of(3L));
        assertFunction("ARRAY_MAX(ARRAY [1.0, 2.5, 3.0])", DOUBLE, 3.0);
        assertFunction("ARRAY_MAX(ARRAY ['puppies', 'kittens'])", VARCHAR, "puppies");
        assertFunction("ARRAY_MAX(ARRAY [TRUE, FALSE])", BOOLEAN, true);
        assertFunction("ARRAY_MAX(ARRAY [NULL, FALSE])", BOOLEAN, null);
    }

    @Test
    public void testArrayPosition()
            throws Exception
    {
        assertFunction("ARRAY_POSITION(ARRAY [10, 20, 30, 40], 30)", BIGINT, 3);
        assertFunction("ARRAY_POSITION(cast(cast('[]' as json) as array<bigint>), 30)", BIGINT, 0);
        assertFunction("ARRAY_POSITION(ARRAY [cast(NULL as bigint)], 30)", BIGINT, 0);
        assertFunction("ARRAY_POSITION(ARRAY [cast(NULL as bigint), NULL, NULL], 30)", BIGINT, 0);
        assertFunction("ARRAY_POSITION(ARRAY [NULL, NULL, 30, NULL], 30)", BIGINT, 3);

        assertFunction("ARRAY_POSITION(ARRAY [1.1, 2.1, 3.1, 4.1], 3.1)", BIGINT, 3);
        assertFunction("ARRAY_POSITION(ARRAY [false, false, true, true], true)", BIGINT, 3);
        assertFunction("ARRAY_POSITION(ARRAY ['10', '20', '30', '40'], '30')", BIGINT, 3);

        assertFunction("ARRAY_POSITION(ARRAY [DATE '2000-01-01', DATE '2000-01-02', DATE '2000-01-03', DATE '2000-01-04'], DATE '2000-01-03')", BIGINT, 3);
        assertFunction("ARRAY_POSITION(ARRAY [ARRAY [1, 11], ARRAY [2, 12], ARRAY [3, 13], ARRAY [4, 14]], ARRAY [3, 13])", BIGINT, 3);
    }

    @Test
    public void testSubscript()
            throws Exception
    {
        String outOfBounds = "Array subscript out of bounds";
        String negativeIndex = "Array subscript is negative";
        String indexIsZero = "SQL array indices start at 1";
        assertInvalidFunction("ARRAY [][1]", outOfBounds);
        assertInvalidFunction("ARRAY [null][-1]", negativeIndex);
        assertInvalidFunction("ARRAY [1, 2, 3][0]", indexIsZero);
        assertInvalidFunction("ARRAY [1, 2, 3][-1]", negativeIndex);
        assertInvalidFunction("ARRAY [1, 2, 3][4]", outOfBounds);

        try {
            assertFunction("ARRAY [1, 2, 3][1.1]", BIGINT, null);
            fail("Access to array with double subscript should fail");
        }
        catch (SemanticException e) {
            assertTrue(e.getCode() == SemanticErrorCode.TYPE_MISMATCH);
        }

        assertFunction("ARRAY[NULL][1]", UNKNOWN, null);
        assertFunction("ARRAY[NULL, NULL, NULL][3]", UNKNOWN, null);
        assertFunction("1 + ARRAY [2, 1, 3][2]", BIGINT, 2);
        assertFunction("ARRAY [2, 1, 3][2]", BIGINT, 1);
        assertFunction("ARRAY [2, NULL, 3][2]", BIGINT, null);
        assertFunction("ARRAY [1.0, 2.5, 3.5][3]", DOUBLE, 3.5);
        assertFunction("ARRAY [ARRAY[1, 2], ARRAY[3]][2]", new ArrayType(BIGINT), ImmutableList.of(3L));
        assertFunction("ARRAY [ARRAY[1, 2], NULL, ARRAY[3]][2]", new ArrayType(BIGINT), null);
        assertFunction("ARRAY [ARRAY[1, 2], ARRAY[3]][2][1]", BIGINT, 3);
        assertFunction("ARRAY ['puppies', 'kittens'][2]", VARCHAR, "kittens");
        assertFunction("ARRAY ['puppies', 'kittens', NULL][3]", VARCHAR, null);
        assertFunction("ARRAY [TRUE, FALSE][2]", BOOLEAN, false);
        assertFunction("ARRAY [from_unixtime(1), from_unixtime(100)][1]", TIMESTAMP, sqlTimestamp(1000));
        assertFunction("ARRAY [infinity()][1]", DOUBLE, POSITIVE_INFINITY);
        assertFunction("ARRAY [-infinity()][1]", DOUBLE, NEGATIVE_INFINITY);
        assertFunction("ARRAY [sqrt(-1)][1]", DOUBLE, NaN);
    }

    @Test
    public void testElementAt()
            throws Exception
    {
        String outOfBounds = "Array subscript out of bounds";
        assertInvalidFunction("ELEMENT_AT(ARRAY [], -1)", outOfBounds);
        assertInvalidFunction("ELEMENT_AT(ARRAY [], 0)", "SQL array indices start at 1");
        assertInvalidFunction("ELEMENT_AT(ARRAY [], 1)", outOfBounds);
        assertInvalidFunction("ELEMENT_AT(ARRAY [1, 2, 3], 0)", "SQL array indices start at 1");
        assertInvalidFunction("ELEMENT_AT(ARRAY [1, 2, 3], 4)", outOfBounds);
        assertInvalidFunction("ELEMENT_AT(ARRAY [1, 2, 3], -4)", outOfBounds);

        assertFunction("ELEMENT_AT(ARRAY [NULL], 1)", UNKNOWN, null);
        assertFunction("ELEMENT_AT(ARRAY [NULL], -1)", UNKNOWN, null);
        assertFunction("ELEMENT_AT(ARRAY [NULL, NULL, NULL], 3)", UNKNOWN, null);
        assertFunction("ELEMENT_AT(ARRAY [NULL, NULL, NULL], -1)", UNKNOWN, null);
        assertFunction("1 + ELEMENT_AT(ARRAY [2, 1, 3], 2)", BIGINT, 2);
        assertFunction("1 + ELEMENT_AT(ARRAY [2, 1, 3], -2)", BIGINT, 2);
        assertFunction("ELEMENT_AT(ARRAY [2, 1, 3], 2)", BIGINT, 1);
        assertFunction("ELEMENT_AT(ARRAY [2, 1, 3], -2)", BIGINT, 1);
        assertFunction("ELEMENT_AT(ARRAY [2, NULL, 3], 2)", BIGINT, null);
        assertFunction("ELEMENT_AT(ARRAY [2, NULL, 3], -2)", BIGINT, null);
        assertFunction("ELEMENT_AT(ARRAY [1.0, 2.5, 3.5], 3)", DOUBLE, 3.5);
        assertFunction("ELEMENT_AT(ARRAY [1.0, 2.5, 3.5], -1)", DOUBLE, 3.5);
        assertFunction("ELEMENT_AT(ARRAY [ARRAY [1, 2], ARRAY [3]], 2)", new ArrayType(BIGINT), ImmutableList.of(3L));
        assertFunction("ELEMENT_AT(ARRAY [ARRAY [1, 2], ARRAY [3]], -1)", new ArrayType(BIGINT), ImmutableList.of(3L));
        assertFunction("ELEMENT_AT(ARRAY [ARRAY [1, 2], NULL, ARRAY [3]], 2)", new ArrayType(BIGINT), null);
        assertFunction("ELEMENT_AT(ARRAY [ARRAY [1, 2], NULL, ARRAY [3]], -2)", new ArrayType(BIGINT), null);
        assertFunction("ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], 2) , 1)", BIGINT, 3);
        assertFunction("ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], -1) , 1)", BIGINT, 3);
        assertFunction("ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], 2) , -1)", BIGINT, 3);
        assertFunction("ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], -1) , -1)", BIGINT, 3);
        assertFunction("ELEMENT_AT(ARRAY ['puppies', 'kittens'], 2)", VARCHAR, "kittens");
        assertFunction("ELEMENT_AT(ARRAY ['puppies', 'kittens'], -1)", VARCHAR, "kittens");
        assertFunction("ELEMENT_AT(ARRAY ['puppies', 'kittens', NULL], 3)", VARCHAR, null);
        assertFunction("ELEMENT_AT(ARRAY ['puppies', 'kittens', NULL], -1)", VARCHAR, null);
        assertFunction("ELEMENT_AT(ARRAY [TRUE, FALSE], 2)", BOOLEAN, false);
        assertFunction("ELEMENT_AT(ARRAY [TRUE, FALSE], -1)", BOOLEAN, false);
        assertFunction("ELEMENT_AT(ARRAY [from_unixtime(1), from_unixtime(100)], 1)", TIMESTAMP, sqlTimestamp(1000));
        assertFunction("ELEMENT_AT(ARRAY [from_unixtime(1), from_unixtime(100)], -2)", TIMESTAMP, sqlTimestamp(1000));
        assertFunction("ELEMENT_AT(ARRAY [infinity()], 1)", DOUBLE, POSITIVE_INFINITY);
        assertFunction("ELEMENT_AT(ARRAY [infinity()], -1)", DOUBLE, POSITIVE_INFINITY);
        assertFunction("ELEMENT_AT(ARRAY [-infinity()], 1)", DOUBLE, NEGATIVE_INFINITY);
        assertFunction("ELEMENT_AT(ARRAY [-infinity()], -1)", DOUBLE, NEGATIVE_INFINITY);
        assertFunction("ELEMENT_AT(ARRAY [sqrt(-1)], 1)", DOUBLE, NaN);
        assertFunction("ELEMENT_AT(ARRAY [sqrt(-1)], -1)", DOUBLE, NaN);
    }

    @Test
    public void testSort()
            throws Exception
    {
        assertFunction("ARRAY_SORT(ARRAY[2, 3, 4, 1])", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L, 4L));
        assertFunction("ARRAY_SORT(ARRAY['z', 'f', 's', 'd', 'g'])", new ArrayType(VARCHAR), ImmutableList.of("d", "f", "g", "s", "z"));
        assertFunction("ARRAY_SORT(ARRAY[TRUE, FALSE])", new ArrayType(BOOLEAN), ImmutableList.of(false, true));
        assertFunction("ARRAY_SORT(ARRAY[22.1, 11.1, 1.1, 44.1])", new ArrayType(DOUBLE), ImmutableList.of(1.1, 11.1, 22.1, 44.1));
        assertFunction("ARRAY_SORT(ARRAY [from_unixtime(100), from_unixtime(1), from_unixtime(200)])",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp(1000), sqlTimestamp(100 * 1000), sqlTimestamp(200 * 1000)));
        assertFunction("ARRAY_SORT(ARRAY [ARRAY [1], ARRAY [2]])",
                new ArrayType(new ArrayType(BIGINT)),
                ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(2L)));

        assertInvalidFunction("ARRAY_SORT(ARRAY[color('red'), color('blue')])", FUNCTION_NOT_FOUND.toErrorCode());
    }

    @Test void testDistinct()
            throws Exception
    {
        assertFunction("ARRAY_DISTINCT(ARRAY [])", new ArrayType(UNKNOWN), ImmutableList.of());

        // Order matters here. Result should be stable.
        assertFunction("ARRAY_DISTINCT(ARRAY [2, 3, 4, 3, 1, 2, 3])", new ArrayType(BIGINT), ImmutableList.of(2L, 3L, 4L, 1L));
        assertFunction("ARRAY_DISTINCT(ARRAY [2.2, 3.3, 4.4, 3.3, 1, 2.2, 3.3])", new ArrayType(DOUBLE), ImmutableList.of(2.2, 3.3, 4.4, 1.0));
        assertFunction("ARRAY_DISTINCT(ARRAY [TRUE, TRUE, TRUE])", new ArrayType(BOOLEAN), ImmutableList.of(true));
        assertFunction("ARRAY_DISTINCT(ARRAY [TRUE, FALSE, FALSE, TRUE])", new ArrayType(BOOLEAN), ImmutableList.of(true, false));
        assertFunction("ARRAY_DISTINCT(ARRAY [from_unixtime(100), from_unixtime(1), from_unixtime(100)])", new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp(100 * 1000), sqlTimestamp(1000)));
        assertFunction("ARRAY_DISTINCT(ARRAY ['2', '3', '2'])", new ArrayType(VARCHAR), ImmutableList.of("2", "3"));
        assertFunction("ARRAY_DISTINCT(ARRAY ['BB', 'CCC', 'BB'])", new ArrayType(VARCHAR), ImmutableList.of("BB", "CCC"));
        assertFunction(
                "ARRAY_DISTINCT(ARRAY [ARRAY [1], ARRAY [1, 2], ARRAY [1, 2, 3], ARRAY [1, 2]])",
                new ArrayType(new ArrayType(BIGINT)),
                ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(1L, 2L), ImmutableList.of(1L, 2L, 3L)));

        assertFunction("ARRAY_DISTINCT(ARRAY [NULL, 2.2, 3.3, 4.4, 3.3, 1, 2.2, 3.3])", new ArrayType(DOUBLE), asList(null, 2.2, 3.3, 4.4, 1.0));
        assertFunction("ARRAY_DISTINCT(ARRAY [2, 3, NULL, 4, 3, 1, 2, 3])", new ArrayType(BIGINT), asList(2L, 3L, null, 4L, 1L));
        assertFunction("ARRAY_DISTINCT(ARRAY ['BB', 'CCC', 'BB', NULL])", new ArrayType(VARCHAR), asList("BB", "CCC", null));
        assertFunction("ARRAY_DISTINCT(ARRAY [NULL])", new ArrayType(UNKNOWN), asList((Object) null));
        assertFunction("ARRAY_DISTINCT(ARRAY [NULL, NULL])", new ArrayType(UNKNOWN), asList((Object) null));
        assertFunction("ARRAY_DISTINCT(ARRAY [NULL, NULL, NULL])", new ArrayType(UNKNOWN), asList((Object) null));
    }

    @Test
    public void testSlice()
            throws Exception
    {
        assertFunction("SLICE(ARRAY [1, 2, 3, 4, 5], 1, 4)", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L, 4L));
        assertFunction("SLICE(ARRAY [1, 2], 1, 4)", new ArrayType(BIGINT), ImmutableList.of(1L, 2L));
        assertFunction("SLICE(ARRAY [1, 2, 3, 4, 5], 3, 2)", new ArrayType(BIGINT), ImmutableList.of(3L, 4L));
        assertFunction("SLICE(ARRAY ['1', '2', '3', '4'], 2, 1)", new ArrayType(VARCHAR), ImmutableList.of("2"));
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], 3, 3)", new ArrayType(BIGINT), ImmutableList.of(3L, 4L));
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], -3, 3)", new ArrayType(BIGINT), ImmutableList.of(2L, 3L, 4L));
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], -3, 5)", new ArrayType(BIGINT), ImmutableList.of(2L, 3L, 4L));
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], 1, 0)", new ArrayType(BIGINT), ImmutableList.of());
        assertFunction("SLICE(ARRAY [1, 2, 3, 4], -2, 0)", new ArrayType(BIGINT), ImmutableList.of());
        assertFunction("SLICE(ARRAY [ARRAY [1], ARRAY [2, 3], ARRAY [4, 5, 6]], 1, 2)", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(2L, 3L)));

        assertInvalidFunction("SLICE(ARRAY [1, 2, 3, 4], 1, -1)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("SLICE(ARRAY [1, 2, 3, 4], 0, 1)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testArrayIntersect()
            throws Exception
    {
        assertFunction("ARRAY_INTERSECT(ARRAY [12], ARRAY [10])", new ArrayType(BIGINT), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY ['foo', 'bar', 'baz'], ARRAY ['foo', 'test', 'bar'])", new ArrayType(VARCHAR), ImmutableList.of("bar", "foo"));
        assertFunction("ARRAY_INTERSECT(ARRAY [NULL], ARRAY [NULL, NULL])", new ArrayType(UNKNOWN), asList((Object) null));
        assertFunction("ARRAY_INTERSECT(ARRAY ['abc', NULL, 'xyz', NULL], ARRAY [NULL, 'abc', NULL, NULL])", new ArrayType(VARCHAR), asList(null, "abc"));
        assertFunction("ARRAY_INTERSECT(ARRAY [1, 5], ARRAY [1])", new ArrayType(BIGINT), ImmutableList.of(1L));
        assertFunction("ARRAY_INTERSECT(ARRAY [1, 1, 2, 4], ARRAY [1, 1, 4, 4])", new ArrayType(BIGINT), ImmutableList.of(1L, 4L));
        assertFunction("ARRAY_INTERSECT(ARRAY [2, 8], ARRAY [8, 3])", new ArrayType(BIGINT), ImmutableList.of(8L));
        assertFunction("ARRAY_INTERSECT(ARRAY [IF (RAND() < 1.0, 7, 1) , 2], ARRAY [7])", new ArrayType(BIGINT), ImmutableList.of(7L));
        assertFunction("ARRAY_INTERSECT(ARRAY [1, 5], ARRAY [1.0])", new ArrayType(DOUBLE), ImmutableList.of(1.0));
        assertFunction("ARRAY_INTERSECT(ARRAY [8.3, 1.6, 4.1, 5.2], ARRAY [4.0, 5.2, 8.3, 9.7, 3.5])", new ArrayType(DOUBLE), ImmutableList.of(5.2, 8.3));
        assertFunction("ARRAY_INTERSECT(ARRAY [5.1, 7, 3.0, 4.8, 10], ARRAY [6.5, 10.0, 1.9, 5.1, 3.9, 4.8])", new ArrayType(DOUBLE), ImmutableList.of(4.8, 5.1, 10.0));
        assertFunction("ARRAY_INTERSECT(ARRAY [ARRAY [4, 5], ARRAY [6, 7]], ARRAY [ARRAY [4, 5], ARRAY [6, 8]])", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(4L, 5L)));
    }

    @Test
    public void testComparison()
            throws Exception
    {
        assertFunction("ARRAY [1, 2, 3] = ARRAY [1, 2, 3]", BOOLEAN, true);
        assertFunction("ARRAY [1, 2, 3] != ARRAY [1, 2, 3]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] = ARRAY [TRUE, FALSE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] != ARRAY [TRUE, FALSE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] = ARRAY [1.1, 2.2, 3.3, 4.4]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] != ARRAY [1.1, 2.2, 3.3, 4.4]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens'] = ARRAY ['puppies', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens'] != ARRAY ['puppies', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] = ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] != ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [timestamp '2012-10-31 08:00 UTC'] = ARRAY [timestamp '2012-10-31 01:00 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [timestamp '2012-10-31 08:00 UTC'] != ARRAY [timestamp '2012-10-31 01:00 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] = ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] != ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, false);

        assertFunction("ARRAY [10, 20, 30] != ARRAY [5]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] = ARRAY [5]", BOOLEAN, false);
        assertFunction("ARRAY [1, 2, 3] != ARRAY [3, 2, 1]", BOOLEAN, true);
        assertFunction("ARRAY [1, 2, 3] = ARRAY [3, 2, 1]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] != ARRAY [TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] = ARRAY [TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] != ARRAY [FALSE, FALSE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] = ARRAY [FALSE, FALSE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] != ARRAY [1.1, 2.2]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] = ARRAY [1.1, 2.2]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2, 3.3] != ARRAY [11.1, 22.1, 1.1, 44.1]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2, 3.3] = ARRAY [11.1, 22.1, 1.1, 44.1]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] != ARRAY ['puppies', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] = ARRAY ['puppies', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens'] != ARRAY ['z', 'f', 's', 'd', 'g']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens'] = ARRAY ['z', 'f', 's', 'd', 'g']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] != ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] = ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] != ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] = ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] != ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] = ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] != ARRAY [ARRAY [1, 2, 3], ARRAY [4, 5]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] = ARRAY [ARRAY [1, 2, 3], ARRAY [4, 5]]", BOOLEAN, false);

        assertFunction("ARRAY [10, 20, 30] < ARRAY [10, 20, 40, 50]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] >= ARRAY [10, 20, 40, 50]", BOOLEAN, false);
        assertFunction("ARRAY [10, 20, 30] < ARRAY [10, 40]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] >= ARRAY [10, 40]", BOOLEAN, false);
        assertFunction("ARRAY [10, 20] < ARRAY [10, 20, 30]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20] >= ARRAY [10, 20, 30]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] < ARRAY [TRUE, TRUE, TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] >= ARRAY [TRUE, TRUE, TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE, FALSE] < ARRAY [TRUE, TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE, FALSE] >= ARRAY [TRUE, TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] < ARRAY [TRUE, FALSE, FALSE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] >= ARRAY [TRUE, FALSE, FALSE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] < ARRAY [1.1, 2.2, 4.4, 4.4]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] >= ARRAY [1.1, 2.2, 4.4, 4.4]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] < ARRAY [1.1, 2.2, 5.5]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] >= ARRAY [1.1, 2.2, 5.5]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2] < ARRAY [1.1, 2.2, 5.5]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2] >= ARRAY [1.1, 2.2, 5.5]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] < ARRAY ['puppies', 'lizards', 'lizards']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] >= ARRAY ['puppies', 'lizards', 'lizards']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] < ARRAY ['puppies', 'lizards']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] >= ARRAY ['puppies', 'lizards']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens'] < ARRAY ['puppies', 'kittens', 'lizards']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens'] >= ARRAY ['puppies', 'kittens', 'lizards']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]", BOOLEAN, false);

        assertFunction("ARRAY [10, 20, 30] > ARRAY [10, 20, 20]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] <= ARRAY [10, 20, 20]", BOOLEAN, false);
        assertFunction("ARRAY [10, 20, 30] > ARRAY [10, 20]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] <= ARRAY [10, 20]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, TRUE, TRUE] > ARRAY [TRUE, TRUE, FALSE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, TRUE, TRUE] <= ARRAY [TRUE, TRUE, FALSE]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, TRUE, FALSE] > ARRAY [TRUE, TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, TRUE, FALSE] <= ARRAY [TRUE, TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] > ARRAY [1.1, 2.2, 2.2, 4.4]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] <= ARRAY [1.1, 2.2, 2.2, 4.4]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] > ARRAY [1.1, 2.2, 3.3]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] <= ARRAY [1.1, 2.2, 3.3]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] > ARRAY ['puppies', 'kittens', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] <= ARRAY ['puppies', 'kittens', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] > ARRAY ['puppies', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] <= ARRAY ['puppies', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:20.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:20.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 4]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 4]]", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 3, 4]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 3, 4]]", BOOLEAN, false);

        assertFunction("ARRAY [10, 20, 30] <= ARRAY [50]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] > ARRAY [50]", BOOLEAN, false);
        assertFunction("ARRAY [10, 20, 30] <= ARRAY [10, 20, 30]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] > ARRAY [10, 20, 30]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] <= ARRAY [TRUE, FALSE, true]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] > ARRAY [TRUE, FALSE, true]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE] <= ARRAY [TRUE, FALSE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE] > ARRAY [TRUE, FALSE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] <= ARRAY [2.2, 5.5]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] > ARRAY [2.2, 5.5]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] <= ARRAY [1.1, 2.2, 3.3, 4.4]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] > ARRAY [1.1, 2.2, 3.3, 4.4]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] <= ARRAY ['puppies', 'lizards']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] > ARRAY ['puppies', 'lizards']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens'] <= ARRAY ['puppies', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens'] > ARRAY ['puppies', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]", BOOLEAN, false);

        assertFunction("ARRAY [10, 20, 30] >= ARRAY [10, 20, 30]", BOOLEAN, true);
        assertFunction("ARRAY [10, 20, 30] < ARRAY [10, 20, 30]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] >= ARRAY [TRUE, FALSE, TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] < ARRAY [TRUE, FALSE, TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] >= ARRAY [TRUE]", BOOLEAN, true);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] < ARRAY [TRUE]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] >= ARRAY [1.1, 2.2]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] < ARRAY [1.1, 2.2]", BOOLEAN, false);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] >= ARRAY [1.1, 2.2, 3.3, 4.4]", BOOLEAN, true);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] < ARRAY [1.1, 2.2, 3.3, 4.4]", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] >= ARRAY ['puppies', 'kittens', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens', 'lizards'] < ARRAY ['puppies', 'kittens', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY ['puppies', 'kittens'] >= ARRAY ['puppies', 'kittens']", BOOLEAN, true);
        assertFunction("ARRAY ['puppies', 'kittens'] < ARRAY ['puppies', 'kittens']", BOOLEAN, false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 4]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 4]]", BOOLEAN, false);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, true);
        assertFunction("ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]", BOOLEAN, false);

        assertInvalidFunction("ARRAY [1, NULL] = ARRAY [1, 2]", NOT_SUPPORTED.toErrorCode());
    }

    @Test
    public void testArrayRemove()
            throws Exception
    {
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'foo')", new ArrayType(VARCHAR), ImmutableList.of("bar", "baz"));
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'bar')", new ArrayType(VARCHAR), ImmutableList.of("foo", "baz"));
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'baz')", new ArrayType(VARCHAR), ImmutableList.of("foo", "bar"));
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'zzz')", new ArrayType(VARCHAR), ImmutableList.of("foo", "bar", "baz"));
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'foo', 'foo'], 'foo')", new ArrayType(VARCHAR), ImmutableList.of());
        assertFunction("ARRAY_REMOVE(ARRAY [NULL, 'bar', 'baz'], 'foo')", new ArrayType(VARCHAR), asList(null, "bar", "baz"));
        assertFunction("ARRAY_REMOVE(ARRAY ['foo', 'bar', NULL], 'foo')", new ArrayType(VARCHAR), asList("bar", null));
        assertFunction("ARRAY_REMOVE(ARRAY [1, 2, 3], 1)", new ArrayType(BIGINT), ImmutableList.of(2L, 3L));
        assertFunction("ARRAY_REMOVE(ARRAY [1, 2, 3], 2)", new ArrayType(BIGINT), ImmutableList.of(1L, 3L));
        assertFunction("ARRAY_REMOVE(ARRAY [1, 2, 3], 3)", new ArrayType(BIGINT), ImmutableList.of(1L, 2L));
        assertFunction("ARRAY_REMOVE(ARRAY [1, 2, 3], 4)", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L));
        assertFunction("ARRAY_REMOVE(ARRAY [1, 1, 1], 1)", new ArrayType(BIGINT), ImmutableList.of());
        assertFunction("ARRAY_REMOVE(ARRAY [NULL, 2, 3], 1)", new ArrayType(BIGINT), asList(null, 2L, 3L));
        assertFunction("ARRAY_REMOVE(ARRAY [1, NULL, 3], 1)", new ArrayType(BIGINT), asList(null, 3L));
        assertFunction("ARRAY_REMOVE(ARRAY [-1.23, 3.14], 3.14)", new ArrayType(DOUBLE), ImmutableList.of(-1.23));
        assertFunction("ARRAY_REMOVE(ARRAY [3.14], 0.0)", new ArrayType(DOUBLE), ImmutableList.of(3.14));
        assertFunction("ARRAY_REMOVE(ARRAY [sqrt(-1), 3.14], 3.14)", new ArrayType(DOUBLE), ImmutableList.of(NaN));
        assertFunction("ARRAY_REMOVE(ARRAY [-1.23, sqrt(-1)], nan())", new ArrayType(DOUBLE), ImmutableList.of(-1.23, NaN));
        assertFunction("ARRAY_REMOVE(ARRAY [-1.23, nan()], nan())", new ArrayType(DOUBLE), ImmutableList.of(-1.23, NaN));
        assertFunction("ARRAY_REMOVE(ARRAY [-1.23, infinity()], -1.23)", new ArrayType(DOUBLE), ImmutableList.of(POSITIVE_INFINITY));
        assertFunction("ARRAY_REMOVE(ARRAY [infinity(), 3.14], infinity())", new ArrayType(DOUBLE), ImmutableList.of(3.14));
        assertFunction("ARRAY_REMOVE(ARRAY [-1.23, NULL, 3.14], 3.14)", new ArrayType(DOUBLE), asList(-1.23, null));
        assertFunction("ARRAY_REMOVE(ARRAY [TRUE, FALSE, TRUE], TRUE)", new ArrayType(BOOLEAN), ImmutableList.of(false));
        assertFunction("ARRAY_REMOVE(ARRAY [TRUE, FALSE, TRUE], FALSE)", new ArrayType(BOOLEAN), ImmutableList.of(true, true));
        assertFunction("ARRAY_REMOVE(ARRAY [NULL, FALSE, TRUE], TRUE)", new ArrayType(BOOLEAN), asList(null, false));
        assertFunction("ARRAY_REMOVE(ARRAY [ARRAY ['foo'], ARRAY ['bar'], ARRAY ['baz']], ARRAY ['bar'])", new ArrayType(new ArrayType(VARCHAR)), ImmutableList.of(ImmutableList.of("foo"), ImmutableList.of("baz")));
    }

    public void assertInvalidFunction(String projection, ErrorCode errorCode)
    {
        try {
            assertFunction(projection, UNKNOWN, null);
            fail("Expected error " + errorCode + " from " + projection);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), errorCode);
        }
    }

    private SqlTimestamp sqlTimestamp(long millisUtc)
    {
        return new SqlTimestamp(millisUtc, TEST_SESSION.getTimeZoneKey());
    }
}
