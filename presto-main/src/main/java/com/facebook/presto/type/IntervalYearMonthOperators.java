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

import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.type.SqlIntervalYearMonth;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.DIVIDE;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.MULTIPLY;
import static com.facebook.presto.metadata.OperatorType.NEGATION;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static io.airlift.slice.Slices.utf8Slice;

public final class IntervalYearMonthOperators
{
    private IntervalYearMonthOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long add(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left + right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long subtract(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left - right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long multiplyByBigint(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left * right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long multiplyByDouble(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return (long) (left * right);
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long bigintMultiply(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left * right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long doubleMultiply(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return (long) (left * right);
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long divideByDouble(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return (long) (left / right);
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long negate(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value)
    {
        return -value;
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long min,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castToSlice(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value)
    {
        return utf8Slice(SqlIntervalYearMonth.formatMonths(value));
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value)
    {
        return (int) (value ^ (value >>> 32));
    }
}
