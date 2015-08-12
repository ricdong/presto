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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

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
import static com.facebook.presto.metadata.OperatorType.MODULUS;
import static com.facebook.presto.metadata.OperatorType.MULTIPLY;
import static com.facebook.presto.metadata.OperatorType.NEGATION;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class BigintOperators
{
    private BigintOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.BIGINT)
    public static long add(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left + right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.BIGINT)
    public static long subtract(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left - right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.BIGINT)
    public static long multiply(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left * right;
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.BIGINT)
    public static long divide(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        try {
            return left / right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(DIVISION_BY_ZERO, e);
        }
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.BIGINT)
    public static long modulus(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        try {
            return left % right;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(DIVISION_BY_ZERO, e);
        }
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.BIGINT)
    public static long negate(@SqlType(StandardTypes.BIGINT) long value)
    {
        return -value;
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long min, @SqlType(StandardTypes.BIGINT) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.BIGINT) long value)
    {
        return value != 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.BIGINT) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castToVarchar(@SqlType(StandardTypes.BIGINT) long value)
    {
        // todo optimize me
        return Slices.copiedBuffer(String.valueOf(value), UTF_8);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.BIGINT) long value)
    {
        return (int) (value ^ (value >>> 32));
    }
}
