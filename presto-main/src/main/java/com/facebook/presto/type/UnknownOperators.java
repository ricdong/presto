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
import com.facebook.presto.spi.type.StandardTypes;

import javax.annotation.Nullable;

import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;

public final class UnknownOperators
{
    private UnknownOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @Nullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean equal(@SqlType("unknown") @Nullable Void left, @SqlType("unknown") @Nullable Void right)
    {
        return null;
    }

    @ScalarOperator(NOT_EQUAL)
    @Nullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean notEqual(@SqlType("unknown") @Nullable Void left, @SqlType("unknown") @Nullable Void right)
    {
        return null;
    }

    @ScalarOperator(LESS_THAN)
    @Nullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean lessThan(@SqlType("unknown") @Nullable Void left, @SqlType("unknown") @Nullable Void right)
    {
        return null;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @Nullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean lessThanOrEqual(@SqlType("unknown") @Nullable Void left, @SqlType("unknown") @Nullable Void right)
    {
        return null;
    }

    @ScalarOperator(GREATER_THAN)
    @Nullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean greaterThan(@SqlType("unknown") @Nullable Void left, @SqlType("unknown") @Nullable Void right)
    {
        return null;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @Nullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean greaterThanOrEqual(@SqlType("unknown") @Nullable Void left, @SqlType("unknown") @Nullable Void right)
    {
        return null;
    }

    @ScalarOperator(BETWEEN)
    @Nullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean between(@SqlType("unknown") @Nullable Void value, @SqlType("unknown") @Nullable Void min, @SqlType("unknown") @Nullable Void max)
    {
        return null;
    }

    @ScalarOperator(HASH_CODE)
    @Nullable
    @SqlType(StandardTypes.BIGINT)
    public static Long hashCode(@SqlType("unknown") @Nullable Void value)
    {
        return null;
    }
}
