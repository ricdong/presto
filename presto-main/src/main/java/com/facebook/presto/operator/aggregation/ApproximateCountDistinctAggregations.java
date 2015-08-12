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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.aggregation.state.HyperLogLogState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Failures.checkCondition;

@AggregationFunction("approx_distinct")
public final class ApproximateCountDistinctAggregations
{
    public static final InternalAggregationFunction LONG_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(ApproximateCountDistinctAggregations.class, BIGINT, ImmutableList.<Type>of(BIGINT, DOUBLE));
    public static final InternalAggregationFunction DOUBLE_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(ApproximateCountDistinctAggregations.class, BIGINT, ImmutableList.<Type>of(DOUBLE, DOUBLE));
    public static final InternalAggregationFunction VARBINARY_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS = new AggregationCompiler().generateAggregationFunction(ApproximateCountDistinctAggregations.class, BIGINT, ImmutableList.<Type>of(VARCHAR, DOUBLE));
    private static final double DEFAULT_STANDARD_ERROR = 0.023;
    private static final double LOWEST_MAX_STANDARD_ERROR = 0.01150;
    private static final double HIGHEST_MAX_STANDARD_ERROR = 0.26000;

    private ApproximateCountDistinctAggregations() {}

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        input(state, Double.doubleToLongBits(value), maxStandardError);
    }

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void input(HyperLogLogState state, @SqlType(StandardTypes.VARCHAR) Slice value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    private static HyperLogLog getOrCreateHyperLogLog(HyperLogLogState state, double maxStandardError)
    {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = HyperLogLog.newInstance(standardErrorToBuckets(maxStandardError));
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    @VisibleForTesting
    static int standardErrorToBuckets(double maxStandardError)
    {
        checkCondition(maxStandardError >= LOWEST_MAX_STANDARD_ERROR && maxStandardError <= HIGHEST_MAX_STANDARD_ERROR,
                INVALID_FUNCTION_ARGUMENT,
                "Max standard error must be in [%s, %s]: %s", LOWEST_MAX_STANDARD_ERROR, HIGHEST_MAX_STANDARD_ERROR, maxStandardError);
        return log2Ceiling((int) Math.ceil(1.0816 / (maxStandardError * maxStandardError)));
    }

    private static int log2Ceiling(int value)
    {
        return Integer.highestOneBit(value - 1) << 1;
    }

    @CombineFunction
    public static void combineState(HyperLogLogState state, HyperLogLogState otherState)
    {
        HyperLogLog input = otherState.getHyperLogLog();

        HyperLogLog previous = state.getHyperLogLog();
        if (previous == null) {
            state.setHyperLogLog(input);
            state.addMemoryUsage(input.estimatedInMemorySize());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySize());
            previous.mergeWith(input);
            state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(HyperLogLogState state, BlockBuilder out)
    {
        HyperLogLog hyperLogLog = state.getHyperLogLog();
        if (hyperLogLog == null) {
            BIGINT.writeLong(out, 0);
        }
        else {
            BIGINT.writeLong(out, hyperLogLog.cardinality());
        }
    }
}
