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

import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.ApproximateUtils.countError;
import static com.facebook.presto.operator.aggregation.ApproximateUtils.formatApproximateResult;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

@AggregationFunction(value = "count", approximate = true)
public final class ApproximateCountColumnAggregations
{
    private ApproximateCountColumnAggregations() {}

    @InputFunction
    public static void booleanInput(ApproximateCountState state, @BlockPosition @SqlType(StandardTypes.BOOLEAN) Block block, @BlockIndex int index, @SampleWeight long sampleWeight)
    {
        state.setCount(state.getCount() + sampleWeight);
        state.setSamples(state.getSamples() + 1);
    }

    @InputFunction
    public static void bigintInput(ApproximateCountState state, @BlockPosition @SqlType(StandardTypes.BIGINT) Block block, @BlockIndex int index, @SampleWeight long sampleWeight)
    {
        state.setCount(state.getCount() + sampleWeight);
        state.setSamples(state.getSamples() + 1);
    }

    @InputFunction
    public static void doubleInput(ApproximateCountState state, @BlockPosition @SqlType(StandardTypes.DOUBLE) Block block, @BlockIndex int index, @SampleWeight long sampleWeight)
    {
        state.setCount(state.getCount() + sampleWeight);
        state.setSamples(state.getSamples() + 1);
    }

    @InputFunction
    public static void varcharInput(ApproximateCountState state, @BlockPosition @SqlType(StandardTypes.VARCHAR) Block block, @BlockIndex int index, @SampleWeight long sampleWeight)
    {
        state.setCount(state.getCount() + sampleWeight);
        state.setSamples(state.getSamples() + 1);
    }

    @CombineFunction
    public static void combine(ApproximateCountState state, ApproximateCountState otherState)
    {
        state.setCount(state.getCount() + otherState.getCount());
        state.setSamples(state.getSamples() + otherState.getSamples());
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(ApproximateCountState state, double confidence, BlockBuilder out)
    {
        String result = formatApproximateResult(state.getCount(), countError(state.getSamples(), state.getCount()), confidence, true);
        VARCHAR.writeSlice(out, Slices.utf8Slice(result));
    }

    public interface ApproximateCountState
            extends AccumulatorState
    {
        long getCount();

        void setCount(long value);

        long getSamples();

        void setSamples(long value);
    }
}
