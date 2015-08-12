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
package com.facebook.presto.benchmark;

import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class HashAggregationBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    public HashAggregationBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hash_agg", 5, 25);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        OperatorFactory tableScanOperator = createTableScanOperator(0, "orders", "orderstatus", "totalprice");
        List<Type> types = ImmutableList.of(tableScanOperator.getTypes().get(0));
        HashAggregationOperatorFactory aggregationOperator = new HashAggregationOperatorFactory(1,
                types,
                Ints.asList(0),
                Step.SINGLE,
                ImmutableList.of(DOUBLE_SUM.bind(ImmutableList.of(1), Optional.empty(), Optional.empty(), 1.0)),
                Optional.empty(),
                Optional.empty(),
                100_000,
                new DataSize(16, MEGABYTE));
        return ImmutableList.of(tableScanOperator, aggregationOperator);
    }

    public static void main(String[] args)
    {
        new HashAggregationBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
