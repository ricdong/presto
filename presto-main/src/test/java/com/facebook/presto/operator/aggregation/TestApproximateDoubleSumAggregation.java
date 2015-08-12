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

import com.facebook.presto.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;

public class TestApproximateDoubleSumAggregation
        extends AbstractTestApproximateAggregationFunction
{
    @Override
    protected Type getType()
    {
        return DOUBLE;
    }

    @Override
    protected Double getExpectedValue(List<Number> values)
    {
        List<Number> nonNull = values.stream()
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        if (nonNull.isEmpty()) {
            return null;
        }
        double sum = 0;
        for (Number value : nonNull) {
            sum += value.doubleValue();
        }
        return sum;
    }

    @Override
    protected String getFunctionName()
    {
        return "sum";
    }
}
