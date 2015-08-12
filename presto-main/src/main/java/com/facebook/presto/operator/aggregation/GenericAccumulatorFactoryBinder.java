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

import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.google.common.base.Throwables;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class GenericAccumulatorFactoryBinder
        implements AccumulatorFactoryBinder
{
    private final boolean approximationSupported;
    private final AccumulatorStateSerializer<?> stateSerializer;
    private final AccumulatorStateFactory<?> stateFactory;
    private final Constructor<? extends Accumulator> accumulatorConstructor;
    private final Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor;

    public GenericAccumulatorFactoryBinder(
            AccumulatorStateSerializer<?> stateSerializer,
            AccumulatorStateFactory<?> stateFactory,
            Class<? extends Accumulator> accumulatorClass,
            Class<? extends GroupedAccumulator> groupedAccumulatorClass,
            boolean approximationSupported)
    {
        this.stateSerializer = checkNotNull(stateSerializer, "stateSerializer is null");
        this.stateFactory = checkNotNull(stateFactory, "stateFactory is null");
        this.approximationSupported = approximationSupported;

        try {
            accumulatorConstructor = accumulatorClass.getConstructor(
                    AccumulatorStateSerializer.class,
                    AccumulatorStateFactory.class,
                    List.class,
                    Optional.class,
                    Optional.class,
                    double.class);

            groupedAccumulatorConstructor = groupedAccumulatorClass.getConstructor(
                    AccumulatorStateSerializer.class,
                    AccumulatorStateFactory.class,
                    List.class,
                    Optional.class,
                    Optional.class,
                    double.class);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public AccumulatorFactory bind(List<Integer> argumentChannels, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
    {
        if (!approximationSupported) {
            checkArgument(confidence == 1.0, "Approximate queries not supported");
            checkArgument(!sampleWeightChannel.isPresent(), "Sampled data not supported");
        }
        return new GenericAccumulatorFactory(stateSerializer, stateFactory, accumulatorConstructor, groupedAccumulatorConstructor, argumentChannels, maskChannel, sampleWeightChannel, confidence);
    }
}
