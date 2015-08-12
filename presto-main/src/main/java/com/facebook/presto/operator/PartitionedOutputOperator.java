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
package com.facebook.presto.operator;

import com.facebook.presto.HashPagePartitionFunction;
import com.facebook.presto.OutputBuffers;
import com.facebook.presto.PagePartitionFunction;
import com.facebook.presto.PartitionedPagePartitionFunction;
import com.facebook.presto.execution.SharedBuffer;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static com.facebook.presto.operator.PartitionGenerator.createHashPartitionGenerator;
import static com.facebook.presto.operator.PartitionGenerator.createRoundRobinPartitionGenerator;
import static com.facebook.presto.sql.planner.PlanFragment.NullPartitioning.REPLICATE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputOperator
        implements Operator
{
    public static class PartitionedOutputFactory
            implements OutputFactory
    {
        private final SharedBuffer sharedBuffer;

        public PartitionedOutputFactory(SharedBuffer sharedBuffer)
        {
            this.sharedBuffer = requireNonNull(sharedBuffer, "sharedBuffer is null");
        }

        @Override
        public OperatorFactory createOutputOperator(int operatorId, List<Type> sourceTypes)
        {
            return new PartitionedOutputOperatorFactory(operatorId, sourceTypes, sharedBuffer);
        }
    }

    public static class PartitionedOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> sourceTypes;
        private final SharedBuffer sharedBuffer;

        public PartitionedOutputOperatorFactory(int operatorId, List<Type> sourceTypes, SharedBuffer sharedBuffer)
        {
            this.operatorId = operatorId;
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.sharedBuffer = requireNonNull(sharedBuffer, "sharedBuffer is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, PartitionedOutputOperator.class.getSimpleName());
            return new PartitionedOutputOperator(operatorContext, sourceTypes, sharedBuffer);
        }

        @Override
        public void close()
        {
        }
    }

    private final OperatorContext operatorContext;
    private final ListenableFuture<PartitionFunction> partitionFunction;
    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private boolean finished;

    public PartitionedOutputOperator(OperatorContext operatorContext, List<Type> sourceTypes, SharedBuffer sharedBuffer)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.partitionFunction = Futures.transform(sharedBuffer.getFinalOutputBuffers(), (OutputBuffers outputBuffers) -> {
            return new PartitionFunction(sharedBuffer, sourceTypes, outputBuffers);
        });
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.of();
    }

    @Override
    public void finish()
    {
        finished = true;
        blocked = getUnchecked(partitionFunction).flush(true);
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!partitionFunction.isDone()) {
            return partitionFunction;
        }
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(isBlocked().isDone(), "output is already blocked");

        if (page.getPositionCount() == 0) {
            return;
        }

        blocked = getUnchecked(partitionFunction).partitionPage(page);

        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    private static class PartitionFunction
    {
        private final SharedBuffer sharedBuffer;
        private final List<Type> sourceTypes;
        private final PartitionGenerator partitionGenerator;
        private final int partitionCount;
        private final List<PageBuilder> pageBuilders;
        private final OptionalInt nullChannel; // when present, send the position to every partition if this channel is null.

        public PartitionFunction(SharedBuffer sharedBuffer, List<Type> sourceTypes, OutputBuffers outputBuffers)
        {
            this.sharedBuffer = requireNonNull(sharedBuffer, "sharedBuffer is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");

            // verify output buffers are a complete set of hash partitions
            checkArgument(outputBuffers.isNoMoreBufferIds(), "output buffers is not final version");
            Map<TaskId, PagePartitionFunction> buffers = outputBuffers.getBuffers();
            checkArgument(!buffers.isEmpty(), "output buffers is empty");
            checkArgument(buffers.values().stream().allMatch(PartitionedPagePartitionFunction.class::isInstance), "None of the buffers can be unpartitioned");

            Collection<PagePartitionFunction> partitionFunctions = buffers.values();

            checkArgument(partitionFunctions.stream()
                    .map(PagePartitionFunction::getPartitionCount)
                    .distinct().count() == 1,
                    "All buffers must have the same partition count");

            checkArgument(partitionFunctions.stream()
                    .map(PagePartitionFunction::getPartition)
                    .distinct().count() == partitionFunctions.size(),
                    "All buffers must have a different partition");

            PagePartitionFunction partitionFunction = partitionFunctions.stream().findAny().get();
            if (partitionFunction instanceof HashPagePartitionFunction) {
                HashPagePartitionFunction hashPartitionFunction = (HashPagePartitionFunction) partitionFunction;
                partitionGenerator = createHashPartitionGenerator(hashPartitionFunction.getHashChannel(), hashPartitionFunction.getPartitioningChannels(), hashPartitionFunction.getTypes());
                if (hashPartitionFunction.getNullPartitioning() == REPLICATE) {
                    List<Integer> partitioningChannels = hashPartitionFunction.getPartitioningChannels();
                    checkState(partitioningChannels.size() == 1);
                    nullChannel = OptionalInt.of(Iterables.getOnlyElement(partitioningChannels));
                }
                else {
                    nullChannel = OptionalInt.empty();
                }
            }
            else {
                partitionGenerator = createRoundRobinPartitionGenerator();
                nullChannel = OptionalInt.empty();
            }

            partitionCount = partitionFunction.getPartitionCount();

            ImmutableList.Builder<PageBuilder> pageBuilders = ImmutableList.builder();
            for (int i = 0; i < partitionCount; i++) {
                pageBuilders.add(new PageBuilder(sourceTypes));
            }
            this.pageBuilders = pageBuilders.build();
        }

        public ListenableFuture<?> partitionPage(Page page)
        {
            requireNonNull(page, "page is null");

            for (int position = 0; position < page.getPositionCount(); position++) {
                if (nullChannel.isPresent() && page.getBlock(nullChannel.getAsInt()).isNull(position)) {
                    for (int i = 0; i < partitionCount; i++) {
                        PageBuilder pageBuilder = pageBuilders.get(i);
                        pageBuilder.declarePosition();

                        for (int channel = 0; channel < sourceTypes.size(); channel++) {
                            Type type = sourceTypes.get(channel);
                            type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
                        }
                    }
                }
                else {
                    int partitionHashBucket = partitionGenerator.getPartitionBucket(partitionCount, position, page);
                    PageBuilder pageBuilder = pageBuilders.get(partitionHashBucket);
                    pageBuilder.declarePosition();

                    for (int channel = 0; channel < sourceTypes.size(); channel++) {
                        Type type = sourceTypes.get(channel);
                        type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
                    }
                }
            }
            return flush(false);
        }

        public ListenableFuture<?> flush(boolean force)
        {
            // add all full pages to output buffer
            List<ListenableFuture<?>> blockedFutures = new ArrayList<>();
            for (int partition = 0; partition < partitionCount; partition++) {
                PageBuilder partitionPageBuilder = pageBuilders.get(partition);
                if (!partitionPageBuilder.isEmpty() && (force || partitionPageBuilder.isFull())) {
                    Page pagePartition = partitionPageBuilder.build();
                    partitionPageBuilder.reset();

                    blockedFutures.add(sharedBuffer.enqueue(partition, pagePartition));
                }
            }
            ListenableFuture<?> future = Futures.allAsList(blockedFutures);
            if (future.isDone()) {
                return NOT_BLOCKED;
            }
            return future;
        }
    }
}
