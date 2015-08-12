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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeUtils;
import com.facebook.presto.util.array.IntBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.type.TypeUtils.expectedValueSize;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;

public class TypedHistogram
{
    private static final float FILL_RATIO = 0.9f;
    private static final long FOUR_MEGABYTES = new DataSize(4, MEGABYTE).toBytes();

    private int maxFill;
    private int mask;

    private Type type;

    private BlockBuilder values;
    private IntBigArray hashPositions;
    private final LongBigArray counts;

    public TypedHistogram(Type type, int expectedSize)
    {
        this.type = type;

        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        int hashSize = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashSize);
        mask = hashSize - 1;
        values = this.type.createBlockBuilder(new BlockBuilderStatus(), hashSize, expectedValueSize(type, hashSize));
        hashPositions = new IntBigArray(-1);
        hashPositions.ensureCapacity(hashSize);
        counts = new LongBigArray();
        counts.ensureCapacity(hashSize);
    }

    public TypedHistogram(Block block, Type type, int expectedSize)
    {
        this(type, expectedSize);
        checkNotNull(block, "block is null");
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            add(i, block, BIGINT.getLong(block, i + 1));
        }
    }

    public long getEstimatedSize()
    {
        return values.getRetainedSizeInBytes() +
                counts.sizeOf() +
                hashPositions.sizeOf();
    }

    private Block getValues()
    {
        return values.build();
    }

    private LongBigArray getCounts()
    {
        return counts;
    }

    public Block serialize()
    {
        Block valuesBlock = values.build();
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(type, BIGINT), new BlockBuilderStatus(), valuesBlock.getPositionCount() * 2);
        for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
            type.appendTo(valuesBlock, i, blockBuilder);
            BIGINT.writeLong(blockBuilder, counts.get(i));
        }
        return blockBuilder.build();
    }

    public void addAll(TypedHistogram other)
    {
        Block otherValues = other.getValues();
        LongBigArray otherCounts = other.getCounts();
        for (int i = 0; i < otherValues.getPositionCount(); i++) {
            long count = otherCounts.get(i);
            if (count > 0) {
                add(i, otherValues, count);
            }
        }
    }

    public void add(int position, Block block, long count)
    {
        int hashPosition = getHashPosition(TypeUtils.hashPosition(type, block, position), mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            if (hashPositions.get(hashPosition) == -1) {
                break;
            }

            if (type.equalTo(block, position, values, hashPositions.get(hashPosition))) {
                counts.add(hashPositions.get(hashPosition), count);
                return;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        addNewGroup(hashPosition, position, block, count);
        return;
    }

    private void addNewGroup(int hashPosition, int position, Block block, long count)
    {
        hashPositions.set(hashPosition, values.getPositionCount());
        counts.set(values.getPositionCount(), count);
        type.appendTo(block, position, values);

        // increase capacity, if necessary
        if (values.getPositionCount() >= maxFill) {
            rehash(maxFill * 2);
        }

        if (getEstimatedSize() > FOUR_MEGABYTES) {
            throw new ExceededMemoryLimitException(new DataSize(4, MEGABYTE));
        }

        return;
    }

    private void rehash(int size)
    {
        int newSize = arraySize(size + 1, FILL_RATIO);

        int newMask = newSize - 1;
        IntBigArray newHashPositions = new IntBigArray(-1);
        newHashPositions.ensureCapacity(newSize);

        for (int i = 0; i < values.getPositionCount(); i++) {
            // find an empty slot for the address
            int hashPosition = getHashPosition(TypeUtils.hashPosition(type, values, i), newMask);
            while (hashPositions.get(hashPosition) != -1) {
                hashPosition = (hashPosition + 1) & newMask;
            }

            // record the mapping
            newHashPositions.set(hashPosition, i);
        }

        mask = newMask;
        maxFill = calculateMaxFill(newSize);
        hashPositions = newHashPositions;

        this.counts.ensureCapacity(maxFill);
    }

    private static int getHashPosition(long rawHash, int mask)
    {
        return ((int) murmurHash3(rawHash)) & mask;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }
}
