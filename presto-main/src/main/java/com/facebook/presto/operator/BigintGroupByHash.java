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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.BigintOperators;
import com.facebook.presto.util.array.IntBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.type.TypeUtils.NULL_HASH_CODE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;

public class BigintGroupByHash
        implements GroupByHash
{
    private static final float FILL_RATIO = 0.9f;
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final List<Type> TYPES_WITH_RAW_HASH = ImmutableList.of(BIGINT, BIGINT);

    private final int hashChannel;
    private final int maskChannel;
    private final boolean outputRawHash;

    private int maxFill;
    private int mask;

    // the hash table from values to groupIds
    private LongBigArray values;
    private IntBigArray groupIds;

    // groupId for the null value
    private int nullGroupId = -1;

    // reverse index from the groupId back to the value
    private final LongBigArray valuesByGroupId;

    private int nextGroupId;

    public BigintGroupByHash(int hashChannel, Optional<Integer> maskChannel, boolean outputRawHash, int expectedSize)
    {
        checkArgument(hashChannel >= 0, "hashChannel must be at least zero");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.hashChannel = hashChannel;
        this.maskChannel = checkNotNull(maskChannel, "maskChannel is null").orElse(-1);
        this.outputRawHash = outputRawHash;

        int hashSize = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashSize);
        mask = hashSize - 1;
        values = new LongBigArray();
        values.ensureCapacity(hashSize);
        groupIds = new IntBigArray(-1);
        groupIds.ensureCapacity(hashSize);

        valuesByGroupId = new LongBigArray();
        valuesByGroupId.ensureCapacity(hashSize);
    }

    @Override
    public long getEstimatedSize()
    {
        return groupIds.sizeOf() +
                values.sizeOf() +
                valuesByGroupId.sizeOf();
    }

    @Override
    public List<Type> getTypes()
    {
        return outputRawHash ? TYPES_WITH_RAW_HASH : TYPES;
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
        if (groupId == nullGroupId) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, valuesByGroupId.get(groupId));
        }

        if (outputRawHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + 1);
            if (groupId == nullGroupId) {
                BIGINT.writeLong(hashBlockBuilder, NULL_HASH_CODE);
            }
            else {
                BIGINT.writeLong(hashBlockBuilder, BigintOperators.hashCode(valuesByGroupId.get(groupId)));
            }
        }
    }

    @Override
    public void addPage(Page page)
    {
        int positionCount = page.getPositionCount();

        Block maskBlock = null;
        if (maskChannel >= 0) {
            maskBlock = page.getBlock(maskChannel);
        }

        // get the group id for each position
        Block block = page.getBlock(hashChannel);
        for (int position = 0; position < positionCount; position++) {
            // skip masked rows
            if (maskBlock != null && !BOOLEAN.getBoolean(maskBlock, position)) {
                continue;
            }

            // get the group for the current row
            putIfAbsent(position, block);
        }
    }

    @Override
    public GroupByIdBlock getGroupIds(Page page)
    {
        int positionCount = page.getPositionCount();

        // we know the exact size required for the block
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(positionCount);

        Block maskBlock = null;
        if (maskChannel >= 0) {
            maskBlock = page.getBlock(maskChannel);
        }

        // get the group id for each position
        Block block = page.getBlock(hashChannel);
        for (int position = 0; position < positionCount; position++) {
            // skip masked rows
            if (maskBlock != null && !BOOLEAN.getBoolean(maskBlock, position)) {
                blockBuilder.appendNull();
                continue;
            }

            // get the group for the current row
            int groupId = putIfAbsent(position, block);

            // output the group id for this row
            BIGINT.writeLong(blockBuilder, groupId);
        }
        return new GroupByIdBlock(nextGroupId, blockBuilder.build());
    }

    @Override
    public boolean contains(int position, Page page)
    {
        Block block = page.getBlock(hashChannel);
        if (block.isNull(position)) {
            return nullGroupId >= 0;
        }

        long value = BIGINT.getLong(block, position);
        int hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            int groupId = groupIds.get(hashPosition);
            if (groupId == -1) {
                return false;
            }
            else if (value == values.get(hashPosition)) {
                return true;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }
    }

    @Override
    public int putIfAbsent(int position, Page page)
    {
        Block block = page.getBlock(hashChannel);
        return putIfAbsent(position, block);
    }

    private int putIfAbsent(int position, Block block)
    {
        if (block.isNull(position)) {
            if (nullGroupId < 0) {
                // set null group id
                nullGroupId = nextGroupId++;
            }

            return nullGroupId;
        }

        long value = BIGINT.getLong(block, position);
        int hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            int groupId = groupIds.get(hashPosition);
            if (groupId == -1) {
                break;
            }

            if (value == values.get(hashPosition)) {
                return groupId;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        return addNewGroup(hashPosition, value);
    }

    private int addNewGroup(int hashPosition, long value)
    {
        // record group id in hash
        int groupId = nextGroupId++;

        values.set(hashPosition, value);
        valuesByGroupId.set(groupId, value);
        groupIds.set(hashPosition, groupId);

        // increase capacity, if necessary
        if (nextGroupId >= maxFill) {
            rehash(maxFill * 2);
        }
        return groupId;
    }

    private void rehash(int size)
    {
        int newSize = arraySize(size + 1, FILL_RATIO);

        int newMask = newSize - 1;
        LongBigArray newValues = new LongBigArray();
        newValues.ensureCapacity(newSize);
        IntBigArray newGroupIds = new IntBigArray(-1);
        newGroupIds.ensureCapacity(newSize);

        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            long value = valuesByGroupId.get(groupId);

            // find an empty slot for the address
            int hashPosition = getHashPosition(value, newMask);
            while (newGroupIds.get(hashPosition) != -1) {
                hashPosition = (hashPosition + 1) & newMask;
            }

            // record the mapping
            newValues.set(hashPosition, value);
            newGroupIds.set(hashPosition, groupId);
        }

        mask = newMask;
        maxFill = calculateMaxFill(newSize);
        values = newValues;
        groupIds = newGroupIds;

        this.valuesByGroupId.ensureCapacity(maxFill);
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
