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
package com.facebook.presto.spi;

import com.facebook.presto.spi.block.Block;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class Page
{
    private final Block[] blocks;
    private final int positionCount;
    private final AtomicLong sizeInBytes = new AtomicLong(-1);
    private final AtomicLong retainedSizeInBytes = new AtomicLong(-1);

    public Page(Block... blocks)
    {
        this(determinePositionCount(blocks), blocks);
    }

    public Page(int positionCount, Block... blocks)
    {
        requireNonNull(blocks, "blocks is null");
        this.blocks = Arrays.copyOf(blocks, blocks.length);
        this.positionCount = positionCount;
    }

    public int getChannelCount()
    {
        return blocks.length;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public long getSizeInBytes()
    {
        long sizeInBytes = this.sizeInBytes.get();
        if (sizeInBytes < 0) {
            sizeInBytes = 0;
            for (Block block : blocks) {
                sizeInBytes += block.getSizeInBytes();
            }
            this.sizeInBytes.set(sizeInBytes);
        }
        return sizeInBytes;
    }

    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = this.retainedSizeInBytes.get();
        if (retainedSizeInBytes < 0) {
            retainedSizeInBytes = 0;
            for (Block block : blocks) {
                retainedSizeInBytes += block.getRetainedSizeInBytes();
            }
            this.retainedSizeInBytes.set(retainedSizeInBytes);
        }
        return retainedSizeInBytes;
    }

    public Block[] getBlocks()
    {
        return blocks.clone();
    }

    public Block getBlock(int channel)
    {
        return blocks[channel];
    }

    public Page getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in page with " + positionCount + " positions");
        }

        int channelCount = getChannelCount();
        Block[] slicedBlocks = new Block[channelCount];
        for (int i = 0; i < channelCount; i++) {
            slicedBlocks[i] = blocks[i].getRegion(positionOffset, length);
        }
        return new Page(length, slicedBlocks);
    }

    /**
     * Assures that all data for the block is in memory.
     *
     * This allows streaming data sources to skip sections that are not
     * accessed in a query.
     */
    public void assureLoaded()
    {
        for (Block block : blocks) {
            block.assureLoaded();
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Page{");
        builder.append("positions=").append(positionCount);
        builder.append(", channels=").append(getChannelCount());
        builder.append('}');
        builder.append("@").append(Integer.toHexString(System.identityHashCode(this)));
        return builder.toString();
    }

    private static int determinePositionCount(Block... blocks)
    {
        requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks is empty");
        }

        return blocks[0].getPositionCount();
    }
}
