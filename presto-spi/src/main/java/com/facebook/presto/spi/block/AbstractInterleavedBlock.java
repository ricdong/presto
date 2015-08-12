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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;

import java.util.List;

public abstract class AbstractInterleavedBlock
        implements Block
{
    private final int columns;

    protected abstract Block getBlock(int blockIndex);

    @Override
    public abstract InterleavedBlockEncoding getEncoding();

    protected AbstractInterleavedBlock(int columns)
    {
        if (columns < 0) {
            throw new IllegalArgumentException("Number of blocks in InterleavedBlock must be positive");
        }
        this.columns = columns;
    }

    int getBlockCount()
    {
        return columns;
    }

    /**
     * Can only be called after the child class is initialized enough that getBlock will return the right value
     */
    protected InterleavedBlockEncoding computeBlockEncoding()
    {
        BlockEncoding[] individualBlockEncodings = new BlockEncoding[columns];
        for (int i = 0; i < columns; i++) {
            Block block = getBlock(i);
            individualBlockEncodings[i] = block.getEncoding();
        }
        return new InterleavedBlockEncoding(individualBlockEncodings);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        getBlock(blockIndex).writePositionTo(positionInBlock, blockBuilder);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getByte(positionInBlock, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getShort(positionInBlock, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getInt(positionInBlock, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getLong(positionInBlock, offset);
    }

    @Override
    public float getFloat(int position, int offset)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getFloat(positionInBlock, offset);
    }

    @Override
    public double getDouble(int position, int offset)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getDouble(positionInBlock, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getSlice(positionInBlock, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getObject(positionInBlock, clazz);
    }

    @Override
    public int getLength(int position)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).getLength(positionInBlock);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).equals(positionInBlock, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).bytesEqual(positionInBlock, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int hash(int position, int offset, int length)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).hash(positionInBlock, offset, length);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).compareTo(positionInBlock, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).bytesCompare(positionInBlock, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        getBlock(blockIndex).writeBytesTo(positionInBlock, offset, length, blockBuilder);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        // return the underlying block directly, as it is unnecessary to wrap around it if there's only one block
        return getBlock(blockIndex).getSingleValueBlock(positionInBlock);
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getRegion(int position, int length)
    {
        return getRegion(position, length, false);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        return getRegion(position, length, true);
    }

    private Block getRegion(int position, int length, boolean compact)
    {
        int positionCount = getPositionCount();
        if (position < 0 || length < 0 || position + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position (" + position + "), length (" + length + ") in block with " + positionCount + " positions");
        }
        if (length <= 1) {
            // This is not only an optimization. It is required for correctness in the case of length == 0
            int positionInBlock = position / columns;
            if (compact) {
                return getBlock(position % columns).copyRegion(positionInBlock, length);
            }
            else {
                return getBlock(position % columns).getRegion(positionInBlock, length);
            }
        }
        else {
            Block[] resultBlocks = new Block[Math.min(columns, length)];
            for (int newBlockIndex = 0; newBlockIndex < resultBlocks.length; newBlockIndex++) {
                int positionInBlock = (position + newBlockIndex) / columns;
                int subBlockLength = (length + columns - 1 - newBlockIndex) / columns;
                if (compact) {
                    resultBlocks[newBlockIndex] = getBlock((newBlockIndex + position) % columns).copyRegion(positionInBlock, subBlockLength);
                }
                else {
                    resultBlocks[newBlockIndex] = getBlock((newBlockIndex + position) % columns).getRegion(positionInBlock, subBlockLength);
                }
            }
            return new InterleavedBlock(resultBlocks);
        }
    }

    @Override
    public boolean isNull(int position)
    {
        int blockIndex = position % columns;
        int positionInBlock = position / columns;

        return getBlock(blockIndex).isNull(positionInBlock);
    }

    @Override
    public void assureLoaded()
    {
    }
}
