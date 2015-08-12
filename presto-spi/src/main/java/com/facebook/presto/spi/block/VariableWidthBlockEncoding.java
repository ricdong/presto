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

import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.block.EncoderUtil.decodeNullBits;
import static com.facebook.presto.spi.block.EncoderUtil.encodeNullsAsBits;

public class VariableWidthBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<VariableWidthBlockEncoding> FACTORY = new VariableWidthBlockEncodingFactory();
    private static final String NAME = "VARIABLE_WIDTH";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) block;

        int positionCount = variableWidthBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // offsets
        int totalLength = 0;
        for (int position = 0; position < positionCount; position++) {
            int length = variableWidthBlock.getLength(position);
            sliceOutput.appendInt(length);
            totalLength += length;
        }

        encodeNullsAsBits(sliceOutput, variableWidthBlock);

        sliceOutput
                .appendInt(totalLength)
                .writeBytes(variableWidthBlock.getRawSlice(0), variableWidthBlock.getPositionOffset(0), totalLength);
    }

    @Override
    public int getEstimatedSize(Block block)
    {
        int positionCount = block.getPositionCount();

        int size = 4; // positionCount integer bytes
        int totalLength = 0;
        for (int position = 0; position < positionCount; position++) {
            totalLength += block.getLength(position);
            size += 4; // length integer bytes
        }

        size += positionCount / 8 + 1; // one byte null bits per eight elements and possibly last null bits
        size += 4 + totalLength; // totalLength integer bytes and data bytes

        return size;
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        // offsets
        int[] offsets = new int[positionCount + 1];
        int offset = 0;
        for (int position = 0; position < positionCount; position++) {
            offset += sliceInput.readInt();
            offsets[position + 1] = offset;
        }

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);

        int blockSize = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(blockSize);

        return new VariableWidthBlock(positionCount, slice, Slices.wrappedIntArray(offsets), Slices.wrappedBooleanArray(valueIsNull));
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public static class VariableWidthBlockEncodingFactory
            implements BlockEncodingFactory<VariableWidthBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public VariableWidthBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            return new VariableWidthBlockEncoding();
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, VariableWidthBlockEncoding blockEncoding)
        {
        }
    }
}
