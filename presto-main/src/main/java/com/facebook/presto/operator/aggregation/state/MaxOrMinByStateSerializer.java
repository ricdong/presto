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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class MaxOrMinByStateSerializer
        implements AccumulatorStateSerializer<MaxOrMinByState>
{
    private final Type valueType;
    private final Type keyType;

    public MaxOrMinByStateSerializer(Type valueType, Type keyType)
    {
        this.valueType = valueType;
        this.keyType = keyType;
    }

    @Override
    public Type getSerializedType()
    {
        return VARCHAR;
    }

    @Override
    public void serialize(MaxOrMinByState state, BlockBuilder out)
    {
        SliceOutput sliceOutput = new DynamicSliceOutput((int) state.getEstimatedSize());

        int keyLength = 0;
        if (state.getKey() != null && !state.getKey().isNull(0)) {
            keyLength = state.getKey().getLength(0);
        }
        sliceOutput.writeInt(keyLength);

        int valueLength = 0;
        if (state.getValue() != null && !state.getValue().isNull(0)) {
            valueLength = state.getValue().getLength(0);
        }
        sliceOutput.writeInt(valueLength);

        if (state.getKey() != null && !state.getKey().isNull(0)) {
            appendTo(keyType, sliceOutput, state.getKey());
        }
        if (state.getValue() != null && !state.getValue().isNull(0)) {
            appendTo(valueType, sliceOutput, state.getValue());
        }
        Slice slice = sliceOutput.slice();
        out.writeBytes(slice, 0, slice.length());
        out.closeEntry();
    }

    private static void appendTo(Type type, SliceOutput output, Block block)
    {
        if (type.getJavaType() == long.class) {
            output.appendLong(type.getLong(block, 0));
        }
        else if (type.getJavaType() == double.class) {
            output.appendDouble(type.getDouble(block, 0));
        }
        else if (type.getJavaType() == Slice.class) {
            output.appendBytes(type.getSlice(block, 0));
        }
        else if (type.getJavaType() == boolean.class) {
            output.appendByte(type.getBoolean(block, 0) ? 1 : 0);
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + type.getJavaType().getSimpleName());
        }
    }

    @Override
    public void deserialize(Block block, int index, MaxOrMinByState state)
    {
        SliceInput input = block.getSlice(index, 0, block.getLength(index)).getInput();

        int keyLength = input.readInt();
        int valueLength = input.readInt();
        state.setKey(null);
        state.setValue(null);
        if (keyLength > 0) {
            state.setKey(toBlock(keyType, input, keyLength));
        }
        if (valueLength > 0) {
            state.setValue(toBlock(valueType, input, valueLength));
        }
    }

    private static Block toBlock(Type type, SliceInput input, int length)
    {
        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), 1, length);
        if (type.getJavaType() == long.class) {
            type.writeLong(builder, input.readLong());
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(builder, input.readDouble());
        }
        else if (type.getJavaType() == Slice.class) {
            type.writeSlice(builder, input.readSlice(length));
        }
        else if (type.getJavaType() == boolean.class) {
            type.writeBoolean(builder, input.readByte() != 0);
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + type.getJavaType().getSimpleName());
        }

        return builder.build();
    }
}
