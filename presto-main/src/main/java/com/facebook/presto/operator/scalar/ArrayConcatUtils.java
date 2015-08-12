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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

public final class ArrayConcatUtils
{
    private ArrayConcatUtils() {}

    public static Block concat(Type elementType, Block leftBlock, Block rightBlock)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), leftBlock.getSizeInBytes() + rightBlock.getSizeInBytes());
        for (int i = 0; i < leftBlock.getPositionCount(); i++) {
            elementType.appendTo(leftBlock, i, blockBuilder);
        }
        for (int i = 0; i < rightBlock.getPositionCount(); i++) {
            elementType.appendTo(rightBlock, i, blockBuilder);
        }
        return blockBuilder.build();
    }

    // Usage of appendElement: ArrayToElementConcatFunction
    public static Block appendElement(Type elementType, Block block, long value)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        elementType.writeLong(blockBuilder, value);

        return blockBuilder.build();
    }

    public static Block appendElement(Type elementType, Block block, boolean value)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        elementType.writeBoolean(blockBuilder, value);

        return blockBuilder.build();
    }

    public static Block appendElement(Type elementType, Block block, double value)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        elementType.writeDouble(blockBuilder, value);

        return blockBuilder.build();
    }

    public static Block appendElement(Type elementType, Block block, Slice value)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        elementType.writeSlice(blockBuilder, value);

        return blockBuilder.build();
    }

    public static Block appendElement(Type elementType, Block block, Object value)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        elementType.writeObject(blockBuilder, value);

        return blockBuilder.build();
    }

    // Usage of prependElement: ElementToArrayConcatFunction
    public static Block prependElement(Type elementType, Slice value, Block block)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);

        elementType.writeSlice(blockBuilder, value);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        return blockBuilder.build();
    }

    public static Block prependElement(Type elementType, Object value, Block block)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);

        elementType.writeObject(blockBuilder, value);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        return blockBuilder.build();
    }

    public static Block prependElement(Type elementType, long value, Block block)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);

        elementType.writeLong(blockBuilder, value);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        return blockBuilder.build();
    }

    public static Block prependElement(Type elementType, boolean value, Block block)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);

        elementType.writeBoolean(blockBuilder, value);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        return blockBuilder.build();
    }

    public static Block prependElement(Type elementType, double value, Block block)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);

        elementType.writeDouble(blockBuilder, value);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        return blockBuilder.build();
    }
}
