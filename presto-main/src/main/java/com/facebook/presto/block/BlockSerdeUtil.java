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
package com.facebook.presto.block;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.util.Reflection.methodHandle;

public final class BlockSerdeUtil
{
    public static final MethodHandle READ_BLOCK = methodHandle(BlockSerdeUtil.class, "readBlock", BlockEncodingSerde.class, Slice.class);

    private BlockSerdeUtil()
    {
    }

    public static Block readBlock(BlockEncodingSerde blockEncodingSerde, Slice slice)
    {
        SliceInput input = slice.getInput();
        BlockEncoding blockEncoding = blockEncodingSerde.readBlockEncoding(input);
        return blockEncoding.readBlock(input);
    }

    // This class is only used in LiteralInterpreter for magic literal. Most likely, you shouldn't use it from anywhere else.
    public static void writeBlock(SliceOutput output, Block block)
    {
        BlockEncoding encoding = block.getEncoding();
        BlockEncodingManager.writeBlockEncodingInternal(output, encoding);
        encoding.writeBlock(output, block);
    }
}
