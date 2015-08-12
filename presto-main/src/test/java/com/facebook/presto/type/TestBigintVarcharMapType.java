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
package com.facebook.presto.type;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.MapType.toStackRepresentation;

public class TestBigintVarcharMapType
        extends AbstractTestType
{
    public TestBigintVarcharMapType()
    {
        super(new TypeRegistry().getType(parseTypeSignature("map<bigint,varchar>")), Map.class, createTestBlock(new TypeRegistry().getType(parseTypeSignature("map<bigint,varchar>"))));
    }

    public static Block createTestBlock(Type mapType)
    {
        BlockBuilder blockBuilder = mapType.createBlockBuilder(new BlockBuilderStatus(), 2);
        mapType.writeObject(blockBuilder, toStackRepresentation(ImmutableMap.of(1, "hi"), BIGINT, VARCHAR));
        mapType.writeObject(blockBuilder, toStackRepresentation(ImmutableMap.of(1, "2", 2, "hello"), BIGINT, VARCHAR));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        throw new UnsupportedOperationException();
    }
}
