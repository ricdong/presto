package com.facebook.presto.operator.scalar;
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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;

public class RowHashCodeOperator
        extends ParametricOperator
{
    public static final RowHashCodeOperator ROW_HASH_CODE = new RowHashCodeOperator();
    private static final TypeSignature RETURN_TYPE = parseTypeSignature(StandardTypes.BIGINT);
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowHashCodeOperator.class, "hash", Type.class, Block.class);

    private RowHashCodeOperator()
    {
        super(HASH_CODE, ImmutableList.of(comparableWithVariadicBound("T", "row")), StandardTypes.BIGINT, ImmutableList.of("T"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("T");
        TypeSignature typeSignature = type.getTypeSignature();
        return operatorInfo(HASH_CODE, RETURN_TYPE, ImmutableList.of(typeSignature), METHOD_HANDLE.bindTo(type), false, ImmutableList.of(false));
    }

    public static long hash(Type rowType, Block block)
    {
        BlockBuilder blockBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), 1);
        blockBuilder.writeObject(block).closeEntry();
        return rowType.hash(blockBuilder.build(), 0);
    }
}
