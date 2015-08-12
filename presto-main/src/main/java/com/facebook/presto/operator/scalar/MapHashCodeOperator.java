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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MapHashCodeOperator
        extends ParametricOperator
{
    public static final MapHashCodeOperator MAP_HASH_CODE = new MapHashCodeOperator();
    private static final TypeSignature RETURN_TYPE = parseTypeSignature(StandardTypes.BIGINT);

    private static final MethodHandle METHOD_HANDLE = methodHandle(MapHashCodeOperator.class, "hash", MethodHandle.class, MethodHandle.class, Type.class, Type.class, Block.class);

    private MapHashCodeOperator()
    {
        super(HASH_CODE, ImmutableList.of(comparableTypeParameter("K"), comparableTypeParameter("V")), StandardTypes.BIGINT, ImmutableList.of("map<K,V>"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");

        Type type = typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()), ImmutableList.of());
        TypeSignature typeSignature = type.getTypeSignature();

        MethodHandle keyHashCodeFunction = functionRegistry.resolveOperator(HASH_CODE, ImmutableList.of(keyType)).getMethodHandle();
        MethodHandle valueHashCodeFunction = functionRegistry.resolveOperator(HASH_CODE, ImmutableList.of(valueType)).getMethodHandle();

        MethodHandle method = METHOD_HANDLE.bindTo(keyHashCodeFunction).bindTo(valueHashCodeFunction).bindTo(keyType).bindTo(valueType);
        return operatorInfo(HASH_CODE, RETURN_TYPE, ImmutableList.of(typeSignature), method, false, ImmutableList.of(false));
    }

    public static long hash(MethodHandle keyHashCodeFunction, MethodHandle valueHashCodeFunction, Type keyType, Type valueType, Block block)
    {
        long result = 0;
        for (int position = 0; position < block.getPositionCount(); position += 2) {
            result += hashPosition(keyHashCodeFunction, keyType, block, position);
            result += hashPosition(valueHashCodeFunction, valueType, block, position + 1);
        }
        return result;
    }
}
