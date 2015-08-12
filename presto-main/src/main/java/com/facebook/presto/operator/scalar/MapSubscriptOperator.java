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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.SUBSCRIPT;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.type.TypeUtils.castValue;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MapSubscriptOperator
        extends ParametricOperator
{
    public static final MapSubscriptOperator MAP_SUBSCRIPT = new MapSubscriptOperator();

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(MapSubscriptOperator.class, "subscript", MethodHandle.class, Type.class, Type.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(MapSubscriptOperator.class, "subscript", MethodHandle.class, Type.class, Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(MapSubscriptOperator.class, "subscript", MethodHandle.class, Type.class, Type.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(MapSubscriptOperator.class, "subscript", MethodHandle.class, Type.class, Type.class, Block.class, Slice.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(MapSubscriptOperator.class, "subscript", MethodHandle.class, Type.class, Type.class, Block.class, Object.class);

    protected MapSubscriptOperator()
    {
        super(SUBSCRIPT, ImmutableList.of(typeParameter("K"), typeParameter("V")), "V", ImmutableList.of("map<K,V>", "K"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");

        MethodHandle keyEqualsMethod = functionRegistry.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType)).getMethodHandle();

        MethodHandle methodHandle;
        if (keyType.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (keyType.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (keyType.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (keyType.getJavaType() == Slice.class) {
            methodHandle = METHOD_HANDLE_SLICE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT;
        }
        methodHandle = methodHandle.bindTo(keyEqualsMethod).bindTo(keyType).bindTo(valueType);

        // this casting is necessary because otherwise presto byte code generator will generate illegal byte code
        if (valueType.getJavaType() == void.class) {
            methodHandle = methodHandle.asType(methodHandle.type().changeReturnType(void.class));
        }
        else {
            methodHandle = methodHandle.asType(methodHandle.type().changeReturnType(Primitives.wrap(valueType.getJavaType())));
        }

        Signature signature = new Signature(SUBSCRIPT.name(), valueType.getTypeSignature(), parameterizedTypeName("map", keyType.getTypeSignature(), valueType.getTypeSignature()), keyType.getTypeSignature());
        return new FunctionInfo(signature, "Map subscript", true, methodHandle, true, true, ImmutableList.of(false, false));
    }

    public static Object subscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Block map, boolean key)
    {
        for (int position = 0; position < map.getPositionCount(); position += 2) {
            try {
                if ((boolean) keyEqualsMethod.invokeExact(keyType.getBoolean(map, position), key)) {
                    return castValue(valueType, map, position + 1); // position + 1: value position
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        return null;
    }

    public static Object subscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Block map, long key)
    {
        for (int position = 0; position < map.getPositionCount(); position += 2) {
            try {
                if ((boolean) keyEqualsMethod.invokeExact(keyType.getLong(map, position), key)) {
                    return castValue(valueType, map, position + 1); // position + 1: value position
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        return null;
    }

    public static Object subscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Block map, double key)
    {
        for (int position = 0; position < map.getPositionCount(); position += 2) {
            try {
                if ((boolean) keyEqualsMethod.invokeExact(keyType.getDouble(map, position), key)) {
                    return castValue(valueType, map, position + 1); // position + 1: value position
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        return null;
    }

    public static Object subscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Block map, Slice key)
    {
        for (int position = 0; position < map.getPositionCount(); position += 2) {
            try {
                if ((boolean) keyEqualsMethod.invokeExact(keyType.getSlice(map, position), key)) {
                    return castValue(valueType, map, position + 1); // position + 1: value position
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        return null;
    }

    public static Object subscript(MethodHandle keyEqualsMethod, Type keyType, Type valueType, Block map, Object key)
    {
        for (int position = 0; position < map.getPositionCount(); position += 2) {
            try {
                if ((boolean) keyEqualsMethod.invokeExact(keyType.getObject(map, position), key)) {
                    return castValue(valueType, map, position + 1); // position + 1: value position
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        return null;
    }
}
