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
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.type.TypeUtils.castValue;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class ArrayRemoveFunction
        extends ParametricScalar
{
    public static final ArrayRemoveFunction ARRAY_REMOVE_FUNCTION = new ArrayRemoveFunction();
    private static final String FUNCTION_NAME = "array_remove";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(comparableTypeParameter("E")), "array<E>", ImmutableList.of("array<E>", "E"), false, false);

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(ArrayRemoveFunction.class, "remove", MethodHandle.class, Type.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(ArrayRemoveFunction.class, "remove", MethodHandle.class, Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(ArrayRemoveFunction.class, "remove", MethodHandle.class, Type.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(ArrayRemoveFunction.class, "remove", MethodHandle.class, Type.class, Block.class, Slice.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(ArrayRemoveFunction.class, "remove", MethodHandle.class, Type.class, Block.class, Object.class);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Remove specified values from the given array";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, format("%s expects only one argument", FUNCTION_NAME));
        Type type = types.get("E");
        TypeSignature valueType = type.getTypeSignature();
        TypeSignature arrayType = parameterizedTypeName(StandardTypes.ARRAY, valueType);

        MethodHandle equalsFunction = functionRegistry.resolveOperator(EQUAL, ImmutableList.of(type, type)).getMethodHandle();
        MethodHandle baseMethodHandle;
        if (type.getJavaType() == long.class) {
            baseMethodHandle = METHOD_HANDLE_LONG;
        }
        else if (type.getJavaType() == double.class) {
            baseMethodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (type.getJavaType() == Slice.class) {
            baseMethodHandle = METHOD_HANDLE_SLICE;
        }
        else if (type.getJavaType() == boolean.class) {
            baseMethodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else {
            baseMethodHandle = METHOD_HANDLE_OBJECT;
        }

        MethodHandle methodHandle = baseMethodHandle.bindTo(equalsFunction).bindTo(type);
        Signature signature = new Signature(FUNCTION_NAME, arrayType, arrayType, valueType);
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false, false));
    }

    public static Block remove(MethodHandle equalsFunction, Type type, Block array, Slice value)
    {
        return remove(equalsFunction, type, array, (Object) value);
    }

    public static Block remove(MethodHandle equalsFunction, Type type, Block array, long value)
    {
        return remove(equalsFunction, type, array, (Object) value);
    }

    public static Block remove(MethodHandle equalsFunction, Type type, Block array, double value)
    {
        return remove(equalsFunction, type, array, (Object) value);
    }

    public static Block remove(MethodHandle equalsFunction, Type type, Block array, boolean value)
    {
        return remove(equalsFunction, type, array, (Object) value);
    }

    public static Block remove(MethodHandle equalsFunction, Type type, Block array, Object value)
    {
        int sizeAfterRemove = 0;
        List<Integer> positions = new ArrayList<>();

        for (int i = 0; i < array.getPositionCount(); i++) {
            Object element = castValue(type, array, i);

            try {
                if (element == null || !(boolean) equalsFunction.invoke(element, value)) {
                    positions.add(i);
                    sizeAfterRemove += array.getLength(i);
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }

        if (array.getPositionCount() == positions.size()) {
            return array;
        }

        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), sizeAfterRemove);

        for (int position : positions) {
            type.appendTo(array, position, blockBuilder);
        }

        return blockBuilder.build();
    }
}
