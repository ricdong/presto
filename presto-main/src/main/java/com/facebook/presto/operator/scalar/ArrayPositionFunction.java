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
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ArrayPositionFunction
        extends ParametricScalar
{
    public static final ArrayPositionFunction ARRAY_POSITION = new ArrayPositionFunction();
    private static final Signature SIGNATURE = new Signature("array_position", ImmutableList.of(comparableTypeParameter("E")), "bigint", ImmutableList.of("array<E>", "E"), false, false);
    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(ArrayPositionFunction.class, "arrayPosition", Type.class, MethodHandle.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(ArrayPositionFunction.class, "arrayPosition", Type.class, MethodHandle.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(ArrayPositionFunction.class, "arrayPosition", Type.class, MethodHandle.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(ArrayPositionFunction.class, "arrayPosition", Type.class, MethodHandle.class, Block.class, Slice.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(ArrayPositionFunction.class, "arrayPosition", Type.class, MethodHandle.class, Block.class, Object.class);

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
        return "Returns the position of the first occurrence of the given value in array (or 0 if not found)";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("E");
        MethodHandle equalMethodHandle = functionRegistry.resolveOperator(OperatorType.EQUAL, ImmutableList.of(type, type)).getMethodHandle();
        MethodHandle arrayPositionMethodHandle;
        if (type.getJavaType() == boolean.class) {
            arrayPositionMethodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (type.getJavaType() == long.class) {
            arrayPositionMethodHandle = METHOD_HANDLE_LONG;
        }
        else if (type.getJavaType() == double.class) {
            arrayPositionMethodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (type.getJavaType() == Slice.class) {
            arrayPositionMethodHandle = METHOD_HANDLE_SLICE;
        }
        else {
            arrayPositionMethodHandle = METHOD_HANDLE_OBJECT;
        }
        return new FunctionInfo(
                new Signature("array_position", parseTypeSignature(StandardTypes.BIGINT), parameterizedTypeName("array", type.getTypeSignature()), type.getTypeSignature()),
                getDescription(),
                isHidden(),
                arrayPositionMethodHandle.bindTo(type).bindTo(equalMethodHandle),
                isDeterministic(),
                false,
                ImmutableList.of(false, false));
    }

    public static long arrayPosition(Type type, MethodHandle equalMethodHandle, Block array, boolean element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                boolean arrayValue = type.getBoolean(array, i);
                try {
                    if ((boolean) equalMethodHandle.invokeExact(arrayValue, element)) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(INTERNAL_ERROR, t);
                }
            }
        }
        return 0;
    }

    public static long arrayPosition(Type type, MethodHandle equalMethodHandle, Block array, long element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                long arrayValue = type.getLong(array, i);
                try {
                    if ((boolean) equalMethodHandle.invokeExact(arrayValue, element)) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(INTERNAL_ERROR, t);
                }
            }
        }
        return 0;
    }

    public static long arrayPosition(Type type, MethodHandle equalMethodHandle, Block array, double element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                double arrayValue = type.getDouble(array, i);
                try {
                    if ((boolean) equalMethodHandle.invokeExact(arrayValue, element)) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(INTERNAL_ERROR, t);
                }
            }
        }
        return 0;
    }

    public static long arrayPosition(Type type, MethodHandle equalMethodHandle, Block array, Slice element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                Slice arrayValue = type.getSlice(array, i);
                try {
                    if ((boolean) equalMethodHandle.invokeExact(arrayValue, element)) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(INTERNAL_ERROR, t);
                }
            }
        }
        return 0;
    }

    public static long arrayPosition(Type type, MethodHandle equalMethodHandle, Block array, Object element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                Object arrayValue = type.getObject(array, i);
                try {
                    if ((boolean) equalMethodHandle.invoke(arrayValue, element)) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(INTERNAL_ERROR, t);
                }
            }
        }
        return 0;
    }
}
