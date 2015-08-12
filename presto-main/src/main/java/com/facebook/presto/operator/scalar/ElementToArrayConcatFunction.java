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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ElementToArrayConcatFunction
        extends ParametricScalar
{
    public static final ElementToArrayConcatFunction ELEMENT_TO_ARRAY_CONCAT_FUNCTION = new ElementToArrayConcatFunction();
    private static final String FUNCTION_NAME = "concat";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(typeParameter("E")), "array<E>", ImmutableList.of("E", "array<E>"), false, false);

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(ArrayConcatUtils.class, "prependElement", Type.class, boolean.class, Block.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(ArrayConcatUtils.class, "prependElement", Type.class, long.class, Block.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(ArrayConcatUtils.class, "prependElement", Type.class, double.class, Block.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(ArrayConcatUtils.class, "prependElement", Type.class, Slice.class, Block.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(ArrayConcatUtils.class, "prependElement", Type.class, Object.class, Block.class);

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
        return "Concatenates an element to an array";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("E");
        MethodHandle methodHandle;
        if (type.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (type.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (type.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (type.getJavaType() == Slice.class) {
            methodHandle = METHOD_HANDLE_SLICE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT;
        }
        methodHandle = methodHandle.bindTo(type);

        TypeSignature typeSignature = type.getTypeSignature();
        TypeSignature returnType = parameterizedTypeName("array", typeSignature);
        Signature signature = new Signature(FUNCTION_NAME, returnType, typeSignature, returnType);
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false, false));
    }
}
