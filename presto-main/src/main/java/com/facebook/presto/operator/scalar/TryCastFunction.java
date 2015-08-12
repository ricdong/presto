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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.internalFunction;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static java.lang.invoke.MethodHandles.catchException;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodType.methodType;

public class TryCastFunction
        extends ParametricScalar
{
    public static final TryCastFunction TRY_CAST = new TryCastFunction();

    private static final Signature SIGNATURE = new Signature("TRY_CAST", ImmutableList.of(typeParameter("F"), typeParameter("T")), "T", ImmutableList.of("F"), false, false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type fromType = types.get("F");
        Type toType = types.get("T");

        Class<?> returnType = Primitives.wrap(toType.getJavaType());
        MethodHandle tryCastHandle;

        if (fromType.equals(UNKNOWN)) {
            tryCastHandle = dropArguments(constant(returnType, null), 0, Void.class);
        }
        else {
            // the resulting method needs to return a boxed type
            MethodHandle coercion = functionRegistry.getCoercion(fromType, toType).getMethodHandle();
            coercion = coercion.asType(methodType(returnType, coercion.type()));

            MethodHandle exceptionHandler = dropArguments(constant(returnType, null), 0, RuntimeException.class);
            tryCastHandle = catchException(coercion, RuntimeException.class, exceptionHandler);
        }

        return new FunctionInfo(
                internalFunction(SIGNATURE.getName(), toType.getTypeSignature(), ImmutableList.of(fromType.getTypeSignature())),
                getDescription(),
                isHidden(),
                tryCastHandle,
                isDeterministic(),
                true,
                ImmutableList.of(true));
    }
}
