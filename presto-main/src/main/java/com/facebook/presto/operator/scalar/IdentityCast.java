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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.google.common.base.Preconditions.checkArgument;

public class IdentityCast
        extends ParametricOperator
{
    public static final IdentityCast IDENTITY_CAST = new IdentityCast();

    protected IdentityCast()
    {
        super(OperatorType.CAST, ImmutableList.of(typeParameter("T")), "T", ImmutableList.of("T"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "Expected only one type");
        Type type = types.get("T");
        MethodHandle identity = MethodHandles.identity(type.getJavaType());
        return operatorInfo(OperatorType.CAST, type.getTypeSignature(), ImmutableList.of(type.getTypeSignature()), identity, false, ImmutableList.of(false));
    }
}
