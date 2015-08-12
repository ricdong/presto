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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.type.ArrayType.toStackRepresentation;
import static com.facebook.presto.type.TypeJsonUtils.canCastFromJson;
import static com.facebook.presto.type.TypeJsonUtils.stackRepresentationToObject;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class JsonToArrayCast
        extends ParametricOperator
{
    public static final JsonToArrayCast JSON_TO_ARRAY = new JsonToArrayCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonToArrayCast.class, "toArray", Type.class, ConnectorSession.class, Slice.class);

    private JsonToArrayCast()
    {
        super(OperatorType.CAST, ImmutableList.of(typeParameter("T")), "array<T>", ImmutableList.of(StandardTypes.JSON));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type type = types.get("T");
        Type arrayType = typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(type.getTypeSignature()), ImmutableList.of());
        checkCondition(canCastFromJson(arrayType), INVALID_CAST_ARGUMENT, "Cannot cast JSON to %s", arrayType);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(arrayType);
        return operatorInfo(OperatorType.CAST, arrayType.getTypeSignature(), ImmutableList.of(parameterizedTypeName(StandardTypes.JSON)), methodHandle, true, ImmutableList.of(false));
    }

    public static Block toArray(Type arrayType, ConnectorSession connectorSession, Slice json)
    {
        try {
            Object array = stackRepresentationToObject(connectorSession, json, arrayType);
            if (array == null) {
                return null;
            }
            return toStackRepresentation((List<?>) array, ((ArrayType) arrayType).getElementType());
        }
        catch (RuntimeException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to " + arrayType, e);
        }
    }
}
