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
package com.facebook.presto.byteCode.expression;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.ParameterizedType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class InvokeByteCodeExpression
        extends ByteCodeExpression
{
    public static InvokeByteCodeExpression createInvoke(
            ByteCodeExpression instance,
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        return new InvokeByteCodeExpression(
                checkNotNull(instance, "instance is null"),
                instance.getType(),
                checkNotNull(methodName, "methodName is null"),
                checkNotNull(returnType, "returnType is null"),
                checkNotNull(parameterTypes, "parameterTypes is null"),
                checkNotNull(parameters, "parameters is null"));
    }

    @Nullable
    private final ByteCodeExpression instance;
    private final ParameterizedType methodTargetType;
    private final String methodName;
    private final ParameterizedType returnType;
    private final List<ByteCodeExpression> parameters;
    private final ImmutableList<ParameterizedType> parameterTypes;

    public InvokeByteCodeExpression(
            @Nullable ByteCodeExpression instance,
            ParameterizedType methodTargetType,
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        super(checkNotNull(returnType, "returnType is null"));
        checkArgument(instance == null || !instance.getType().isPrimitive(), "Type %s does not have methods", getType());
        this.instance = instance;
        this.methodTargetType = checkNotNull(methodTargetType, "methodTargetType is null");
        this.methodName = checkNotNull(methodName, "methodName is null");
        this.returnType = returnType;
        this.parameterTypes = ImmutableList.copyOf(checkNotNull(parameterTypes, "parameterTypes is null"));
        this.parameters = ImmutableList.copyOf(checkNotNull(parameters, "parameters is null"));
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        Block block = new Block();
        if (instance != null) {
            block.append(instance);
        }

        for (ByteCodeExpression parameter : parameters) {
            block.append(parameter);
        }

        if (instance == null) {
            return block.invokeStatic(methodTargetType, methodName, returnType, parameterTypes);
        }
        else if (instance.getType().isInterface()) {
            return block.invokeInterface(methodTargetType, methodName, returnType, parameterTypes);
        }
        else {
            return block.invokeVirtual(methodTargetType, methodName, returnType, parameterTypes);
        }
    }

    @Override
    protected String formatOneLine()
    {
        if (instance == null) {
            return methodTargetType.getSimpleName() + "." + methodName + "(" + Joiner.on(", ").join(parameters) + ")";
        }

        return instance + "." + methodName + "(" + Joiner.on(", ").join(parameters) + ")";
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        ImmutableList.Builder<ByteCodeNode> children = ImmutableList.builder();
        if (instance != null) {
            children.add(instance);
        }
        children.addAll(parameters);
        return children.build();
    }
}
