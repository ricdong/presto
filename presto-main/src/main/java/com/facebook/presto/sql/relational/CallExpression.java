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
package com.facebook.presto.sql.relational;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public final class CallExpression
        extends RowExpression
{
    private final Signature signature;
    private final Type returnType;
    private final List<RowExpression> arguments;

    public CallExpression(Signature signature, Type returnType, List<RowExpression> arguments)
    {
        Preconditions.checkNotNull(signature, "signature is null");
        Preconditions.checkNotNull(arguments, "arguments is null");
        Preconditions.checkNotNull(returnType, "returnType is null");

        this.signature = signature;
        this.returnType = returnType;
        this.arguments = ImmutableList.copyOf(arguments);
    }

    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public Type getType()
    {
        return returnType;
    }

    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return signature.getName() + "(" + Joiner.on(", ").join(arguments) + ")";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signature, arguments);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CallExpression other = (CallExpression) obj;
        return Objects.equals(this.signature, other.signature) && Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public <C, R> R accept(RowExpressionVisitor<C, R> visitor, C context)
    {
        return visitor.visitCall(this, context);
    }
}
