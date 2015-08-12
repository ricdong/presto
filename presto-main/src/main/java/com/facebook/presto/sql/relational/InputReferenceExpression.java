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

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;

public final class InputReferenceExpression
        extends RowExpression
{
    private final int field;
    private final Type type;

    public InputReferenceExpression(int field, Type type)
    {
        Preconditions.checkNotNull(type, "type is null");

        this.field = field;
        this.type = type;
    }

    @JsonProperty
    public int getField()
    {
        return field;
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(field, type);
    }

    @Override
    public String toString()
    {
        return "#" + field;
    }

    @Override
    public <C, R> R accept(RowExpressionVisitor<C, R> visitor, C context)
    {
        return visitor.visitInputReference(this, context);
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
        InputReferenceExpression other = (InputReferenceExpression) obj;
        return Objects.equals(this.field, other.field) && Objects.equals(this.type, other.type);
    }
}
