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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Preconditions;

import java.util.Optional;

/**
 * Represents an expression or a direct field reference. The latter is used, for
 * instance, when expanding "*" in SELECT * FROM ....
 */
public class FieldOrExpression
{
    // reference to field in underlying relation
    private final Optional<Integer> fieldIndex;
    private final Optional<Expression> expression;

    public FieldOrExpression(int fieldIndex)
    {
        this.fieldIndex = Optional.of(fieldIndex);
        this.expression = Optional.empty();
    }

    public FieldOrExpression(Expression expression)
    {
        Preconditions.checkNotNull(expression, "expression is null");

        this.fieldIndex = Optional.empty();
        this.expression = Optional.of(expression);
    }

    public boolean isFieldReference()
    {
        return fieldIndex.isPresent();
    }

    public int getFieldIndex()
    {
        Preconditions.checkState(isFieldReference(), "Not a field reference");
        return fieldIndex.get();
    }

    public boolean isExpression()
    {
        return expression.isPresent();
    }

    public Expression getExpression()
    {
        Preconditions.checkState(isExpression(), "Not an expression");
        return expression.get();
    }

    @Override
    public String toString()
    {
        if (fieldIndex.isPresent()) {
            return fieldIndex.get().toString();
        }

        return expression.get().toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldOrExpression that = (FieldOrExpression) o;

        if (!expression.equals(that.expression)) {
            return false;
        }
        if (!fieldIndex.equals(that.fieldIndex)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = fieldIndex.hashCode();
        result = 31 * result + expression.hashCode();
        return result;
    }
}
