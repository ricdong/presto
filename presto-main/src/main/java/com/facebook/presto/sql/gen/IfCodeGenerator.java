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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Preconditions;

import java.util.List;

import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFalse;

public class IfCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext context, Type returnType, List<RowExpression> arguments)
    {
        Preconditions.checkArgument(arguments.size() == 3);

        Variable wasNull = context.wasNull();
        Block condition = new Block()
                .append(context.generate(arguments.get(0)))
                .comment("... and condition value was not null")
                .append(wasNull)
                .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class)
                .invokeStatic(CompilerOperations.class, "and", boolean.class, boolean.class, boolean.class)
                .append(wasNull.set(constantFalse()));

        return new IfStatement()
                .condition(condition)
                .ifTrue(context.generate(arguments.get(1)))
                .ifFalse(context.generate(arguments.get(2)));
    }
}
