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
package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockSerdeUtil;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.VarbinaryFunctions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.DateTimeUtils.parseDayTimeInterval;
import static com.facebook.presto.util.DateTimeUtils.parseTime;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteral;
import static com.facebook.presto.util.DateTimeUtils.parseYearMonthInterval;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class LiteralInterpreter
{
    private LiteralInterpreter() {}

    public static Object evaluate(Metadata metadata, ConnectorSession session, Expression node)
    {
        if (!(node instanceof Literal)) {
            throw new IllegalArgumentException("node must be a Literal");
        }
        return new LiteralVisitor(metadata).process(node, session);
    }

    public static List<Expression> toExpressions(List<?> objects, List<? extends Type> types)
    {
        checkNotNull(objects, "objects is null");
        checkNotNull(types, "types is null");
        checkArgument(objects.size() == types.size(), "objects and types do not have the same size");

        ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
        for (int i = 0; i < objects.size(); i++) {
            Object object = objects.get(i);
            Type type = types.get(i);
            expressions.add(toExpression(object, type));
        }
        return expressions.build();
    }

    public static Expression toExpression(Object object, Type type)
    {
        if (object instanceof Expression) {
            return (Expression) object;
        }

        if (object == null) {
            if (type.equals(UNKNOWN)) {
                return new NullLiteral();
            }
            return new Cast(new NullLiteral(), type.getTypeSignature().toString());
        }

        checkArgument(Primitives.wrap(type.getJavaType()).isInstance(object), "object.getClass (%s) and type.getJavaType (%s) do not agree", object.getClass(), type.getJavaType());

        if (type.equals(BIGINT)) {
            return new LongLiteral(object.toString());
        }

        if (type.equals(DOUBLE)) {
            Double value = (Double) object;
            // WARNING: the ORC predicate code depends on NaN and infinity not appearing in a tuple domain, so
            // if you remove this, you will need to update the TupleDomainOrcPredicate
            if (value.isNaN()) {
                return new FunctionCall(new QualifiedName("nan"), ImmutableList.<Expression>of());
            }
            else if (value.equals(Double.NEGATIVE_INFINITY)) {
                return ArithmeticUnaryExpression.negative(new FunctionCall(new QualifiedName("infinity"), ImmutableList.<Expression>of()));
            }
            else if (value.equals(Double.POSITIVE_INFINITY)) {
                return new FunctionCall(new QualifiedName("infinity"), ImmutableList.<Expression>of());
            }
            else {
                return new DoubleLiteral(object.toString());
            }
        }

        if (type.equals(VARCHAR)) {
            if (object instanceof Slice) {
                return new StringLiteral(((Slice) object).toString(UTF_8));
            }

            if (object instanceof String) {
                return new StringLiteral((String) object);
            }

            throw new IllegalArgumentException("object must be instance of Slice or String when type is VARCHAR");
        }

        if (type.equals(BOOLEAN)) {
            return new BooleanLiteral(object.toString());
        }

        if (object instanceof Block) {
            SliceOutput output = new DynamicSliceOutput(((Block) object).getSizeInBytes());
            BlockSerdeUtil.writeBlock(output, (Block) object);
            object = output.slice();
            // This if condition will evaluate to true: object instanceof Slice && !type.equals(VARCHAR)
        }

        if (object instanceof Slice && !type.equals(VARCHAR)) {
            // HACK: we need to serialize VARBINARY in a format that can be embedded in an expression to be
            // able to encode it in the plan that gets sent to workers.
            // We do this by transforming the in-memory varbinary into a call to from_base64(<base64-encoded value>)
            FunctionCall fromBase64 = new FunctionCall(new QualifiedName("from_base64"), ImmutableList.of(new StringLiteral(VarbinaryFunctions.toBase64((Slice) object).toStringUtf8())));
            Signature signature = FunctionRegistry.getMagicLiteralFunctionSignature(type);
            return new FunctionCall(new QualifiedName(signature.getName()), ImmutableList.of(fromBase64));
        }

        Signature signature = FunctionRegistry.getMagicLiteralFunctionSignature(type);
        Expression rawLiteral = toExpression(object, FunctionRegistry.typeForMagicLiteral(type));

        return new FunctionCall(new QualifiedName(signature.getName()), ImmutableList.of(rawLiteral));
    }

    private static class LiteralVisitor
            extends AstVisitor<Object, ConnectorSession>
    {
        private final Metadata metadata;

        private LiteralVisitor(Metadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        protected Object visitLiteral(Literal node, ConnectorSession session)
        {
            throw new UnsupportedOperationException("Unhandled literal type: " + node);
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Long visitLongLiteral(LongLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Double visitDoubleLiteral(DoubleLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Slice visitStringLiteral(StringLiteral node, ConnectorSession session)
        {
            return node.getSlice();
        }

        @Override
        protected Object visitGenericLiteral(GenericLiteral node, ConnectorSession session)
        {
            Type type = metadata.getType(parseTypeSignature(node.getType()));
            if (type == null) {
                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
            }

            FunctionInfo operator;
            try {
                operator = metadata.getFunctionRegistry().getCoercion(VARCHAR, type);
            }
            catch (IllegalArgumentException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "No literal form for type %s", type);
            }
            try {
                return ExpressionInterpreter.invoke(session, operator.getMethodHandle(), ImmutableList.<Object>of(utf8Slice(node.getValue())));
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
        }

        @Override
        protected Long visitTimeLiteral(TimeLiteral node, ConnectorSession session)
        {
            return parseTime(session.getTimeZoneKey(), node.getValue());
        }

        @Override
        protected Long visitTimestampLiteral(TimestampLiteral node, ConnectorSession session)
        {
            try {
                return parseTimestampLiteral(session.getTimeZoneKey(), node.getValue());
            }
            catch (Exception e) {
                throw new SemanticException(INVALID_LITERAL, node, "'%s' is not a valid timestamp literal", node.getValue());
            }
        }

        @Override
        protected Long visitIntervalLiteral(IntervalLiteral node, ConnectorSession session)
        {
            if (node.isYearToMonth()) {
                return node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            else {
                return node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
            }

        }

        @Override
        protected Object visitNullLiteral(NullLiteral node, ConnectorSession session)
        {
            return null;
        }
    }
}
