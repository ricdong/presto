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

import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.type.LikePatternType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.metadata.OperatorType.SUBSCRIPT;
import static com.facebook.presto.metadata.Signature.internalFunction;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.tree.ArrayConstructor.ARRAY_CONSTRUCTOR;

public final class Signatures
{
    public static final String IF = "IF";
    public static final String NULL_IF = "NULL_IF";
    public static final String SWITCH = "SWITCH";
    public static final String CAST = mangleOperatorName("CAST");
    public static final String TRY_CAST = "TRY_CAST";
    public static final String IS_NULL = "IS_NULL";
    public static final String COALESCE = "COALESCE";
    public static final String IN = "IN";

    private Signatures()
    {
    }

    // **************** sql operators ****************
    public static Signature notSignature()
    {
        return new Signature("not", StandardTypes.BOOLEAN, ImmutableList.of(StandardTypes.BOOLEAN));
    }

    public static Signature betweenSignature(Type valueType, Type minType, Type maxType)
    {
        return internalOperator("BETWEEN", parseTypeSignature(StandardTypes.BOOLEAN), valueType.getTypeSignature(), minType.getTypeSignature(), maxType.getTypeSignature());
    }

    public static Signature likeSignature()
    {
        return internalFunction("LIKE", StandardTypes.BOOLEAN, StandardTypes.VARCHAR, LikePatternType.NAME);
    }

    public static Signature likePatternSignature()
    {
        return internalFunction("LIKE_PATTERN", LikePatternType.NAME, StandardTypes.VARCHAR, StandardTypes.VARCHAR);
    }

    public static Signature castSignature(Type returnType, Type valueType)
    {
        // Name has already been mangled, so don't use internalOperator
        return internalFunction(CAST, returnType.getTypeSignature(), valueType.getTypeSignature());
    }

    public static Signature tryCastSignature(Type returnType, Type valueType)
    {
        return internalFunction(TRY_CAST, returnType.getTypeSignature(), valueType.getTypeSignature());
    }

    public static Signature logicalExpressionSignature(LogicalBinaryExpression.Type expressionType)
    {
        return internalFunction(expressionType.name(), StandardTypes.BOOLEAN, StandardTypes.BOOLEAN, StandardTypes.BOOLEAN);
    }

    public static Signature arithmeticNegationSignature(Type returnType, Type valueType)
    {
        return internalOperator("NEGATION", returnType.getTypeSignature(), valueType.getTypeSignature());
    }

    public static Signature arithmeticExpressionSignature(ArithmeticBinaryExpression.Type expressionType, Type returnType, Type leftType, Type rightType)
    {
        return internalOperator(expressionType.name(), returnType.getTypeSignature(), leftType.getTypeSignature(), rightType.getTypeSignature());
    }

    public static Signature subscriptSignature(Type returnType, Type leftType, Type rightType)
    {
        return internalOperator(SUBSCRIPT.name(), returnType.getTypeSignature(), leftType.getTypeSignature(), rightType.getTypeSignature());
    }

    public static Signature arrayConstructorSignature(Type returnType, List<? extends Type> argumentTypes)
    {
        return internalFunction(ARRAY_CONSTRUCTOR, returnType.getTypeSignature(), Lists.transform(argumentTypes, Type::getTypeSignature));
    }

    public static Signature arrayConstructorSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalFunction(ARRAY_CONSTRUCTOR, returnType, argumentTypes);
    }

    public static Signature comparisonExpressionSignature(ComparisonExpression.Type expressionType, Type leftType, Type rightType)
    {
        for (OperatorType operatorType : OperatorType.values()) {
            if (operatorType.name().equals(expressionType.name())) {
                return internalOperator(expressionType.name(), parseTypeSignature(StandardTypes.BOOLEAN), leftType.getTypeSignature(), rightType.getTypeSignature());
            }
        }
        return internalFunction(expressionType.name(), parseTypeSignature(StandardTypes.BOOLEAN), leftType.getTypeSignature(), rightType.getTypeSignature());
    }

    // **************** special forms (lazy evaluation, etc) ****************
    public static Signature ifSignature(Type returnType)
    {
        return new Signature(IF, returnType.getTypeSignature());
    }

    public static Signature nullIfSignature(Type returnType, Type firstType, Type secondType)
    {
        return new Signature(NULL_IF, returnType.getTypeSignature(), firstType.getTypeSignature(), secondType.getTypeSignature());
    }

    public static Signature switchSignature(Type returnType)
    {
        return new Signature(SWITCH, returnType.getTypeSignature());
    }

    public static Signature whenSignature(Type returnType)
    {
        return new Signature("WHEN", returnType.getTypeSignature());
    }

    // **************** functions that require varargs and/or complex types (e.g., lists) ****************
    public static Signature inSignature()
    {
        return internalFunction(IN, StandardTypes.BOOLEAN);
    }

    // **************** functions that need to do special null handling ****************
    public static Signature isNullSignature(Type argumentType)
    {
        return internalFunction(IS_NULL, parseTypeSignature(StandardTypes.BOOLEAN), argumentType.getTypeSignature());
    }

    public static Signature coalesceSignature(Type returnType, List<Type> argumentTypes)
    {
        return internalFunction(COALESCE, returnType.getTypeSignature(), Lists.transform(argumentTypes, Type::getTypeSignature));
    }
}
