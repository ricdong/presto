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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;

public class CountConstantOptimizer
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private static class Rewriter
            extends PlanRewriter<Void>
    {
        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            Map<Symbol, FunctionCall> aggregations = new LinkedHashMap<>(node.getAggregations());
            Map<Symbol, Signature> functions = new LinkedHashMap<>(node.getFunctions());

            PlanNode source = context.rewrite(node.getSource());
            if (source instanceof ProjectNode) {
                ProjectNode projectNode = (ProjectNode) source;
                for (Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                    Symbol symbol = entry.getKey();
                    FunctionCall functionCall = entry.getValue();
                    Signature signature = node.getFunctions().get(symbol);
                    if (isCountConstant(projectNode, functionCall, signature)) {
                        aggregations.put(symbol, new FunctionCall(functionCall.getName(), null, functionCall.isDistinct(), ImmutableList.<Expression>of()));
                        functions.put(symbol, new Signature("count", StandardTypes.BIGINT));
                    }
                }
            }

            return new AggregationNode(
                    node.getId(),
                    source,
                    node.getGroupBy(),
                    aggregations,
                    functions,
                    node.getMasks(),
                    node.getStep(),
                    node.getSampleWeight(),
                    node.getConfidence(),
                    node.getHashSymbol());
        }

        public static boolean isCountConstant(ProjectNode projectNode, FunctionCall functionCall, Signature signature)
        {
            if (!"count".equals(signature.getName()) ||
                    signature.getArgumentTypes().size() != 1 ||
                    !signature.getReturnType().equals(StandardTypes.BIGINT)) {
                return false;
            }

            Expression argument = functionCall.getArguments().get(0);
            if (argument instanceof Literal) {
                return true;
            }

            if (argument instanceof QualifiedNameReference) {
                QualifiedNameReference qualifiedNameReference = (QualifiedNameReference) argument;
                QualifiedName qualifiedName = qualifiedNameReference.getName();
                Symbol argumentSymbol = Symbol.fromQualifiedName(qualifiedName);
                Expression argumentExpression = projectNode.getAssignments().get(argumentSymbol);
                return (argumentExpression instanceof Literal) && (!(argumentExpression instanceof NullLiteral));
            }

            return false;
        }
    }
}
