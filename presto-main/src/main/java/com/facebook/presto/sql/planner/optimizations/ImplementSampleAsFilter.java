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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class ImplementSampleAsFilter
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

        return PlanRewriter.rewriteWith(new Rewriter(), plan, null);
    }

    private static class Rewriter
            extends PlanRewriter<Void>
    {
        @Override
        public PlanNode visitSample(SampleNode node, RewriteContext<Void> context)
        {
            if (node.getSampleType() == SampleNode.Type.BERNOULLI) {
                PlanNode rewrittenSource = context.rewrite(node.getSource());

                ComparisonExpression expression = new ComparisonExpression(
                        ComparisonExpression.Type.LESS_THAN,
                        new FunctionCall(QualifiedName.of("rand"), ImmutableList.<Expression>of()),
                        new DoubleLiteral(Double.toString(node.getSampleRatio())));
                return new FilterNode(node.getId(), rewrittenSource, expression);
            }
            else if (node.getSampleType() == SampleNode.Type.POISSONIZED ||
                    node.getSampleType() == SampleNode.Type.SYSTEM) {
                return context.defaultRewrite(node);
            }
            throw new UnsupportedOperationException("not yet implemented");
        }
    }
}
