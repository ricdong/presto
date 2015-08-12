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
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class SetFlatteningOptimizer
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

        return PlanRewriter.rewriteWith(new Rewriter(), plan, false);
    }

    // TODO: remove expectation that UNION DISTINCT => distinct aggregation directly above union node
    private static class Rewriter
            extends PlanRewriter<Boolean>
    {
        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Boolean> context)
        {
            return context.defaultRewrite(node, false);
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Boolean> context)
        {
            ImmutableList.Builder<PlanNode> flattenedSources = ImmutableList.builder();
            ImmutableListMultimap.Builder<Symbol, Symbol> flattenedSymbolMap = ImmutableListMultimap.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                PlanNode rewrittenSource = context.rewrite(subplan, context.get());

                if (rewrittenSource instanceof UnionNode) {
                    // Absorb source's subplans if it is also a UnionNode
                    UnionNode rewrittenUnion = (UnionNode) rewrittenSource;
                    flattenedSources.addAll(rewrittenUnion.getSources());
                    for (Map.Entry<Symbol, Collection<Symbol>> entry : node.getSymbolMapping().asMap().entrySet()) {
                        Symbol inputSymbol = Iterables.get(entry.getValue(), i);
                        flattenedSymbolMap.putAll(entry.getKey(), rewrittenUnion.getSymbolMapping().get(inputSymbol));
                    }
                }
                else {
                    flattenedSources.add(rewrittenSource);
                    for (Map.Entry<Symbol, Collection<Symbol>> entry : node.getSymbolMapping().asMap().entrySet()) {
                        flattenedSymbolMap.put(entry.getKey(), Iterables.get(entry.getValue(), i));
                    }
                }
            }
            return new UnionNode(node.getId(), flattenedSources.build(), flattenedSymbolMap.build());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Boolean> context)
        {
            boolean distinct = isDistinctOperator(node);

            PlanNode rewrittenNode = context.rewrite(node.getSource(), distinct);

            if (context.get() && distinct) {
                // Assumes underlying node has same output symbols as this distinct node
                return rewrittenNode;
            }

            return new AggregationNode(
                    node.getId(),
                    rewrittenNode,
                    node.getGroupBy(),
                    node.getAggregations(),
                    node.getFunctions(),
                    node.getMasks(),
                    node.getStep(),
                    node.getSampleWeight(),
                    node.getConfidence(),
                    node.getHashSymbol());
        }

        private static boolean isDistinctOperator(AggregationNode node)
        {
            return node.getAggregations().isEmpty();
        }
    }
}
