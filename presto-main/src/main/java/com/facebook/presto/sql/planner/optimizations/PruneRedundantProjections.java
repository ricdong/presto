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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Removes pure identity projections (e.g., Project $0 := $0, $1 := $1, ...)
 */
public class PruneRedundantProjections
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
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            if (node.getOutputSymbols().size() != source.getOutputSymbols().size()) {
                // Can't get rid of this projection. It constrains the output tuple from the underlying operator
                return context.replaceChildren(node, ImmutableList.of(source));
            }

            boolean canElide = true;
            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                Expression expression = entry.getValue();
                Symbol symbol = entry.getKey();
                if (!(expression instanceof QualifiedNameReference && ((QualifiedNameReference) expression).getName().equals(symbol.toQualifiedName()))) {
                    canElide = false;
                    break;
                }
            }

            if (canElide) {
                return source;
            }

            return context.replaceChildren(node, ImmutableList.of(source));
        }
    }
}
