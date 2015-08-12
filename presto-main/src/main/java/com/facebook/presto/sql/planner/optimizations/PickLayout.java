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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static java.util.Objects.requireNonNull;

/**
 * Pick an arbitrary layout if none has been chosen
 */
public class PickLayout
        extends PlanOptimizer
{
    private final Metadata metadata;

    public PickLayout(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return PlanRewriter.rewriteWith(new Rewriter(metadata, session, symbolAllocator, idAllocator), plan);
    }

    private static class Rewriter
            extends PlanRewriter<Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(Metadata metadata, Session session, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
        {
            this.metadata = metadata;
            this.session = session;
            this.symbolAllocator = symbolAllocator;
            this.idAllocator = idAllocator;
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            return context.defaultRewrite(node);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            if (node.getSource() instanceof TableScanNode && !((TableScanNode) node.getSource()).getLayout().isPresent()) {
                return planTableScan((TableScanNode) node.getSource(), node.getPredicate());
            }

            return context.defaultRewrite(node);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            if (node.getLayout().isPresent()) {
                return node;
            }

            return planTableScan(node, BooleanLiteral.TRUE_LITERAL);
        }

        private PlanNode planTableScan(TableScanNode node, Expression predicate)
        {
            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                    metadata,
                    session,
                    predicate,
                    symbolAllocator.getTypes());

            TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                    .transform(node.getAssignments()::get)
                    .intersect(node.getCurrentConstraint());

            List<TableLayoutResult> layouts = metadata.getLayouts(
                    session, node.getTable(),
                    new Constraint<>(simplifiedConstraint, bindings -> true),
                    Optional.of(ImmutableSet.copyOf(node.getAssignments().values())));

            if (layouts.isEmpty()) {
                return new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of());
            }

            TableLayoutResult layout = layouts.get(0);

            TableScanNode result = new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    Optional.of(layout.getLayout().getHandle()),
                    simplifiedConstraint.intersect(layout.getLayout().getPredicate()),
                    Optional.ofNullable(node.getOriginalConstraint()).orElse(predicate));

            Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
            Expression resultingPredicate = combineConjuncts(
                    decomposedPredicate.getRemainingExpression(),
                    DomainTranslator.toPredicate(
                            layout.getUnenforcedConstraint().transform(assignments::get),
                            symbolAllocator.getTypes()));

            if (!BooleanLiteral.TRUE_LITERAL.equals(resultingPredicate)) {
                return new FilterNode(idAllocator.getNextId(), result, resultingPredicate);
            }

            return result;
        }
    }
}
