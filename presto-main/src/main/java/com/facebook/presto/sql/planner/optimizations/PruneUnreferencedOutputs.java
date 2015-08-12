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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.concat;

/**
 * Removes all computation that does is not referenced transitively from the root of the plan
 * <p>
 * E.g.,
 * <p>
 * {@code Output[$0] -> Project[$0 := $1 + $2, $3 = $4 / $5] -> ...}
 * <p>
 * gets rewritten as
 * <p>
 * {@code Output[$0] -> Project[$0 := $1 + $2] -> ...}
 */
public class PruneUnreferencedOutputs
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

        return PlanRewriter.rewriteWith(new Rewriter(types), plan, ImmutableSet.<Symbol>of());
    }

    private static class Rewriter
            extends PlanRewriter<Set<Symbol>>
    {
        private final Map<Symbol, Type> types;

        public Rewriter(Map<Symbol, Type> types)
        {
            this.types = types;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedOutputSymbols = Sets.newHashSet(context.get());
            node.getHashSymbol().ifPresent(expectedOutputSymbols::add);
            node.getPartitionKeys().ifPresent(expectedOutputSymbols::addAll);

            List<List<Symbol>> inputsBySource = new ArrayList<>(node.getInputs().size());
            for (int i = 0; i < node.getInputs().size(); i++) {
                inputsBySource.add(new ArrayList<>());
            }

            List<Symbol> newOutputSymbols = new ArrayList<>(node.getOutputSymbols().size());
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol outputSymbol = node.getOutputSymbols().get(i);
                if (expectedOutputSymbols.contains(outputSymbol)) {
                    newOutputSymbols.add(outputSymbol);
                    for (int source = 0; source < node.getInputs().size(); source++) {
                        inputsBySource.get(source).add(node.getInputs().get(source).get(i));
                    }
                }
            }

            ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.<PlanNode>builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                        .addAll(inputsBySource.get(i));

                rewrittenSources.add(context.rewrite(
                        node.getSources().get(i),
                        expectedInputs.build()));
            }

            return new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getPartitionKeys(),
                    node.getHashSymbol(),
                    rewrittenSources.build(),
                    newOutputSymbols,
                    inputsBySource);
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> leftInputsBuilder = ImmutableSet.builder();
            leftInputsBuilder.addAll(context.get()).addAll(Iterables.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft));
            if (node.getLeftHashSymbol().isPresent()) {
                leftInputsBuilder.add(node.getLeftHashSymbol().get());
            }
            Set<Symbol> leftInputs = leftInputsBuilder.build();

            ImmutableSet.Builder<Symbol> rightInputsBuilder = ImmutableSet.builder();
            rightInputsBuilder.addAll(context.get()).addAll(Iterables.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight));
            if (node.getRightHashSymbol().isPresent()) {
                rightInputsBuilder.add(node.getRightHashSymbol().get());
            }

            Set<Symbol> rightInputs = rightInputsBuilder.build();

            PlanNode left = context.rewrite(node.getLeft(), leftInputs);
            PlanNode right = context.rewrite(node.getRight(), rightInputs);

            return new JoinNode(node.getId(), node.getType(), left, right, node.getCriteria(), node.getLeftHashSymbol(), node.getRightHashSymbol());
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> sourceInputsBuilder = ImmutableSet.builder();
            sourceInputsBuilder.addAll(context.get()).add(node.getSourceJoinSymbol());
            if (node.getSourceHashSymbol().isPresent()) {
                sourceInputsBuilder.add(node.getSourceHashSymbol().get());
            }
            Set<Symbol> sourceInputs = sourceInputsBuilder.build();

            ImmutableSet.Builder<Symbol> filteringSourceInputBuilder = ImmutableSet.builder();
            filteringSourceInputBuilder.add(node.getFilteringSourceJoinSymbol());
            if (node.getFilteringSourceHashSymbol().isPresent()) {
                filteringSourceInputBuilder.add(node.getFilteringSourceHashSymbol().get());
            }
            Set<Symbol> filteringSourceInputs = filteringSourceInputBuilder.build();

            PlanNode source = context.rewrite(node.getSource(), sourceInputs);
            PlanNode filteringSource = context.rewrite(node.getFilteringSource(), filteringSourceInputs);

            return new SemiJoinNode(node.getId(),
                    source,
                    filteringSource,
                    node.getSourceJoinSymbol(),
                    node.getFilteringSourceJoinSymbol(),
                    node.getSemiJoinOutput(),
                    node.getSourceHashSymbol(),
                    node.getFilteringSourceHashSymbol());
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> probeInputsBuilder = ImmutableSet.builder();
            probeInputsBuilder.addAll(context.get())
                    .addAll(Iterables.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getProbe));
            if (node.getProbeHashSymbol().isPresent()) {
                probeInputsBuilder.add(node.getProbeHashSymbol().get());
            }
            Set<Symbol> probeInputs = probeInputsBuilder.build();

            ImmutableSet.Builder<Symbol> indexInputBuilder = ImmutableSet.builder();
            indexInputBuilder.addAll(context.get())
                    .addAll(Iterables.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getIndex));
            if (node.getIndexHashSymbol().isPresent()) {
                indexInputBuilder.add(node.getIndexHashSymbol().get());
            }
            Set<Symbol> indexInputs = indexInputBuilder.build();

            PlanNode probeSource = context.rewrite(node.getProbeSource(), probeInputs);
            PlanNode indexSource = context.rewrite(node.getIndexSource(), indexInputs);

            return new IndexJoinNode(node.getId(), node.getType(), probeSource, indexSource, node.getCriteria(), node.getProbeHashSymbol(), node.getIndexHashSymbol());
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, RewriteContext<Set<Symbol>> context)
        {
            List<Symbol> newOutputSymbols = FluentIterable.from(node.getOutputSymbols())
                    .filter(in(context.get()))
                    .toList();

            Set<Symbol> newLookupSymbols = FluentIterable.from(node.getLookupSymbols())
                    .filter(in(context.get()))
                    .toSet();

            Set<Symbol> requiredAssignmentSymbols = context.get();
            if (!node.getEffectiveTupleDomain().isNone()) {
                Set<Symbol> requiredSymbols = Maps.filterValues(node.getAssignments(), in(node.getEffectiveTupleDomain().getDomains().keySet())).keySet();
                requiredAssignmentSymbols = Sets.union(context.get(), requiredSymbols);
            }
            Map<Symbol, ColumnHandle> newAssignments = Maps.filterKeys(node.getAssignments(), in(requiredAssignmentSymbols));

            return new IndexSourceNode(node.getId(), node.getIndexHandle(), node.getTableHandle(), newLookupSymbols, newOutputSymbols, newAssignments, node.getEffectiveTupleDomain());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(node.getGroupBy());
            if (node.getHashSymbol().isPresent()) {
                expectedInputs.add(node.getHashSymbol().get());
            }

            ImmutableMap.Builder<Symbol, Signature> functions = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, FunctionCall> functionCalls = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, Symbol> masks = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                if (context.get().contains(symbol)) {
                    FunctionCall call = entry.getValue();
                    expectedInputs.addAll(DependencyExtractor.extractUnique(call));
                    if (node.getMasks().containsKey(symbol)) {
                        expectedInputs.add(node.getMasks().get(symbol));
                        masks.put(symbol, node.getMasks().get(symbol));
                    }

                    functionCalls.put(symbol, call);
                    functions.put(symbol, node.getFunctions().get(symbol));
                }
            }
            if (node.getSampleWeight().isPresent()) {
                expectedInputs.add(node.getSampleWeight().get());
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new AggregationNode(node.getId(),
                    source,
                    node.getGroupBy(),
                    functionCalls.build(),
                    functions.build(),
                    masks.build(),
                    node.getStep(),
                    node.getSampleWeight(),
                    node.getConfidence(),
                    node.getHashSymbol());
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(context.get())
                    .addAll(node.getPartitionBy())
                    .addAll(node.getOrderBy());

            if (node.getFrame().getStartValue().isPresent()) {
                expectedInputs.add(node.getFrame().getStartValue().get());
            }
            if (node.getFrame().getEndValue().isPresent()) {
                expectedInputs.add(node.getFrame().getEndValue().get());
            }

            if (node.getHashSymbol().isPresent()) {
                expectedInputs.add(node.getHashSymbol().get());
            }

            ImmutableMap.Builder<Symbol, Signature> functions = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, FunctionCall> functionCalls = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                Symbol symbol = entry.getKey();

                if (context.get().contains(symbol)) {
                    FunctionCall call = entry.getValue();
                    expectedInputs.addAll(DependencyExtractor.extractUnique(call));

                    functionCalls.put(symbol, call);
                    functions.put(symbol, node.getSignatures().get(symbol));
                }
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new WindowNode(
                    node.getId(),
                    source,
                    node.getPartitionBy(),
                    node.getOrderBy(),
                    node.getOrderings(),
                    node.getFrame(),
                    functionCalls.build(),
                    functions.build(),
                    node.getHashSymbol(),
                    node.getPrePartitionedInputs(),
                    node.getPreSortedOrderPrefix());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> requiredTableScanOutputs = FluentIterable.from(context.get())
                    .filter(in(ImmutableSet.copyOf(node.getOutputSymbols())))
                    .toSet();

            List<Symbol> newOutputSymbols = FluentIterable.from(node.getOutputSymbols())
                    .filter(in(requiredTableScanOutputs))
                    .toList();

            Map<Symbol, ColumnHandle> newAssignments = Maps.filterKeys(node.getAssignments(), in(requiredTableScanOutputs));

            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    newOutputSymbols,
                    newAssignments,
                    node.getLayout(),
                    node.getCurrentConstraint(),
                    node.getOriginalConstraint());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(DependencyExtractor.extractUnique(node.getPredicate()))
                    .addAll(context.get())
                    .build();

            PlanNode source = context.rewrite(node.getSource(), expectedInputs);

            return new FilterNode(node.getId(), source, node.getPredicate());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Set<Symbol>> context)
        {
            if (!context.get().contains(node.getMarkerSymbol())) {
                return context.rewrite(node.getSource(), context.get());
            }

            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(node.getDistinctSymbols())
                    .addAll(context.get());

            if (node.getHashSymbol().isPresent()) {
                expectedInputs.add(node.getHashSymbol().get());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new MarkDistinctNode(node.getId(), source, node.getMarkerSymbol(), node.getDistinctSymbols(), node.getHashSymbol());
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Set<Symbol>> context)
        {
            List<Symbol> replicateSymbols = FluentIterable.from(node.getReplicateSymbols())
                    .filter(in(context.get()))
                    .toList();
            Optional<Symbol> ordinalitySymbol = node.getOrdinalitySymbol();
            if (ordinalitySymbol.isPresent() && !context.get().contains(ordinalitySymbol.get())) {
                ordinalitySymbol = Optional.empty();
            }
            Map<Symbol, List<Symbol>> unnestSymbols = node.getUnnestSymbols();
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(replicateSymbols)
                    .addAll(unnestSymbols.keySet());

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new UnnestNode(node.getId(), source, replicateSymbols, unnestSymbols, ordinalitySymbol);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.builder();

            ImmutableMap.Builder<Symbol, Expression> builder = ImmutableMap.builder();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol output = node.getOutputSymbols().get(i);
                Expression expression = node.getExpressions().get(i);

                if (context.get().contains(output)) {
                    expectedInputs.addAll(DependencyExtractor.extractUnique(expression));
                    builder.put(output, expression);
                }
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new ProjectNode(node.getId(), source, builder.build());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedInputs = ImmutableSet.copyOf(node.getOutputSymbols());
            PlanNode source = context.rewrite(node.getSource(), expectedInputs);
            return new OutputNode(node.getId(), source, node.getColumnNames(), node.getOutputSymbols());
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(context.get());
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new LimitNode(node.getId(), source, node.getCount());
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedInputs;
            if (node.getHashSymbol().isPresent()) {
                expectedInputs = ImmutableSet.copyOf(concat(node.getOutputSymbols(), ImmutableList.of(node.getHashSymbol().get())));
            }
            else {
                expectedInputs = ImmutableSet.copyOf(node.getOutputSymbols());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs);
            return new DistinctLimitNode(node.getId(), source, node.getLimit(), node.getHashSymbol());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(context.get())
                    .addAll(node.getOrderBy());

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new TopNNode(node.getId(), source, node.getCount(), node.getOrderBy(), node.getOrderings(), node.isPartial());
        }

        @Override
        public PlanNode visitRowNumber(RowNumberNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> inputsBuilder = ImmutableSet.builder();
            ImmutableSet.Builder<Symbol> expectedInputs = inputsBuilder
                    .addAll(context.get())
                    .addAll(node.getPartitionBy());

            if (node.getHashSymbol().isPresent()) {
                inputsBuilder.add(node.getHashSymbol().get());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new RowNumberNode(node.getId(), source, node.getPartitionBy(), node.getRowNumberSymbol(), node.getMaxRowCountPerPartition(), node.getHashSymbol());
        }

        @Override
        public PlanNode visitTopNRowNumber(TopNRowNumberNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(context.get())
                    .addAll(node.getPartitionBy())
                    .addAll(node.getOrderBy());

            if (node.getHashSymbol().isPresent()) {
                expectedInputs.add(node.getHashSymbol().get());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new TopNRowNumberNode(node.getId(),
                    source,
                    node.getPartitionBy(),
                    node.getOrderBy(),
                    node.getOrderings(),
                    node.getRowNumberSymbol(),
                    node.getMaxRowCountPerPartition(),
                    node.isPartial(),
                    node.getHashSymbol());
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedInputs = ImmutableSet.copyOf(concat(context.get(), node.getOrderBy()));

            PlanNode source = context.rewrite(node.getSource(), expectedInputs);

            return new SortNode(node.getId(), source, node.getOrderBy(), node.getOrderings());
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(node.getColumns());
            if (node.getSampleWeightSymbol().isPresent()) {
                expectedInputs.add(node.getSampleWeightSymbol().get());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new TableWriterNode(node.getId(), source, node.getTarget(), node.getColumns(), node.getColumnNames(), node.getOutputSymbols(), node.getSampleWeightSymbol());
        }

        @Override
        public PlanNode visitTableCommit(TableCommitNode node, RewriteContext<Set<Symbol>> context)
        {
            // Maintain the existing inputs needed for TableCommitNode
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.copyOf(node.getSource().getOutputSymbols()));
            return new TableCommitNode(node.getId(), source, node.getTarget(), node.getOutputSymbols());
        }

        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Set<Symbol>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.of(node.getRowId()));
            return new DeleteNode(node.getId(), source, node.getTarget(), node.getRowId(), node.getOutputSymbols());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Set<Symbol>> context)
        {
            // Find out which output symbols we need to keep
            ImmutableListMultimap.Builder<Symbol, Symbol> rewrittenSymbolMappingBuilder = ImmutableListMultimap.builder();
            for (Symbol symbol : node.getOutputSymbols()) {
                if (context.get().contains(symbol)) {
                    rewrittenSymbolMappingBuilder.putAll(symbol, node.getSymbolMapping().get(symbol));
                }
            }
            ListMultimap<Symbol, Symbol> rewrittenSymbolMapping = rewrittenSymbolMappingBuilder.build();

            // Find the corresponding input symbol to the remaining output symbols and prune the subplans
            ImmutableList.Builder<PlanNode> rewrittenSubPlans = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                ImmutableSet.Builder<Symbol> expectedInputSymbols = ImmutableSet.builder();
                for (Collection<Symbol> symbols : rewrittenSymbolMapping.asMap().values()) {
                    expectedInputSymbols.add(Iterables.get(symbols, i));
                }
                rewrittenSubPlans.add(context.rewrite(node.getSources().get(i), expectedInputSymbols.build()));
            }

            return new UnionNode(node.getId(), rewrittenSubPlans.build(), rewrittenSymbolMapping);
        }
    }
}
