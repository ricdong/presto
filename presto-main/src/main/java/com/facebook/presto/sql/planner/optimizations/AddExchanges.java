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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LookupSymbolResolver;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ChildReplacer;
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
import com.facebook.presto.sql.planner.plan.PlanVisitor;
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
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripDeterministicConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripNonDeterministicConjuncts;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.optimizations.LocalProperties.grouped;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchangeNullReplicate;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.toList;

public class AddExchanges
        extends PlanOptimizer
{
    private final SqlParser parser;
    private final Metadata metadata;
    private final boolean distributedIndexJoins;

    public AddExchanges(Metadata metadata, SqlParser parser, boolean distributedIndexJoins)
    {
        this.metadata = metadata;
        this.parser = parser;
        this.distributedIndexJoins = distributedIndexJoins;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        boolean distributedJoinEnabled = SystemSessionProperties.isDistributedJoinEnabled(session);
        boolean redistributeWrites = SystemSessionProperties.isRedistributeWrites(session);
        boolean preferStreamingOperators = SystemSessionProperties.preferStreamingOperators(session);
        PlanWithProperties result = plan.accept(new Rewriter(symbolAllocator, idAllocator, symbolAllocator, session, distributedIndexJoins, distributedJoinEnabled, preferStreamingOperators, redistributeWrites), new Context(PreferredProperties.any(), false));
        return result.getNode();
    }

    private static class Context
    {
        private PreferredProperties preferredProperties;
        // For delete queries, the TableScan node that corresponds to the table being deleted on must be collocated with the Delete node.
        // Care must be taken so that Exchange node is not introduced between the two. For now, only SemiJoin may introduce it.
        private boolean downstreamIsDelete;

        Context(PreferredProperties preferredProperties, boolean downstreamIsDelete)
        {
            this.preferredProperties = preferredProperties;
            this.downstreamIsDelete = downstreamIsDelete;
        }

        Context withPreferredProperties(PreferredProperties preferredProperties)
        {
            return new Context(preferredProperties, downstreamIsDelete);
        }

        Context withHashPartitionedSemiJoinBanned(boolean hashPartitionedSemiJoinBanned)
        {
            return new Context(preferredProperties, hashPartitionedSemiJoinBanned);
        }

        PreferredProperties getPreferredProperties()
        {
            return preferredProperties;
        }

        boolean isDownstreamIsDelete()
        {
            return downstreamIsDelete;
        }
    }

    private class Rewriter
            extends PlanVisitor<Context, PlanWithProperties>
    {
        private final SymbolAllocator allocator;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Session session;
        private final boolean distributedIndexJoins;
        private final boolean distributedJoins;
        private final boolean preferStreamingOperators;
        private final boolean redistributeWrites;

        public Rewriter(SymbolAllocator allocator, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session, boolean distributedIndexJoins, boolean distributedJoins, boolean preferStreamingOperators, boolean redistributeWrites)
        {
            this.allocator = allocator;
            this.idAllocator = idAllocator;
            this.symbolAllocator = symbolAllocator;
            this.session = session;
            this.distributedIndexJoins = distributedIndexJoins;
            this.distributedJoins = distributedJoins;
            this.preferStreamingOperators = preferStreamingOperators;
            this.redistributeWrites = redistributeWrites;
        }

        @Override
        protected PlanWithProperties visitPlan(PlanNode node, Context context)
        {
            return rebaseAndDeriveProperties(node, planChild(node, context));
        }

        @Override
        public PlanWithProperties visitDelete(DeleteNode node, Context context)
        {
            // Delete operator does not work unless it is co-located with the corresponding TableScan.
            return rebaseAndDeriveProperties(node, planChild(node, context.withHashPartitionedSemiJoinBanned(true)));
        }

        @Override
        public PlanWithProperties visitProject(ProjectNode node, Context context)
        {
            Map<Symbol, Symbol> identities = computeIdentityTranslations(node.getAssignments());
            PreferredProperties translatedPreferred = context.getPreferredProperties().translate(symbol -> Optional.ofNullable(identities.get(symbol)));

            return rebaseAndDeriveProperties(node, planChild(node, context.withPreferredProperties(translatedPreferred)));
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            if (child.getProperties().isDistributed()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitAggregation(AggregationNode node, Context context)
        {
            boolean decomposable = node.getFunctions()
                    .values().stream()
                    .map(metadata::getExactFunction)
                    .map(FunctionInfo::getAggregationFunction)
                    .allMatch(InternalAggregationFunction::isDecomposable);

            PreferredProperties preferredProperties = node.getGroupBy().isEmpty()
                    ? PreferredProperties.any()
                    : PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(node.getGroupBy()), Optional.of(node.getGroupBy()), grouped(node.getGroupBy()));

            PlanWithProperties child = planChild(node, context.withPreferredProperties(preferredProperties));

            if (!child.getProperties().isDistributed()) {
                // If already unpartitioned, just drop the single aggregation back on
                return rebaseAndDeriveProperties(node, child);
            }

            if (node.getGroupBy().isEmpty()) {
                if (decomposable) {
                    return splitAggregation(node, child, partial -> gatheringExchange(idAllocator.getNextId(), partial));
                }
                else {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), child.getNode()),
                            child.getProperties());

                    return rebaseAndDeriveProperties(node, child);
                }
            }
            else {
                if (child.getProperties().isPartitionedOn(node.getGroupBy())) {
                    return rebaseAndDeriveProperties(node, child);
                }
                else {
                    if (decomposable) {
                        return splitAggregation(node, child, partial -> partitionedExchange(idAllocator.getNextId(), partial, Optional.of(node.getGroupBy()), node.getHashSymbol()));
                    }
                    else {
                        child = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), child.getNode(), Optional.of(node.getGroupBy()), node.getHashSymbol()),
                                child.getProperties());
                        return rebaseAndDeriveProperties(node, child);
                    }
                }
            }
        }

        @NotNull
        private PlanWithProperties splitAggregation(AggregationNode node, PlanWithProperties newChild, Function<PlanNode, PlanNode> exchanger)
        {
            // otherwise, add a partial and final with an exchange in between
            Map<Symbol, Symbol> masks = node.getMasks();

            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            Map<Symbol, FunctionCall> intermediateCalls = new HashMap<>();
            Map<Symbol, Signature> intermediateFunctions = new HashMap<>();
            Map<Symbol, Symbol> intermediateMask = new HashMap<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Signature signature = node.getFunctions().get(entry.getKey());
                FunctionInfo function = metadata.getExactFunction(signature);

                Symbol intermediateSymbol = allocator.newSymbol(function.getName().getSuffix(), metadata.getType(function.getIntermediateType()));
                intermediateCalls.put(intermediateSymbol, entry.getValue());
                intermediateFunctions.put(intermediateSymbol, signature);
                if (masks.containsKey(entry.getKey())) {
                    intermediateMask.put(intermediateSymbol, masks.get(entry.getKey()));
                }

                // rewrite final aggregation in terms of intermediate function
                finalCalls.put(entry.getKey(), new FunctionCall(function.getName(), ImmutableList.<Expression>of(new QualifiedNameReference(intermediateSymbol.toQualifiedName()))));
            }

            PlanWithProperties partial = withDerivedProperties(
                    new AggregationNode(
                            idAllocator.getNextId(),
                            newChild.getNode(),
                            node.getGroupBy(),
                            intermediateCalls,
                            intermediateFunctions,
                            intermediateMask,
                            PARTIAL,
                            node.getSampleWeight(),
                            node.getConfidence(),
                            node.getHashSymbol()),
                    newChild.getProperties());

            PlanNode exchange = exchanger.apply(partial.getNode());

            return withDerivedProperties(
                    new AggregationNode(
                            node.getId(),
                            exchange,
                            node.getGroupBy(),
                            finalCalls,
                            node.getFunctions(),
                            ImmutableMap.of(),
                            FINAL,
                            Optional.empty(),
                            node.getConfidence(),
                            node.getHashSymbol()),
                    deriveProperties(exchange, partial.getProperties()));
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, Context context)
        {
            PreferredProperties preferredChildProperties = PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(node.getDistinctSymbols()), Optional.of(node.getDistinctSymbols()), grouped(node.getDistinctSymbols()));
            PlanWithProperties child = node.getSource().accept(this, context.withPreferredProperties(preferredChildProperties));

            if (!child.getProperties().isDistributed() ||
                    !child.getProperties().isPartitionedOn(node.getDistinctSymbols())) {
                child = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                child.getNode(),
                                Optional.of(node.getDistinctSymbols()),
                                node.getHashSymbol()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, Context context)
        {
            List<LocalProperty<Symbol>> desiredProperties = new ArrayList<>();
            if (!node.getPartitionBy().isEmpty()) {
                desiredProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }
            for (Symbol symbol : node.getOrderBy()) {
                desiredProperties.add(new SortingProperty<>(symbol, node.getOrderings().get(symbol)));
            }

            PlanWithProperties child = planChild(
                    node,
                    context.withPreferredProperties(PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(node.getPartitionBy()), desiredProperties)));

            if (!child.getProperties().isPartitionedOn(node.getPartitionBy())) {
                if (node.getPartitionBy().isEmpty()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), child.getNode()),
                            child.getProperties());
                }
                else {
                    child = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), child.getNode(), Optional.of(node.getPartitionBy()), node.getHashSymbol()),
                            child.getProperties());
                }
            }

            Iterator<Optional<LocalProperty<Symbol>>> matchIterator = LocalProperties.match(child.getProperties().getLocalProperties(), desiredProperties).iterator();

            Set<Symbol> prePartitionedInputs = ImmutableSet.of();
            if (!node.getPartitionBy().isEmpty()) {
                Optional<LocalProperty<Symbol>> groupingRequirement = matchIterator.next();
                Set<Symbol> unPartitionedInputs = groupingRequirement.map(LocalProperty::getColumns).orElse(ImmutableSet.of());
                prePartitionedInputs = node.getPartitionBy().stream()
                        .filter(symbol -> !unPartitionedInputs.contains(symbol))
                        .collect(toImmutableSet());
            }

            int preSortedOrderPrefix = 0;
            if (prePartitionedInputs.equals(ImmutableSet.copyOf(node.getPartitionBy()))) {
                while (matchIterator.hasNext() && !matchIterator.next().isPresent()) {
                    preSortedOrderPrefix++;
                }
            }

            return withDerivedProperties(
                    new WindowNode(
                            node.getId(),
                            child.getNode(),
                            node.getPartitionBy(),
                            node.getOrderBy(),
                            node.getOrderings(),
                            node.getFrame(),
                            node.getWindowFunctions(),
                            node.getSignatures(),
                            node.getHashSymbol(),
                            prePartitionedInputs,
                            preSortedOrderPrefix),
                    child.getProperties());
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, Context context)
        {
            if (node.getPartitionBy().isEmpty()) {
                PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.undistributed()));

                if (child.getProperties().isDistributed()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), child.getNode()),
                            child.getProperties());
                }

                return rebaseAndDeriveProperties(node, child);
            }

            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(node.getPartitionBy()), grouped(node.getPartitionBy()))));

            // TODO: add config option/session property to force parallel plan if child is unpartitioned and window has a PARTITION BY clause
            if (!child.getProperties().isPartitionedOn(node.getPartitionBy())) {
                child = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                child.getNode(),
                                Optional.of(node.getPartitionBy()),
                                node.getHashSymbol()),
                        child.getProperties());
            }

            // TODO: streaming

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTopNRowNumber(TopNRowNumberNode node, Context context)
        {
            PreferredProperties preferredChildProperties;
            Function<PlanNode, PlanNode> addExchange;

            if (node.getPartitionBy().isEmpty()) {
                preferredChildProperties = PreferredProperties.any();
                addExchange = partial -> gatheringExchange(idAllocator.getNextId(), partial);
            }
            else {
                preferredChildProperties = PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(node.getPartitionBy()), grouped(node.getPartitionBy()));
                addExchange = partial -> partitionedExchange(idAllocator.getNextId(), partial, Optional.of(node.getPartitionBy()), node.getHashSymbol());
            }

            PlanWithProperties child = planChild(node, context.withPreferredProperties(preferredChildProperties));
            if (!child.getProperties().isPartitionedOn(node.getPartitionBy())) {
                // add exchange + push function to child
                child = withDerivedProperties(
                        new TopNRowNumberNode(
                                idAllocator.getNextId(),
                                child.getNode(),
                                node.getPartitionBy(),
                                node.getOrderBy(),
                                node.getOrderings(),
                                node.getRowNumberSymbol(),
                                node.getMaxRowCountPerPartition(),
                                true,
                                node.getHashSymbol()),
                        child.getProperties());

                child = withDerivedProperties(addExchange.apply(child.getNode()), child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTopN(TopNNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            if (child.getProperties().isDistributed()) {
                child = withDerivedProperties(
                        new TopNNode(idAllocator.getNextId(), child.getNode(), node.getCount(), node.getOrderBy(), node.getOrderings(), true),
                        child.getProperties());

                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitSort(SortNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.undistributed()));

            if (child.getProperties().isDistributed()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitLimit(LimitNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            if (child.getProperties().isDistributed()) {
                child = withDerivedProperties(
                        new LimitNode(idAllocator.getNextId(), child.getNode(), node.getCount()),
                        child.getProperties());

                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));

            if (child.getProperties().isDistributed()) {
                child = withDerivedProperties(
                        new DistinctLimitNode(idAllocator.getNextId(), child.getNode(), node.getLimit(), node.getHashSymbol()),
                        child.getProperties());

                child = withDerivedProperties(
                        gatheringExchange(
                                idAllocator.getNextId(),
                                new DistinctLimitNode(idAllocator.getNextId(), child.getNode(), node.getLimit(), node.getHashSymbol())),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitFilter(FilterNode node, Context context)
        {
            if (node.getSource() instanceof TableScanNode) {
                return planTableScan((TableScanNode) node.getSource(), node.getPredicate(), context);
            }

            return rebaseAndDeriveProperties(node, planChild(node, context));
        }

        @Override
        public PlanWithProperties visitTableScan(TableScanNode node, Context context)
        {
            return planTableScan(node, BooleanLiteral.TRUE_LITERAL, context);
        }

        @Override
        public PlanWithProperties visitTableWriter(TableWriterNode node, Context context)
        {
            PlanWithProperties source = node.getSource().accept(this, context);
            if (redistributeWrites) {
                source = withDerivedProperties(
                        partitionedExchange(idAllocator.getNextId(), source.getNode(), Optional.empty(), Optional.empty()),
                        source.getProperties()
                );
            }
            return rebaseAndDeriveProperties(node, source);
        }

        private PlanWithProperties planTableScan(TableScanNode node, Expression predicate, Context context)
        {
            // don't include non-deterministic predicates
            Expression deterministicPredicate = stripNonDeterministicConjuncts(predicate);

            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                    metadata,
                    session,
                    deterministicPredicate,
                    symbolAllocator.getTypes());

            TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                    .transform(node.getAssignments()::get)
                    .intersect(node.getCurrentConstraint());

            Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            Expression constraint = combineConjuncts(
                    deterministicPredicate,
                    DomainTranslator.toPredicate(
                            node.getCurrentConstraint().transform(assignments::get),
                            symbolAllocator.getTypes()));

            // Layouts will be returned in order of the connector's preference
            List<TableLayoutResult> layouts = metadata.getLayouts(
                    session, node.getTable(),
                    new Constraint<>(simplifiedConstraint, bindings -> !shouldPrune(constraint, node.getAssignments(), bindings)),
                    Optional.of(node.getOutputSymbols().stream()
                            .map(node.getAssignments()::get)
                            .collect(toImmutableSet())));

            if (layouts.isEmpty()) {
                return new PlanWithProperties(
                        new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()),
                        ActualProperties.undistributed());
            }

            // Filter out layouts that cannot supply all the required columns
            layouts = layouts.stream()
                    .filter(layoutHasAllNeededOutputs(node))
                    .collect(toList());
            checkState(!layouts.isEmpty(), "No usable layouts for %s", node);

            List<PlanWithProperties> possiblePlans = layouts.stream()
                    .map(layout -> {
                        TableScanNode tableScan = new TableScanNode(
                                node.getId(),
                                node.getTable(),
                                node.getOutputSymbols(),
                                node.getAssignments(),
                                Optional.of(layout.getLayout().getHandle()),
                                simplifiedConstraint.intersect(layout.getLayout().getPredicate()),
                                Optional.ofNullable(node.getOriginalConstraint()).orElse(predicate));

                        PlanWithProperties result = new PlanWithProperties(tableScan, deriveProperties(tableScan, ImmutableList.of()));

                        Expression resultingPredicate = combineConjuncts(
                                DomainTranslator.toPredicate(
                                        layout.getUnenforcedConstraint().transform(assignments::get),
                                        symbolAllocator.getTypes()),
                                stripDeterministicConjuncts(predicate),
                                decomposedPredicate.getRemainingExpression());

                        if (!BooleanLiteral.TRUE_LITERAL.equals(resultingPredicate)) {
                            return withDerivedProperties(
                                    new FilterNode(idAllocator.getNextId(), result.getNode(), resultingPredicate),
                                    deriveProperties(tableScan, ImmutableList.of()));
                        }

                        return result;
                    })
                    .collect(toList());

            return pickPlan(possiblePlans, context);
        }

        private Predicate<TableLayoutResult> layoutHasAllNeededOutputs(TableScanNode node)
        {
            return layout -> !layout.getLayout().getColumns().isPresent()
                    || layout.getLayout().getColumns().get().containsAll(Lists.transform(node.getOutputSymbols(), node.getAssignments()::get));
        }

        /**
         * possiblePlans should be provided in layout preference order
         */
        private PlanWithProperties pickPlan(List<PlanWithProperties> possiblePlans, Context context)
        {
            checkArgument(!possiblePlans.isEmpty());

            if (preferStreamingOperators) {
                possiblePlans = new ArrayList<>(possiblePlans);
                Collections.sort(possiblePlans, Comparator.comparing(PlanWithProperties::getProperties, streamingExecutionPreference(context.getPreferredProperties()))); // stable sort; is Collections.min() guaranteed to be stable?
            }

            return possiblePlans.get(0);
        }

        private boolean shouldPrune(Expression predicate, Map<Symbol, ColumnHandle> assignments, Map<ColumnHandle, ?> bindings)
        {
            List<Expression> conjuncts = extractConjuncts(predicate);
            IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypes(session, metadata, parser, symbolAllocator.getTypes(), predicate);

            LookupSymbolResolver inputs = new LookupSymbolResolver(assignments, bindings);

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            for (Expression expression : conjuncts) {
                ExpressionInterpreter optimizer = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
                Object optimized = optimizer.optimize(inputs);
                if (Boolean.FALSE.equals(optimized) || optimized == null || optimized instanceof NullLiteral) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public PlanWithProperties visitValues(ValuesNode node, Context context)
        {
            return new PlanWithProperties(node, ActualProperties.undistributed());
        }

        @Override
        public PlanWithProperties visitTableCommit(TableCommitNode node, Context context)
        {
            PlanWithProperties child = planChild(node, context.withPreferredProperties(PreferredProperties.any()));
            if (child.getProperties().isDistributed() || !child.getProperties().isCoordinatorOnly()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitJoin(JoinNode node, Context context)
        {
            List<Symbol> leftSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight);

            PlanWithProperties left;
            PlanWithProperties right;

            if (distributedJoins || node.getType() == FULL || node.getType() == RIGHT) {
                // The implementation of full outer join only works if the data is hash partitioned. See LookupJoinOperators#buildSideOuterJoinUnvisitedPositions

                left = node.getLeft().accept(this, context.withPreferredProperties(PreferredProperties.hashPartitioned(leftSymbols)));
                right = node.getRight().accept(this, context.withPreferredProperties(PreferredProperties.hashPartitioned(rightSymbols)));

                // force partitioning
                if (!left.getProperties().isHashPartitionedOn(leftSymbols)) {
                    left = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), left.getNode(), Optional.of(leftSymbols), node.getLeftHashSymbol()),
                            left.getProperties());
                }

                if (!right.getProperties().isHashPartitionedOn(rightSymbols)) {
                    right = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), right.getNode(), Optional.of(rightSymbols), node.getRightHashSymbol()),
                            right.getProperties());
                }
            }
            else {
                // It can only be INNER or LEFT here. Therefore, no flipping is necessary even though the below code assumes the node is not RIGHT.

                left = node.getLeft().accept(this, context.withPreferredProperties(PreferredProperties.any()));
                right = node.getRight().accept(this, context.withPreferredProperties(PreferredProperties.any()));

                if (!left.getProperties().isDistributed() && right.getProperties().isDistributed()) {
                    // force single-node join
                    // TODO: if inner join, flip order and do a broadcast join
                    right = withDerivedProperties(gatheringExchange(idAllocator.getNextId(), right.getNode()), right.getProperties());
                }
                else if (left.getProperties().isDistributed() && !(left.getProperties().isHashPartitionedOn(leftSymbols) && right.getProperties().isHashPartitionedOn(rightSymbols))) {
                    right = withDerivedProperties(new ExchangeNode(
                                    idAllocator.getNextId(),
                                    ExchangeNode.Type.REPLICATE,
                                    Optional.empty(),
                                    Optional.<Symbol>empty(),
                                    ImmutableList.of(right.getNode()),
                                    right.getNode().getOutputSymbols(),
                                    ImmutableList.of(right.getNode().getOutputSymbols())),
                            right.getProperties());
                }
            }

            JoinNode result = new JoinNode(node.getId(),
                    node.getType(),
                    left.getNode(),
                    right.getNode(),
                    node.getCriteria(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol());

            return new PlanWithProperties(result, deriveProperties(result, ImmutableList.of(left.getProperties(), right.getProperties())));
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, Context context)
        {
            PlanWithProperties source;
            PlanWithProperties filteringSource;

            if (distributedJoins && !context.isDownstreamIsDelete()) {
                List<Symbol> sourceSymbols = ImmutableList.of(node.getSourceJoinSymbol());
                List<Symbol> filteringSourceSymbols = ImmutableList.of(node.getFilteringSourceJoinSymbol());

                source = node.getSource().accept(this, context.withPreferredProperties(PreferredProperties.hashPartitioned(sourceSymbols)));
                // Child will not satisfy hash with null replicate anyways, and repartition is always necessary.
                // Therefore, tell the filtering source pick whatever partition it likes.
                filteringSource = node.getFilteringSource().accept(this, context.withPreferredProperties(PreferredProperties.any()));

                // force partitioning if source isn't already partitioned on sourceSymbols
                if (!source.getProperties().isHashPartitionedOn(sourceSymbols)) {
                    source = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), source.getNode(), Optional.of(sourceSymbols), node.getSourceHashSymbol()),
                            source.getProperties());
                }

                // The following statements would normally be written as: if (condition) { filteringSource = ...; }
                // However, the if-condition will always evaluate to true in this case because no externally-visible node produces partition with null replicate.
                // As a result, it is written as checkState instead.
                checkState(!filteringSource.getProperties().isHashPartitionedOn(filteringSourceSymbols) || !filteringSource.getProperties().isNullReplication());
                filteringSource = withDerivedProperties(
                        partitionedExchangeNullReplicate(idAllocator.getNextId(), filteringSource.getNode(), Iterables.getOnlyElement(filteringSourceSymbols), node.getFilteringSourceHashSymbol()),
                        filteringSource.getProperties());
            }
            else {
                source = node.getSource().accept(this, context.withPreferredProperties(PreferredProperties.any()));
                // Delete operator works fine even if TableScans on the filtering (right) side is not co-located with itself. It only cares about the corresponding TableScan,
                // which is always on the source (left) side. Therefore, hash-partitioned semi-join is always allowed on the filtering side.
                filteringSource = node.getFilteringSource().accept(this, context.withPreferredProperties(PreferredProperties.any()).withHashPartitionedSemiJoinBanned(false));

                // make filtering source match requirements of source
                if (source.getProperties().isDistributed()) {
                    filteringSource = withDerivedProperties(
                            new ExchangeNode(
                                    idAllocator.getNextId(),
                                    ExchangeNode.Type.REPLICATE,
                                    Optional.empty(),
                                    Optional.<Symbol>empty(),
                                    ImmutableList.of(filteringSource.getNode()),
                                    filteringSource.getNode().getOutputSymbols(),
                                    ImmutableList.of(filteringSource.getNode().getOutputSymbols())),
                            filteringSource.getProperties());
                }
                else {
                    filteringSource = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), filteringSource.getNode()),
                            filteringSource.getProperties());
                }
            }

            return rebaseAndDeriveProperties(node, ImmutableList.of(source, filteringSource));
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, Context context)
        {
            List<Symbol> joinColumns = Lists.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getProbe);

            // Only prefer grouping on join columns if no parent local property preferences
            List<LocalProperty<Symbol>> desiredLocalProperties = context.getPreferredProperties().getLocalProperties().isEmpty() ? grouped(joinColumns) : ImmutableList.of();

            PlanWithProperties probeSource = node.getProbeSource().accept(this, context.withPreferredProperties(PreferredProperties.derivePreferences(context.getPreferredProperties(), ImmutableSet.copyOf(joinColumns), desiredLocalProperties)));
            ActualProperties probeProperties = probeSource.getProperties();

            PlanWithProperties indexSource = node.getIndexSource().accept(this, context.withPreferredProperties(PreferredProperties.any()));

            // TODO: allow repartitioning if unpartitioned to increase parallelism
            if (distributedIndexJoins && probeProperties.isDistributed()) {
                // Force partitioned exchange if we are not effectively partitioned on the join keys, or if the probe is currently executing as a single stream
                // and the repartitioning will make a difference.
                boolean parentPartitioningPreferences = context.getPreferredProperties().getGlobalProperties()
                        .flatMap(PreferredProperties.Global::getPartitioningProperties)
                        .isPresent();
                boolean enableSinglePartitionRedistribute = !parentPartitioningPreferences || !preferStreamingOperators;
                if (!probeProperties.isPartitionedOn(joinColumns) || (enableSinglePartitionRedistribute && probeProperties.isEffectivelySinglePartition() && probeProperties.isRepartitionEffective(joinColumns))) {
                    probeSource = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), probeSource.getNode(), Optional.of(joinColumns), node.getProbeHashSymbol()),
                            probeProperties);
                }
            }

            // TODO: if input is grouped, create streaming join

            // index side is really a nested-loops plan, so don't add exchanges
            PlanNode result = ChildReplacer.replaceChildren(node, ImmutableList.of(probeSource.getNode(), node.getIndexSource()));
            return new PlanWithProperties(result, deriveProperties(result, ImmutableList.of(probeSource.getProperties(), indexSource.getProperties())));
        }

        @Override
        public PlanWithProperties visitIndexSource(IndexSourceNode node, Context context)
        {
            return new PlanWithProperties(node, ActualProperties.undistributed());
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, Context context)
        {
            if (!context.getPreferredProperties().getGlobalProperties().isPresent() || !context.getPreferredProperties().getGlobalProperties().get().isHashPartitioned()) {
                // first, classify children into partitioned and unpartitioned
                List<PlanNode> unpartitionedChildren = new ArrayList<>();
                List<List<Symbol>> unpartitionedOutputLayouts = new ArrayList<>();

                List<PlanNode> partitionedChildren = new ArrayList<>();
                List<List<Symbol>> partitionedOutputLayouts = new ArrayList<>();

                List<PlanNode> sources = node.getSources();
                for (int i = 0; i < sources.size(); i++) {
                    PlanWithProperties child = sources.get(i).accept(this, context.withPreferredProperties(PreferredProperties.any()));
                    if (!child.getProperties().isDistributed()) {
                        unpartitionedChildren.add(child.getNode());
                        unpartitionedOutputLayouts.add(node.sourceOutputLayout(i));
                    }
                    else {
                        partitionedChildren.add(child.getNode());
                        partitionedOutputLayouts.add(node.sourceOutputLayout(i));
                    }
                }

                PlanNode result = null;
                if (!partitionedChildren.isEmpty()) {
                    // add an exchange above partitioned inputs and fold it into the
                    // set of unpartitioned inputs
                    result = new ExchangeNode(
                            idAllocator.getNextId(),
                            ExchangeNode.Type.GATHER,
                            Optional.empty(),
                            Optional.<Symbol>empty(),
                            partitionedChildren,
                            node.getOutputSymbols(),
                            partitionedOutputLayouts);

                    unpartitionedChildren.add(result);
                    unpartitionedOutputLayouts.add(result.getOutputSymbols());
                }

                // if there's at least one unpartitioned input (including the exchange that might have been added in the
                // previous step), add a local union
                if (unpartitionedChildren.size() > 1) {
                    ImmutableListMultimap.Builder<Symbol, Symbol> mappings = ImmutableListMultimap.builder();
                    for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                        for (List<Symbol> outputLayout : unpartitionedOutputLayouts) {
                            mappings.put(node.getOutputSymbols().get(i), outputLayout.get(i));
                        }
                    }

                    result = new UnionNode(node.getId(), unpartitionedChildren, mappings.build());
                }

                return new PlanWithProperties(result, ActualProperties.undistributed());
            }

            // hash partition the sources
            List<Symbol> hashingColumns = context.getPreferredProperties().getGlobalProperties().get().getPartitioningProperties().get().getHashingOrder().get();

            ImmutableList.Builder<PlanNode> partitionedSources = ImmutableList.builder();
            ImmutableListMultimap.Builder<Symbol, Symbol> outputToSourcesMapping = ImmutableListMultimap.builder();

            for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                ImmutableList.Builder<Symbol> hashColumnsBuilder = ImmutableList.builder();
                for (Symbol column : hashingColumns) {
                    hashColumnsBuilder.add(node.getSymbolMapping().get(column).get(sourceIndex));
                }
                List<Symbol> sourceHashColumns = hashColumnsBuilder.build();

                PlanWithProperties source = node.getSources().get(sourceIndex).accept(this, context.withPreferredProperties(PreferredProperties.hashPartitioned(sourceHashColumns)));
                if (!source.getProperties().isHashPartitionedOn(sourceHashColumns)) {
                    source = withDerivedProperties(
                            partitionedExchange(
                                    idAllocator.getNextId(),
                                    source.getNode(),
                                    Optional.of(sourceHashColumns),
                                    Optional.empty()),
                            source.getProperties());
                }
                partitionedSources.add(source.getNode());

                for (int column = 0; column < node.getOutputSymbols().size(); column++) {
                    outputToSourcesMapping.put(node.getOutputSymbols().get(column), node.sourceOutputLayout(sourceIndex).get(column));
                }
            }
            return new PlanWithProperties(new UnionNode(node.getId(), partitionedSources.build(), outputToSourcesMapping.build()), ActualProperties.hashPartitioned(hashingColumns));
        }

        private PlanWithProperties planChild(PlanNode node, Context context)
        {
            return getOnlyElement(node.getSources()).accept(this, context);
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, PlanWithProperties child)
        {
            return withDerivedProperties(
                    ChildReplacer.replaceChildren(node, ImmutableList.of(child.getNode())),
                    child.getProperties());
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, List<PlanWithProperties> children)
        {
            PlanNode result = ChildReplacer.replaceChildren(node, children.stream().map(PlanWithProperties::getNode).collect(toList()));
            return new PlanWithProperties(result, deriveProperties(result, children.stream().map(PlanWithProperties::getProperties).collect(toList())));
        }

        private PlanWithProperties withDerivedProperties(PlanNode node, ActualProperties inputProperties)
        {
            return new PlanWithProperties(node, deriveProperties(node, inputProperties));
        }

        private ActualProperties deriveProperties(PlanNode result, ActualProperties inputProperties)
        {
            return PropertyDerivations.deriveProperties(result, inputProperties, metadata, session, symbolAllocator.getTypes(), parser);
        }

        private ActualProperties deriveProperties(PlanNode result, List<ActualProperties> inputProperties)
        {
            return PropertyDerivations.deriveProperties(result, inputProperties, metadata, session, symbolAllocator.getTypes(), parser);
        }
    }

    private static Map<Symbol, Symbol> computeIdentityTranslations(Map<Symbol, Expression> assignments)
    {
        Map<Symbol, Symbol> outputToInput = new HashMap<>();
        for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
            if (assignment.getValue() instanceof QualifiedNameReference) {
                outputToInput.put(assignment.getKey(), Symbol.fromQualifiedName(((QualifiedNameReference) assignment.getValue()).getName()));
            }
        }
        return outputToInput;
    }

    @VisibleForTesting
    static Comparator<ActualProperties> streamingExecutionPreference(PreferredProperties preferred)
    {
        // Calculating the matches can be a bit expensive, so cache the results between comparisons
        LoadingCache<List<LocalProperty<Symbol>>, List<Optional<LocalProperty<Symbol>>>> matchCache = CacheBuilder.newBuilder()
                .build(new CacheLoader<List<LocalProperty<Symbol>>, List<Optional<LocalProperty<Symbol>>>>()
                {
                    @Override
                    public List<Optional<LocalProperty<Symbol>>> load(List<LocalProperty<Symbol>> actualProperties)
                    {
                        return LocalProperties.match(actualProperties, preferred.getLocalProperties());
                    }
                });

        return (actual1, actual2) -> {
            List<Optional<LocalProperty<Symbol>>> matchLayout1 = matchCache.getUnchecked(actual1.getLocalProperties());
            List<Optional<LocalProperty<Symbol>>> matchLayout2 = matchCache.getUnchecked(actual2.getLocalProperties());

            return ComparisonChain.start()
                    .compareTrueFirst(hasLocalOptimization(preferred.getLocalProperties(), matchLayout1), hasLocalOptimization(preferred.getLocalProperties(), matchLayout2))
                    .compareTrueFirst(meetsPartitioningRequirements(preferred, actual1), meetsPartitioningRequirements(preferred, actual2))
                    .compare(matchLayout1, matchLayout2, matchedLayoutPreference())
                    .result();
        };
    }

    private static <T> boolean hasLocalOptimization(List<LocalProperty<T>> desiredLayout, List<Optional<LocalProperty<T>>> matchResult)
    {
        checkArgument(desiredLayout.size() == matchResult.size());
        if (matchResult.isEmpty()) {
            return false;
        }
        // Optimizations can be applied if the first LocalProperty has been modified in the match in any way
        return !matchResult.get(0).equals(Optional.of(desiredLayout.get(0)));
    }

    private static boolean meetsPartitioningRequirements(PreferredProperties preferred, ActualProperties actual)
    {
        if (!preferred.getGlobalProperties().isPresent()) {
            return true;
        }
        PreferredProperties.Global preferredGlobal = preferred.getGlobalProperties().get();
        if (!preferredGlobal.isDistributed()) {
            return !actual.isDistributed();
        }
        if (!preferredGlobal.getPartitioningProperties().isPresent()) {
            return actual.isDistributed();
        }
        return actual.isPartitionedOn(preferredGlobal.getPartitioningProperties().get().getPartitioningColumns());
    }

    // Prefer the match result that satisfied the most requirements
    private static <T> Comparator<List<Optional<LocalProperty<T>>>> matchedLayoutPreference()
    {
        return (matchLayout1, matchLayout2) -> {
            Iterator<Optional<LocalProperty<T>>> match1Iterator = matchLayout1.iterator();
            Iterator<Optional<LocalProperty<T>>> match2Iterator = matchLayout2.iterator();
            while (match1Iterator.hasNext() && match2Iterator.hasNext()) {
                Optional<LocalProperty<T>> match1 = match1Iterator.next();
                Optional<LocalProperty<T>> match2 = match2Iterator.next();
                if (match1.isPresent() && match2.isPresent()) {
                    return Integer.compare(match1.get().getColumns().size(), match2.get().getColumns().size());
                }
                else if (match1.isPresent()) {
                    return 1;
                }
                else if (match2.isPresent()) {
                    return -1;
                }
            }
            checkState(!match1Iterator.hasNext() && !match2Iterator.hasNext()); // Should be the same size
            return 0;
        };
    }

    @VisibleForTesting
    static class PlanWithProperties
    {
        private final PlanNode node;
        private final ActualProperties properties;

        public PlanWithProperties(PlanNode node, ActualProperties properties)
        {
            this.node = node;
            this.properties = properties;
        }

        public PlanNode getNode()
        {
            return node;
        }

        public ActualProperties getProperties()
        {
            return properties;
        }
    }
}
