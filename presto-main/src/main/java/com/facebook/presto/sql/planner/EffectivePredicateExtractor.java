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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.expressionOrNullSymbols;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripNonDeterministicConjuncts;
import static com.facebook.presto.sql.planner.EqualityInference.createEqualityInference;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.transform;

/**
 * Computes the effective predicate at the top of the specified PlanNode
 * <p>
 * Note: non-deterministic predicates can not be pulled up (so they will be ignored)
 */
public class EffectivePredicateExtractor
        extends PlanVisitor<Void, Expression>
{
    public static Expression extract(PlanNode node, Map<Symbol, Type> symbolTypes)
    {
        return node.accept(new EffectivePredicateExtractor(symbolTypes), null);
    }

    private final Map<Symbol, Type> symbolTypes;

    public EffectivePredicateExtractor(Map<Symbol, Type> symbolTypes)
    {
        this.symbolTypes = symbolTypes;
    }

    @Override
    protected Expression visitPlan(PlanNode node, Void context)
    {
        return BooleanLiteral.TRUE_LITERAL;
    }

    @Override
    public Expression visitAggregation(AggregationNode node, Void context)
    {
        Expression underlyingPredicate = node.getSource().accept(this, context);

        return pullExpressionThroughSymbols(underlyingPredicate, node.getGroupBy());
    }

    @Override
    public Expression visitFilter(FilterNode node, Void context)
    {
        Expression underlyingPredicate = node.getSource().accept(this, context);

        Expression predicate = node.getPredicate();

        // Remove non-deterministic conjuncts
        predicate = stripNonDeterministicConjuncts(predicate);

        return combineConjuncts(predicate, underlyingPredicate);
    }

    private static Predicate<Map.Entry<Symbol, ? extends Expression>> symbolMatchesExpression()
    {
        return entry -> entry.getValue().equals(new QualifiedNameReference(entry.getKey().toQualifiedName()));
    }

    private static Function<Map.Entry<Symbol, ? extends Expression>, Expression> entryToEquality()
    {
        return entry -> {
            QualifiedNameReference reference = new QualifiedNameReference(entry.getKey().toQualifiedName());
            Expression expression = entry.getValue();
            // TODO: switch this to 'IS NOT DISTINCT FROM' syntax when EqualityInference properly supports it
            return new ComparisonExpression(ComparisonExpression.Type.EQUAL, reference, expression);
        };
    }

    @Override
    public Expression visitExchange(ExchangeNode node, Void context)
    {
        return deriveCommonPredicates(node, source -> {
            Map<Symbol, QualifiedNameReference> mappings = new HashMap<>();
            for (int i = 0; i < node.getInputs().get(source).size(); i++) {
                mappings.put(
                        node.getOutputSymbols().get(i),
                        node.getInputs().get(source).get(i).toQualifiedNameReference());
            }
            return mappings.entrySet();
        });
    }

    @Override
    public Expression visitProject(ProjectNode node, Void context)
    {
        // TODO: add simple algebraic solver for projection translation (right now only considers identity projections)

        Expression underlyingPredicate = node.getSource().accept(this, context);

        Iterable<Expression> projectionEqualities = FluentIterable.from(node.getAssignments().entrySet())
                .filter(not(symbolMatchesExpression()))
                .transform(entryToEquality());

        return pullExpressionThroughSymbols(combineConjuncts(
                        ImmutableList.<Expression>builder()
                                .addAll(projectionEqualities)
                                .add(underlyingPredicate)
                                .build()),
                node.getOutputSymbols());
    }

    @Override
    public Expression visitTopN(TopNNode node, Void context)
    {
        return node.getSource().accept(this, context);
    }

    @Override
    public Expression visitLimit(LimitNode node, Void context)
    {
        return node.getSource().accept(this, context);
    }

    @Override
    public Expression visitDistinctLimit(DistinctLimitNode node, Void context)
    {
        return node.getSource().accept(this, context);
    }

    @Override
    public Expression visitTableScan(TableScanNode node, Void context)
    {
        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
        return DomainTranslator.toPredicate(
                spanTupleDomain(node.getCurrentConstraint()).transform(assignments::get),
                symbolTypes);
    }

    private static TupleDomain<ColumnHandle> spanTupleDomain(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return tupleDomain;
        }

        // Retain nullability, but collapse each SortedRangeSet into a single span
        Map<ColumnHandle, Domain> spannedDomains = Maps.transformValues(tupleDomain.getDomains(), domain -> Domain.create(getSortedRangeSpan(domain.getRanges()), domain.isNullAllowed()));

        return TupleDomain.withColumnDomains(spannedDomains);
    }

    private static SortedRangeSet getSortedRangeSpan(SortedRangeSet rangeSet)
    {
        return rangeSet.isNone() ? SortedRangeSet.none(rangeSet.getType()) : SortedRangeSet.of(rangeSet.getSpan());
    }

    @Override
    public Expression visitSort(SortNode node, Void context)
    {
        return node.getSource().accept(this, context);
    }

    @Override
    public Expression visitWindow(WindowNode node, Void context)
    {
        return node.getSource().accept(this, context);
    }

    @Override
    public Expression visitUnion(UnionNode node, Void context)
    {
        return deriveCommonPredicates(node, source -> node.outputSymbolMap(source).entries());
    }

    @Override
    public Expression visitJoin(JoinNode node, Void context)
    {
        Expression leftPredicate = node.getLeft().accept(this, context);
        Expression rightPredicate = node.getRight().accept(this, context);

        List<Expression> joinConjuncts = new ArrayList<>();
        for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
            joinConjuncts.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                    new QualifiedNameReference(clause.getLeft().toQualifiedName()),
                    new QualifiedNameReference(clause.getRight().toQualifiedName())));
        }

        switch (node.getType()) {
            case INNER:
            case CROSS:
                return combineConjuncts(ImmutableList.<Expression>builder()
                        .add(leftPredicate)
                        .add(rightPredicate)
                        .addAll(joinConjuncts)
                        .build());
            case LEFT:
                return combineConjuncts(ImmutableList.<Expression>builder()
                        .add(leftPredicate)
                        .addAll(transform(extractConjuncts(rightPredicate), expressionOrNullSymbols(in(node.getRight().getOutputSymbols()))))
                        .addAll(transform(joinConjuncts, expressionOrNullSymbols(in(node.getRight().getOutputSymbols()))))
                        .build());
            case RIGHT:
                return combineConjuncts(ImmutableList.<Expression>builder()
                        .add(rightPredicate)
                        .addAll(transform(extractConjuncts(leftPredicate), expressionOrNullSymbols(in(node.getLeft().getOutputSymbols()))))
                        .addAll(transform(joinConjuncts, expressionOrNullSymbols(in(node.getLeft().getOutputSymbols()))))
                        .build());
            case FULL:
                return combineConjuncts(ImmutableList.<Expression>builder()
                        .addAll(transform(extractConjuncts(leftPredicate), expressionOrNullSymbols(in(node.getLeft().getOutputSymbols()))))
                        .addAll(transform(extractConjuncts(rightPredicate), expressionOrNullSymbols(in(node.getRight().getOutputSymbols()))))
                        .addAll(transform(joinConjuncts, expressionOrNullSymbols(in(node.getLeft().getOutputSymbols()), in(node.getRight().getOutputSymbols()))))
                        .build());
            default:
                throw new UnsupportedOperationException("Unknown join type: " + node.getType());
        }
    }

    @Override
    public Expression visitSemiJoin(SemiJoinNode node, Void context)
    {
        // Filtering source does not change the effective predicate over the output symbols
        return node.getSource().accept(this, context);
    }

    private Expression deriveCommonPredicates(PlanNode node, Function<Integer, Collection<Map.Entry<Symbol, QualifiedNameReference>>> mapping)
    {
        // Find the predicates that can be pulled up from each source
        List<Set<Expression>> sourceOutputConjuncts = new ArrayList<>();
        for (int i = 0; i < node.getSources().size(); i++) {
            Expression underlyingPredicate = node.getSources().get(i).accept(this, null);

            Iterable<Expression> equalities = FluentIterable.from(mapping.apply(i))
                    .filter(not(symbolMatchesExpression()))
                    .transform(entryToEquality());

            sourceOutputConjuncts.add(ImmutableSet.copyOf(extractConjuncts(pullExpressionThroughSymbols(combineConjuncts(
                            ImmutableList.<Expression>builder()
                                    .addAll(equalities)
                                    .add(underlyingPredicate)
                                    .build()),
                    node.getOutputSymbols()))));
        }

        // Find the intersection of predicates across all sources
        // TODO: use a more precise way to determine overlapping conjuncts (e.g. commutative predicates)
        Iterator<Set<Expression>> iterator = sourceOutputConjuncts.iterator();
        Set<Expression> potentialOutputConjuncts = iterator.next();
        while (iterator.hasNext()) {
            potentialOutputConjuncts = Sets.intersection(potentialOutputConjuncts, iterator.next());
        }

        return combineConjuncts(potentialOutputConjuncts);
    }

    private static Expression pullExpressionThroughSymbols(Expression expression, Collection<Symbol> symbols)
    {
        EqualityInference equalityInference = createEqualityInference(expression);

        ImmutableList.Builder<Expression> effectiveConjuncts = ImmutableList.builder();
        for (Expression conjunct : EqualityInference.nonInferrableConjuncts(expression)) {
            if (DeterminismEvaluator.isDeterministic(conjunct)) {
                Expression rewritten = equalityInference.rewriteExpression(conjunct, in(symbols));
                if (rewritten != null) {
                    effectiveConjuncts.add(rewritten);
                }
            }
        }

        effectiveConjuncts.addAll(equalityInference.generateEqualitiesPartitionedBy(in(symbols)).getScopeEqualities());

        return combineConjuncts(effectiveConjuncts.build());
    }
}
