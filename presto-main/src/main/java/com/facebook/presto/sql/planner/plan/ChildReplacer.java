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
package com.facebook.presto.sql.planner.plan;

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

import com.google.common.collect.Iterables;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ChildReplacer
        extends PlanVisitor<List<PlanNode>, PlanNode>
{
    private static final ChildReplacer INSTANCE = new ChildReplacer();

    public static PlanNode replaceChildren(PlanNode node, List<PlanNode> children)
    {
        return node.accept(INSTANCE, children);
    }

    @Override
    public PlanNode visitPlan(PlanNode node, List<PlanNode> newChildren)
    {
        throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
    }

    @Override
    public PlanNode visitLimit(LimitNode node, List<PlanNode> newChildren)
    {
        return new LimitNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getCount());
    }

    @Override
    public PlanNode visitDistinctLimit(DistinctLimitNode node, List<PlanNode> newChildren)
    {
        return new DistinctLimitNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getLimit(), node.getHashSymbol());
    }

    @Override
    public PlanNode visitRemoteSource(RemoteSourceNode node, List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return node;
    }

    @Override
    public PlanNode visitExchange(ExchangeNode node, List<PlanNode> newChildren)
    {
        return new ExchangeNode(
                node.getId(),
                node.getType(),
                node.getPartitionKeys(),
                node.getHashSymbol(),
                newChildren,
                node.getOutputSymbols(),
                node.getInputs());
    }

    @Override
    public PlanNode visitTopN(TopNNode node, List<PlanNode> newChildren)
    {
        return new TopNNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getCount(), node.getOrderBy(), node.getOrderings(), node.isPartial());
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return node;
    }

    @Override
    public PlanNode visitValues(ValuesNode node, List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return node;
    }

    @Override
    public PlanNode visitUnnest(UnnestNode node, List<PlanNode> newChildren)
    {
        return new UnnestNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getReplicateSymbols(), node.getUnnestSymbols(), node.getOrdinalitySymbol());
    }

    @Override
    public PlanNode visitProject(ProjectNode node, List<PlanNode> newChildren)
    {
        return new ProjectNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getAssignments());
    }

    @Override
    public PlanNode visitFilter(FilterNode node, List<PlanNode> newChildren)
    {
        return new FilterNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getPredicate());
    }

    @Override
    public PlanNode visitSample(SampleNode node, List<PlanNode> newChildren)
    {
        return new SampleNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getSampleRatio(), node.getSampleType(), node.isRescaled(), node.getSampleWeightSymbol());
    }

    @Override
    public PlanNode visitIndexSource(IndexSourceNode node, List<PlanNode> newChildren)
    {
        return node;
    }

    @Override
    public PlanNode visitJoin(JoinNode node, List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new JoinNode(node.getId(), node.getType(), newChildren.get(0), newChildren.get(1), node.getCriteria(), node.getLeftHashSymbol(), node.getRightHashSymbol());
    }

    @Override
    public PlanNode visitSemiJoin(SemiJoinNode node, List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new SemiJoinNode(node.getId(), newChildren.get(0), newChildren.get(1), node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol(), node.getSemiJoinOutput(), node.getSourceHashSymbol(), node.getFilteringSourceHashSymbol());
    }

    @Override
    public PlanNode visitIndexJoin(IndexJoinNode node, List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new IndexJoinNode(node.getId(), node.getType(), newChildren.get(0), newChildren.get(1), node.getCriteria(), node.getProbeHashSymbol(), node.getIndexHashSymbol());
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, List<PlanNode> newChildren)
    {
        return new AggregationNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getGroupBy(), node.getAggregations(), node.getFunctions(), node.getMasks(), node.getStep(), node.getSampleWeight(), node.getConfidence(), node.getHashSymbol());
    }

    @Override
    public PlanNode visitMarkDistinct(MarkDistinctNode node, List<PlanNode> newChildren)
    {
        return new MarkDistinctNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getMarkerSymbol(), node.getDistinctSymbols(), node.getHashSymbol());
    }

    @Override
    public PlanNode visitWindow(WindowNode node, List<PlanNode> newChildren)
    {
        return new WindowNode(
                node.getId(),
                Iterables.getOnlyElement(newChildren),
                node.getPartitionBy(),
                node.getOrderBy(),
                node.getOrderings(),
                node.getFrame(),
                node.getWindowFunctions(),
                node.getSignatures(),
                node.getHashSymbol(),
                node.getPrePartitionedInputs(),
                node.getPreSortedOrderPrefix());
    }

    @Override
    public PlanNode visitTopNRowNumber(TopNRowNumberNode node, List<PlanNode> newChildren)
    {
        return new TopNRowNumberNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getPartitionBy(), node.getOrderBy(), node.getOrderings(), node.getRowNumberSymbol(), node.getMaxRowCountPerPartition(), node.isPartial(), node.getHashSymbol());
    }

    @Override
    public PlanNode visitRowNumber(RowNumberNode node, List<PlanNode> newChildren)
    {
        return new RowNumberNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getPartitionBy(), node.getRowNumberSymbol(), node.getMaxRowCountPerPartition(), node.getHashSymbol());
    }

    @Override
    public PlanNode visitOutput(OutputNode node, List<PlanNode> newChildren)
    {
        return new OutputNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getColumnNames(), node.getOutputSymbols());
    }

    @Override
    public PlanNode visitSort(SortNode node, List<PlanNode> newChildren)
    {
        return new SortNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getOrderBy(), node.getOrderings());
    }

    @Override
    public PlanNode visitTableWriter(TableWriterNode node, List<PlanNode> newChildren)
    {
        return new TableWriterNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getTarget(), node.getColumns(), node.getColumnNames(), node.getOutputSymbols(), node.getSampleWeightSymbol());
    }

    @Override
    public PlanNode visitTableCommit(TableCommitNode node, List<PlanNode> newChildren)
    {
        return new TableCommitNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getTarget(), node.getOutputSymbols());
    }

    @Override
    public PlanNode visitUnion(UnionNode node, List<PlanNode> newChildren)
    {
        return new UnionNode(node.getId(), newChildren, node.getSymbolMapping());
    }

    @Override
    public PlanNode visitDelete(DeleteNode node, List<PlanNode> newChildren)
    {
        return new DeleteNode(node.getId(), Iterables.getOnlyElement(newChildren), node.getTarget(), node.getRowId(), node.getOutputSymbols());
    }
}
