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

import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.AddExchanges;
import com.facebook.presto.sql.planner.optimizations.BeginTableWrite;
import com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions;
import com.facebook.presto.sql.planner.optimizations.CountConstantOptimizer;
import com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer;
import com.facebook.presto.sql.planner.optimizations.ImplementSampleAsFilter;
import com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer;
import com.facebook.presto.sql.planner.optimizations.LimitPushDown;
import com.facebook.presto.sql.planner.optimizations.MergeProjections;
import com.facebook.presto.sql.planner.optimizations.MetadataQueryOptimizer;
import com.facebook.presto.sql.planner.optimizations.PickLayout;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PredicatePushDown;
import com.facebook.presto.sql.planner.optimizations.PruneRedundantProjections;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.SetFlatteningOptimizer;
import com.facebook.presto.sql.planner.optimizations.SimplifyExpressions;
import com.facebook.presto.sql.planner.optimizations.SingleDistinctOptimizer;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.sql.planner.optimizations.WindowFilterPushDown;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import javax.inject.Provider;

import java.util.List;

public class PlanOptimizersFactory
        implements Provider<List<PlanOptimizer>>
{
    private final List<PlanOptimizer> optimizers;

    @Inject
    public PlanOptimizersFactory(Metadata metadata, SqlParser sqlParser, IndexManager indexManager, FeaturesConfig featuresConfig)
    {
        this(metadata, sqlParser, indexManager, featuresConfig, false);
    }

    public PlanOptimizersFactory(Metadata metadata, SqlParser sqlParser, IndexManager indexManager, FeaturesConfig featuresConfig, boolean forceSingleNode)
    {
        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        builder.add(new ImplementSampleAsFilter(),
                new CanonicalizeExpressions(),
                new SimplifyExpressions(metadata, sqlParser),
                new UnaliasSymbolReferences(),
                new PruneRedundantProjections(),
                new SetFlatteningOptimizer(),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                new PredicatePushDown(metadata, sqlParser),
                new MergeProjections(),
                new SimplifyExpressions(metadata, sqlParser), // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                new ProjectionPushDown(),
                new UnaliasSymbolReferences(), // Run again because predicate pushdown and projection pushdown might add more projections
                new PruneUnreferencedOutputs(), // Make sure to run this before index join. Filtered projections may not have all the columns.
                new IndexJoinOptimizer(metadata, indexManager), // Run this after projections and filters have been fully simplified and pushed down
                new CountConstantOptimizer(),
                new WindowFilterPushDown(metadata), // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                new HashGenerationOptimizer(), // This must run after all other optimizers have run to that all the PlanNodes are created
                new MergeProjections(),
                new PruneUnreferencedOutputs(), // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new PruneRedundantProjections()); // This MUST run after PruneUnreferencedOutputs as it may introduce new redundant projections

        if (featuresConfig.isOptimizeMetadataQueries()) {
            builder.add(new MetadataQueryOptimizer(metadata));
        }

        if (featuresConfig.isOptimizeSingleDistinct()) {
            builder.add(new SingleDistinctOptimizer());
            builder.add(new PruneUnreferencedOutputs());
        }

        builder.add(new BeginTableWrite(metadata)); // HACK! see comments in BeginTableWrite

        if (!forceSingleNode) {
            builder.add(new AddExchanges(metadata, sqlParser, featuresConfig.isDistributedIndexJoinsEnabled()));
        }

        builder.add(new PickLayout(metadata));

        builder.add(new PredicatePushDown(metadata, sqlParser)); // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        builder.add(new UnaliasSymbolReferences());
        builder.add(new MergeProjections());
        builder.add(new PruneUnreferencedOutputs());
        builder.add(new PruneRedundantProjections());

        // TODO: consider adding a formal final plan sanitization optimizer that prepares the plan for transmission/execution/logging
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point

        this.optimizers = builder.build();
    }

    @Override
    public synchronized List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
