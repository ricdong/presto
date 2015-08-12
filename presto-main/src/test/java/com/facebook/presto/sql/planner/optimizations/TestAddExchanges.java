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

import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.distributed;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.undistributed;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Partitioning.hashPartitioned;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Partitioning.partitioned;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Partitioning.singlePartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.builder;
import static com.facebook.presto.sql.planner.optimizations.AddExchanges.streamingExecutionPreference;

public class TestAddExchanges
{
    @Test
    public void testPickLayoutAnyPreference()
            throws Exception
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference(PreferredProperties.any());

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();
        // Given no preferences, the original input order should be maintained
        Assert.assertEquals(stableSort(input, preference), input);
    }

    @Test
    public void testPickLayoutPartitionedPreference()
            throws Exception
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference(PreferredProperties.distributed());

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();
        Assert.assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutUnpartitionedPreference()
            throws Exception
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference(PreferredProperties.undistributed());

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .build();
        Assert.assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutPartitionedOnSingle()
            throws Exception
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference(
                PreferredProperties.partitioned(ImmutableSet.of(symbol("a"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .build();
        Assert.assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutPartitionedOnMultiple()
            throws Exception
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference(
                PreferredProperties.partitioned(ImmutableSet.of(symbol("a"), symbol("b"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .build();
        Assert.assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutGrouped()
            throws Exception
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.local(ImmutableList.of(grouped("a"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();
        Assert.assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutGroupedMultiple()
            throws Exception
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.local(ImmutableList.of(grouped("a", "b"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();
        Assert.assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutGroupedMultipleProperties()
            throws Exception
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.local(ImmutableList.of(grouped("a"), grouped("b"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();
        Assert.assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutGroupedWithSort()
            throws Exception
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.local(ImmutableList.of(grouped("a"), sorted("b", ASC_NULLS_FIRST))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();
        Assert.assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutUnpartitionedWithGroupAndSort()
            throws Exception
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.undistributedWithLocal(ImmutableList.of(grouped("a"), sorted("b", ASC_NULLS_FIRST))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();
        Assert.assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutPartitionedWithGroup()
            throws Exception
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.partitionedWithLocal(
                        ImmutableSet.of(symbol("a")),
                        ImmutableList.of(grouped("a"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(distributed(singlePartition()))
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(distributed(partitioned(ImmutableSet.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(undistributed())
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed(hashPartitioned(ImmutableList.of(symbol("a")))))
                        .build())
                .add(builder()
                        .global(distributed())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(distributed())
                        .build())
                .build();
        Assert.assertEquals(stableSort(input, preference), expected);
    }

    private static <T> List<T> stableSort(List<T> list, Comparator<T> comparator)
    {
        ArrayList<T> copy = Lists.newArrayList(list);
        Collections.sort(copy, comparator);
        return copy;
    }

    private static ConstantProperty<Symbol> constant(String column)
    {
        return new ConstantProperty<>(symbol(column));
    }

    private static GroupingProperty<Symbol> grouped(String... columns)
    {
        return new GroupingProperty<>(Lists.transform(Arrays.asList(columns), Symbol::new));
    }

    private static SortingProperty<Symbol> sorted(String column, SortOrder order)
    {
        return new SortingProperty<>(symbol(column), order);
    }

    private static Symbol symbol(String name)
    {
        return new Symbol(name);
    }
}
