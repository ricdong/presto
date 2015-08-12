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
package com.facebook.presto.execution;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

public class TestNodeSchedulerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(NodeSchedulerConfig.class)
                .setMinCandidates(10)
                .setMaxSplitsPerNode(100)
                .setMaxPendingSplitsPerNodePerTask(10)
                .setIncludeCoordinator(true)
                .setLocationAwareSchedulingEnabled(true)
                .setMultipleTasksPerNodeEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("node-scheduler.min-candidates", "11")
                .put("node-scheduler.location-aware-scheduling-enabled", "false")
                .put("node-scheduler.include-coordinator", "false")
                .put("node-scheduler.max-pending-splits-per-node-per-task", "11")
                .put("node-scheduler.max-splits-per-node", "101")
                .put("node-scheduler.multiple-tasks-per-node-enabled", "true")
                .build();

        NodeSchedulerConfig expected = new NodeSchedulerConfig()
                .setIncludeCoordinator(false)
                .setLocationAwareSchedulingEnabled(false)
                .setMultipleTasksPerNodeEnabled(true)
                .setMaxSplitsPerNode(101)
                .setMaxPendingSplitsPerNodePerTask(11)
                .setMinCandidates(11);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
