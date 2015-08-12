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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public class StageInfo
{
    private final StageId stageId;
    private final StageState state;
    private final URI self;
    private final PlanFragment plan;
    private final List<Type> types;
    private final StageStats stageStats;
    private final List<TaskInfo> tasks;
    private final List<StageInfo> subStages;
    private final ExecutionFailureInfo failureCause;

    @JsonCreator
    public StageInfo(
            @JsonProperty("stageId") StageId stageId,
            @JsonProperty("state") StageState state,
            @JsonProperty("self") URI self,
            @JsonProperty("plan") @Nullable PlanFragment plan,
            @JsonProperty("types") List<Type> types,
            @JsonProperty("stageStats") StageStats stageStats,
            @JsonProperty("tasks") List<TaskInfo> tasks,
            @JsonProperty("subStages") List<StageInfo> subStages,
            @JsonProperty("failureCause") ExecutionFailureInfo failureCause)
    {
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(state, "state is null");
        Preconditions.checkNotNull(self, "self is null");
        Preconditions.checkNotNull(stageStats, "stageStats is null");
        Preconditions.checkNotNull(tasks, "tasks is null");
        Preconditions.checkNotNull(subStages, "subStages is null");

        this.stageId = stageId;
        this.state = state;
        this.self = self;
        this.plan = plan;
        this.types = types;
        this.stageStats = stageStats;
        this.tasks = ImmutableList.copyOf(tasks);
        this.subStages = subStages;
        this.failureCause = failureCause;
    }

    @JsonProperty
    public StageId getStageId()
    {
        return stageId;
    }

    @JsonProperty
    public StageState getState()
    {
        return state;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    @Nullable
    public PlanFragment getPlan()
    {
        return plan;
    }

    @JsonProperty
    public List<Type> getTypes()
    {
        return types;
    }

    @JsonProperty
    public StageStats getStageStats()
    {
        return stageStats;
    }

    @JsonProperty
    public List<TaskInfo> getTasks()
    {
        return tasks;
    }

    @JsonProperty
    public List<StageInfo> getSubStages()
    {
        return subStages;
    }

    @JsonProperty
    public ExecutionFailureInfo getFailureCause()
    {
        return failureCause;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stageId", stageId)
                .add("state", state)
                .toString();
    }

    public static List<StageInfo> getAllStages(StageInfo stageInfo)
    {
        ImmutableList.Builder<StageInfo> collector = ImmutableList.builder();
        if (stageInfo != null) {
            addAllStages(stageInfo, collector);
        }
        return collector.build();
    }

    private static void addAllStages(StageInfo stageInfo, ImmutableList.Builder<StageInfo> collector)
    {
        collector.add(stageInfo);
        for (StageInfo subStage : stageInfo.getSubStages()) {
            addAllStages(subStage, collector);
        }
    }
}
