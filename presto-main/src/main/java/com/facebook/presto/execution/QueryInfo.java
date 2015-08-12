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

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.memory.MemoryPoolId;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.StandardErrorCode.ErrorType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.toErrorType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class QueryInfo
{
    private final QueryId queryId;
    private final SessionRepresentation session;
    private final QueryState state;
    private final MemoryPoolId memoryPool;
    private final boolean scheduled;
    private final URI self;
    private final List<String> fieldNames;
    private final String query;
    private final QueryStats queryStats;
    private final Map<String, String> setSessionProperties;
    private final Set<String> resetSessionProperties;
    private final String updateType;
    private final StageInfo outputStage;
    private final FailureInfo failureInfo;
    private final ErrorType errorType;
    private final ErrorCode errorCode;
    private final Set<Input> inputs;

    @JsonCreator
    public QueryInfo(
            @JsonProperty("queryId") QueryId queryId,
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("state") QueryState state,
            @JsonProperty("memoryPool") MemoryPoolId memoryPool,
            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("self") URI self,
            @JsonProperty("fieldNames") List<String> fieldNames,
            @JsonProperty("query") String query,
            @JsonProperty("queryStats") QueryStats queryStats,
            @JsonProperty("setSessionProperties") Map<String, String> setSessionProperties,
            @JsonProperty("resetSessionProperties") Set<String> resetSessionProperties,
            @JsonProperty("updateType") String updateType,
            @JsonProperty("outputStage") StageInfo outputStage,
            @JsonProperty("failureInfo") FailureInfo failureInfo,
            @JsonProperty("errorCode") ErrorCode errorCode,
            @JsonProperty("inputs") Set<Input> inputs)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(state, "state is null");
        Preconditions.checkNotNull(self, "self is null");
        Preconditions.checkNotNull(fieldNames, "fieldNames is null");
        Preconditions.checkNotNull(queryStats, "queryStats is null");
        Preconditions.checkNotNull(setSessionProperties, "setSessionProperties is null");
        Preconditions.checkNotNull(resetSessionProperties, "resetSessionProperties is null");
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkNotNull(inputs, "inputs is null");

        this.queryId = queryId;
        this.session = session;
        this.state = state;
        this.memoryPool = requireNonNull(memoryPool, "memoryPool is null");
        this.scheduled = scheduled;
        this.self = self;
        this.fieldNames = ImmutableList.copyOf(fieldNames);
        this.query = query;
        this.queryStats = queryStats;
        this.setSessionProperties = ImmutableMap.copyOf(setSessionProperties);
        this.resetSessionProperties = ImmutableSet.copyOf(resetSessionProperties);
        this.updateType = updateType;
        this.outputStage = outputStage;
        this.failureInfo = failureInfo;
        this.errorType = errorCode == null ? null : toErrorType(errorCode.getCode());
        this.errorCode = errorCode;
        this.inputs = ImmutableSet.copyOf(inputs);
    }

    @JsonProperty
    public QueryId getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public SessionRepresentation getSession()
    {
        return session;
    }

    @JsonProperty
    public QueryState getState()
    {
        return state;
    }

    @JsonProperty
    public MemoryPoolId getMemoryPool()
    {
        return memoryPool;
    }

    @JsonProperty
    public boolean isScheduled()
    {
        return scheduled;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    public List<String> getFieldNames()
    {
        return fieldNames;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public QueryStats getQueryStats()
    {
        return queryStats;
    }

    @JsonProperty
    public Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    @JsonProperty
    public Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    @Nullable
    @JsonProperty
    public String getUpdateType()
    {
        return updateType;
    }

    @JsonProperty
    public StageInfo getOutputStage()
    {
        return outputStage;
    }

    @Nullable
    @JsonProperty
    public FailureInfo getFailureInfo()
    {
        return failureInfo;
    }

    @Nullable
    @JsonProperty
    public ErrorType getErrorType()
    {
        return errorType;
    }

    @Nullable
    @JsonProperty
    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @JsonProperty
    public Set<Input> getInputs()
    {
        return inputs;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("state", state)
                .add("fieldNames", fieldNames)
                .toString();
    }
}
