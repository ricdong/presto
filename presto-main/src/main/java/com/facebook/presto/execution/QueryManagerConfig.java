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

import io.airlift.configuration.Config;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

@DefunctConfig({"query.max-pending-splits-per-node",
                "experimental.big-query-initial-hash-partitions",
                "experimental.max-concurrent-big-queries",
                "experimental.max-queued-big-queries"})
public class QueryManagerConfig
{
    private int scheduleSplitBatchSize = 1000;
    private int maxConcurrentQueries = 1000;
    private int maxQueuedQueries = 5000;
    private String queueConfigFile;

    private int initialHashPartitions = 8;
    private Duration maxQueryAge = new Duration(15, TimeUnit.MINUTES);
    private int maxQueryHistory = 100;
    private Duration clientTimeout = new Duration(5, TimeUnit.MINUTES);

    private int queryManagerExecutorPoolSize = 5;

    private int remoteTaskMaxConsecutiveErrorCount = 10;
    private Duration remoteTaskMinErrorDuration = new Duration(2, TimeUnit.MINUTES);
    private int remoteTaskMaxCallbackThreads = 1000;

    public String getQueueConfigFile()
    {
        return queueConfigFile;
    }

    @Config("query.queue-config-file")
    public QueryManagerConfig setQueueConfigFile(String queueConfigFile)
    {
        this.queueConfigFile = queueConfigFile;
        return this;
    }

    @Min(1)
    public int getScheduleSplitBatchSize()
    {
        return scheduleSplitBatchSize;
    }

    @Config("query.schedule-split-batch-size")
    public QueryManagerConfig setScheduleSplitBatchSize(int scheduleSplitBatchSize)
    {
        this.scheduleSplitBatchSize = scheduleSplitBatchSize;
        return this;
    }

    @Deprecated
    @Min(1)
    public int getMaxConcurrentQueries()
    {
        return maxConcurrentQueries;
    }

    @Deprecated
    @Config("query.max-concurrent-queries")
    public QueryManagerConfig setMaxConcurrentQueries(int maxConcurrentQueries)
    {
        this.maxConcurrentQueries = maxConcurrentQueries;
        return this;
    }

    @Deprecated
    @Min(1)
    public int getMaxQueuedQueries()
    {
        return maxQueuedQueries;
    }

    @Deprecated
    @Config("query.max-queued-queries")
    public QueryManagerConfig setMaxQueuedQueries(int maxQueuedQueries)
    {
        this.maxQueuedQueries = maxQueuedQueries;
        return this;
    }

    @Min(1)
    public int getInitialHashPartitions()
    {
        return initialHashPartitions;
    }

    @Config("query.initial-hash-partitions")
    public QueryManagerConfig setInitialHashPartitions(int initialHashPartitions)
    {
        this.initialHashPartitions = initialHashPartitions;
        return this;
    }

    @NotNull
    public Duration getMaxQueryAge()
    {
        return maxQueryAge;
    }

    @Config("query.max-age")
    public QueryManagerConfig setMaxQueryAge(Duration maxQueryAge)
    {
        this.maxQueryAge = maxQueryAge;
        return this;
    }

    @Min(0)
    public int getMaxQueryHistory()
    {
        return maxQueryHistory;
    }

    @Config("query.max-history")
    public QueryManagerConfig setMaxQueryHistory(int maxQueryHistory)
    {
        this.maxQueryHistory = maxQueryHistory;
        return this;
    }

    @MinDuration("5s")
    @NotNull
    public Duration getClientTimeout()
    {
        return clientTimeout;
    }

    @Config("query.client.timeout")
    public QueryManagerConfig setClientTimeout(Duration clientTimeout)
    {
        this.clientTimeout = clientTimeout;
        return this;
    }

    @Min(1)
    public int getQueryManagerExecutorPoolSize()
    {
        return queryManagerExecutorPoolSize;
    }

    @Config("query.manager-executor-pool-size")
    public QueryManagerConfig setQueryManagerExecutorPoolSize(int queryManagerExecutorPoolSize)
    {
        this.queryManagerExecutorPoolSize = queryManagerExecutorPoolSize;
        return this;
    }

    @Min(0)
    public int getRemoteTaskMaxConsecutiveErrorCount()
    {
        return remoteTaskMaxConsecutiveErrorCount;
    }

    @Config("query.remote-task.max-consecutive-error-count")
    public QueryManagerConfig setRemoteTaskMaxConsecutiveErrorCount(int remoteTaskMaxConsecutiveErrorCount)
    {
        this.remoteTaskMaxConsecutiveErrorCount = remoteTaskMaxConsecutiveErrorCount;
        return this;
    }

    @NotNull
    public Duration getRemoteTaskMinErrorDuration()
    {
        return remoteTaskMinErrorDuration;
    }

    @Config("query.remote-task.min-error-duration")
    public QueryManagerConfig setRemoteTaskMinErrorDuration(Duration remoteTaskMinErrorDuration)
    {
        this.remoteTaskMinErrorDuration = remoteTaskMinErrorDuration;
        return this;
    }

    @Min(1)
    public int getRemoteTaskMaxCallbackThreads()
    {
        return remoteTaskMaxCallbackThreads;
    }

    @Config("query.remote-task.max-callback-threads")
    public QueryManagerConfig setRemoteTaskMaxCallbackThreads(int remoteTaskMaxCallbackThreads)
    {
        this.remoteTaskMaxCallbackThreads = remoteTaskMaxCallbackThreads;
        return this;
    }
}
