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
package com.facebook.presto.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

@Immutable
public class PipelineStats
{
    private final boolean inputPipeline;
    private final boolean outputPipeline;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int queuedPartitionedDrivers;
    private final int runningDrivers;
    private final int runningPartitionedDrivers;
    private final int completedDrivers;

    private final DataSize memoryReservation;

    private final DistributionSnapshot queuedTime;
    private final DistributionSnapshot elapsedTime;

    private final Duration totalScheduledTime;
    private final Duration totalCpuTime;
    private final Duration totalUserTime;
    private final Duration totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final DataSize rawInputDataSize;
    private final long rawInputPositions;

    private final DataSize processedInputDataSize;
    private final long processedInputPositions;

    private final DataSize outputDataSize;
    private final long outputPositions;

    private final List<OperatorStats> operatorSummaries;
    private final List<DriverStats> drivers;

    @JsonCreator
    public PipelineStats(
            @JsonProperty("inputPipeline") boolean inputPipeline,
            @JsonProperty("outputPipeline") boolean outputPipeline,

            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("queuedPartitionedDrivers") int queuedPartitionedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("runningPartitionedDrivers") int runningPartitionedDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,

            @JsonProperty("memoryReservation") DataSize memoryReservation,

            @JsonProperty("queuedTime") DistributionSnapshot queuedTime,
            @JsonProperty("elapsedTime") DistributionSnapshot elapsedTime,

            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("totalUserTime") Duration totalUserTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,

            @JsonProperty("processedInputDataSize") DataSize processedInputDataSize,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("operatorSummaries") List<OperatorStats> operatorSummaries,
            @JsonProperty("drivers") List<DriverStats> drivers)
    {
        this.inputPipeline = inputPipeline;
        this.outputPipeline = outputPipeline;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(queuedPartitionedDrivers >= 0, "queuedPartitionedDrivers is negative");
        this.queuedPartitionedDrivers = queuedPartitionedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(runningPartitionedDrivers >= 0, "runningPartitionedDrivers is negative");
        this.runningPartitionedDrivers = runningPartitionedDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;

        this.memoryReservation = checkNotNull(memoryReservation, "memoryReservation is null");

        this.queuedTime = checkNotNull(queuedTime, "queuedTime is null");
        this.elapsedTime = checkNotNull(elapsedTime, "elapsedTime is null");
        this.totalScheduledTime = checkNotNull(totalScheduledTime, "totalScheduledTime is null");

        this.totalCpuTime = checkNotNull(totalCpuTime, "totalCpuTime is null");
        this.totalUserTime = checkNotNull(totalUserTime, "totalUserTime is null");
        this.totalBlockedTime = checkNotNull(totalBlockedTime, "totalBlockedTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.rawInputDataSize = checkNotNull(rawInputDataSize, "rawInputDataSize is null");
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;

        this.processedInputDataSize = checkNotNull(processedInputDataSize, "processedInputDataSize is null");
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        this.outputDataSize = checkNotNull(outputDataSize, "outputDataSize is null");
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.operatorSummaries = ImmutableList.copyOf(checkNotNull(operatorSummaries, "operatorSummaries is null"));
        this.drivers = ImmutableList.copyOf(checkNotNull(drivers, "drivers is null"));
    }

    @JsonProperty
    public boolean isInputPipeline()
    {
        return inputPipeline;
    }

    @JsonProperty
    public boolean isOutputPipeline()
    {
        return outputPipeline;
    }

    @JsonProperty
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    @JsonProperty
    public int getQueuedPartitionedDrivers()
    {
        return queuedPartitionedDrivers;
    }

    @JsonProperty
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @JsonProperty
    public int getRunningPartitionedDrivers()
    {
        return runningPartitionedDrivers;
    }

    @JsonProperty
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @JsonProperty
    public DataSize getMemoryReservation()
    {
        return memoryReservation;
    }

    @JsonProperty
    public DistributionSnapshot getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public DistributionSnapshot getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @JsonProperty
    public Duration getTotalUserTime()
    {
        return totalUserTime;
    }

    @JsonProperty
    public Duration getTotalBlockedTime()
    {
        return totalBlockedTime;
    }

    @JsonProperty
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    @JsonProperty
    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    public DataSize getProcessedInputDataSize()
    {
        return processedInputDataSize;
    }

    @JsonProperty
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    public DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    public List<OperatorStats> getOperatorSummaries()
    {
        return operatorSummaries;
    }

    @JsonProperty
    public List<DriverStats> getDrivers()
    {
        return drivers;
    }

    public PipelineStats summarize()
    {
        return new PipelineStats(
                inputPipeline,
                outputPipeline,
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                runningDrivers,
                runningPartitionedDrivers,
                completedDrivers,
                memoryReservation,
                queuedTime,
                elapsedTime,
                totalScheduledTime,
                totalCpuTime,
                totalUserTime,
                totalBlockedTime,
                fullyBlocked,
                blockedReasons,
                rawInputDataSize,
                rawInputPositions,
                processedInputDataSize,
                processedInputPositions,
                outputDataSize,
                outputPositions,
                operatorSummaries,
                ImmutableList.<DriverStats>of());
    }
}
