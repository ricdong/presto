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

import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.UnpartitionedPagePartitionFunction;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.ReservedSystemMemoryConfig;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.spi.Node;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.event.client.NullEventClient;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.TaskTestUtils.PLAN_FRAGMENT;
import static com.facebook.presto.execution.TaskTestUtils.SPLIT;
import static com.facebook.presto.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static com.facebook.presto.execution.TaskTestUtils.createTestingPlanner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test
public class TestSqlTaskManager
{
    private static final TaskId TASK_ID = new TaskId("query", "stage", "task");
    public static final TaskId OUT = new TaskId("query", "stage", "out");

    private final TaskExecutor taskExecutor;
    private final LocalMemoryManager localMemoryManager;

    public TestSqlTaskManager()
    {
        localMemoryManager = new LocalMemoryManager(new MemoryManagerConfig(), new ReservedSystemMemoryConfig());
        taskExecutor = new TaskExecutor(8, 16);
        taskExecutor.start();
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        taskExecutor.stop();
    }

    @Test
    public void testEmptyQuery()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = sqlTaskManager.updateTask(TEST_SESSION,
                    taskId,
                    PLAN_FRAGMENT,
                    ImmutableList.<TaskSource>of(),
                    INITIAL_EMPTY_OUTPUT_BUFFERS);
            assertEquals(taskInfo.getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.updateTask(TEST_SESSION,
                    taskId,
                    PLAN_FRAGMENT,
                    ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.<ScheduledSplit>of(), true)),
                    INITIAL_EMPTY_OUTPUT_BUFFERS.withNoMoreBufferIds());
            assertEquals(taskInfo.getState(), TaskState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getState(), TaskState.FINISHED);
        }
    }

    @Test
    public void testSimpleQuery()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = sqlTaskManager.updateTask(TEST_SESSION,
                    taskId,
                    PLAN_FRAGMENT,
                    ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                    INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(OUT, new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds());
            assertEquals(taskInfo.getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getState(), TaskState.RUNNING);

            BufferResult results = sqlTaskManager.getTaskResults(taskId, OUT, 0, new DataSize(1, Unit.MEGABYTE)).get();
            assertEquals(results.isBufferClosed(), false);
            assertEquals(results.getPages().size(), 1);
            assertEquals(results.getPages().get(0).getPositionCount(), 1);

            results = sqlTaskManager.getTaskResults(taskId, OUT, results.getToken() + results.getPages().size(), new DataSize(1, Unit.MEGABYTE)).get();
            assertEquals(results.isBufferClosed(), true);
            assertEquals(results.getPages().size(), 0);

            taskInfo = sqlTaskManager.getTaskInfo(taskId, taskInfo.getState()).get(1, TimeUnit.SECONDS);
            assertEquals(taskInfo.getState(), TaskState.FINISHED);
            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getState(), TaskState.FINISHED);
        }
    }

    @Test
    public void testCancel()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = sqlTaskManager.updateTask(TEST_SESSION,
                    taskId,
                    PLAN_FRAGMENT,
                    ImmutableList.<TaskSource>of(),
                    INITIAL_EMPTY_OUTPUT_BUFFERS);
            assertEquals(taskInfo.getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.cancelTask(taskId);
            assertEquals(taskInfo.getState(), TaskState.CANCELED);
            assertNotNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getState(), TaskState.CANCELED);
            assertNotNull(taskInfo.getStats().getEndTime());
        }
    }

    @Test
    public void testAbort()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = sqlTaskManager.updateTask(TEST_SESSION,
                    taskId,
                    PLAN_FRAGMENT,
                    ImmutableList.<TaskSource>of(),
                    INITIAL_EMPTY_OUTPUT_BUFFERS);
            assertEquals(taskInfo.getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.abortTask(taskId);
            assertEquals(taskInfo.getState(), TaskState.ABORTED);
            assertNotNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getState(), TaskState.ABORTED);
            assertNotNull(taskInfo.getStats().getEndTime());
        }
    }

    @Test
    public void testAbortResults()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = sqlTaskManager.updateTask(TEST_SESSION,
                    taskId,
                    PLAN_FRAGMENT,
                    ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                    INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(OUT, new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds());
            assertEquals(taskInfo.getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getState(), TaskState.RUNNING);

            sqlTaskManager.abortTaskResults(taskId, OUT);

            taskInfo = sqlTaskManager.getTaskInfo(taskId, taskInfo.getState()).get(1, TimeUnit.SECONDS);
            assertEquals(taskInfo.getState(), TaskState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getState(), TaskState.FINISHED);
        }
    }

    @Test
    public void testRemoveOldTasks()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig().setInfoMaxAge(new Duration(5, TimeUnit.MILLISECONDS)))) {
            TaskId taskId = TASK_ID;

            TaskInfo taskInfo = sqlTaskManager.updateTask(TEST_SESSION,
                    taskId,
                    PLAN_FRAGMENT,
                    ImmutableList.<TaskSource>of(),
                    INITIAL_EMPTY_OUTPUT_BUFFERS);
            assertEquals(taskInfo.getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.cancelTask(taskId);
            assertEquals(taskInfo.getState(), TaskState.CANCELED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getState(), TaskState.CANCELED);

            Thread.sleep(100);
            sqlTaskManager.removeOldTasks();

            for (TaskInfo info : sqlTaskManager.getAllTaskInfo()) {
                assertNotEquals(info.getTaskId(), taskId);
            }
        }
    }

    public SqlTaskManager createSqlTaskManager(TaskManagerConfig config)
    {
        return new SqlTaskManager(
                createTestingPlanner(),
                new MockLocationFactory(),
                taskExecutor,
                new QueryMonitor(new ObjectMapperProvider().get(), new NullEventClient(), new NodeInfo("test"), new NodeVersion("testVersion")),
                new NodeInfo("test"),
                localMemoryManager,
                config,
                new MemoryManagerConfig());
    }

    public static class MockExchangeClientSupplier
            implements Supplier<ExchangeClient>
    {
        @Override
        public ExchangeClient get()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class MockLocationFactory
            implements LocationFactory
    {
        @Override
        public URI createQueryLocation(QueryId queryId)
        {
            return URI.create("fake://query/" + queryId);
        }

        @Override
        public URI createStageLocation(StageId stageId)
        {
            return URI.create("fake://stage/" + stageId);
        }

        @Override
        public URI createLocalTaskLocation(TaskId taskId)
        {
            return URI.create("fake://task/" + taskId);
        }

        @Override
        public URI createTaskLocation(Node node, TaskId taskId)
        {
            return URI.create("fake://task/" + node.getNodeIdentifier() + "/" + taskId);
        }

        @Override
        public URI createMemoryInfoLocation(Node node)
        {
            return URI.create("fake://" + node.getNodeIdentifier() + "/memory");
        }
    }
}
