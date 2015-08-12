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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.UnpartitionedPagePartitionFunction;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.SharedBuffer.BufferState;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.MemoryPoolId;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.event.client.NullEventClient;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.TaskTestUtils.EMPTY_SOURCES;
import static com.facebook.presto.execution.TaskTestUtils.PLAN_FRAGMENT;
import static com.facebook.presto.execution.TaskTestUtils.SPLIT;
import static com.facebook.presto.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static com.facebook.presto.execution.TaskTestUtils.createTestingPlanner;
import static com.facebook.presto.execution.TaskTestUtils.updateTask;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public class TestSqlTask
{
    public static final TaskId OUT = new TaskId("query", "stage", "out");
    private final TaskExecutor taskExecutor;
    private final ScheduledExecutorService taskNotificationExecutor;
    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;

    private final AtomicLong nextTaskId = new AtomicLong();

    public TestSqlTask()
    {
        taskExecutor = new TaskExecutor(8, 16);
        taskExecutor.start();

        taskNotificationExecutor = newScheduledThreadPool(5, threadsNamed("task-notification-%s"));

        LocalExecutionPlanner planner = createTestingPlanner();

        sqlTaskExecutionFactory = new SqlTaskExecutionFactory(
                taskNotificationExecutor,
                taskExecutor,
                planner,
                new QueryMonitor(new ObjectMapperProvider().get(), new NullEventClient(), new NodeInfo("test"), new NodeVersion("testVersion")),
                new TaskManagerConfig());
    }

    @AfterClass
    public void destroy()
            throws Exception
    {
        taskExecutor.stop();
        taskNotificationExecutor.shutdownNow();
    }

    @Test
    public void testEmptyQuery()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                PLAN_FRAGMENT,
                ImmutableList.<TaskSource>of(),
                INITIAL_EMPTY_OUTPUT_BUFFERS);
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTask.updateTask(TEST_SESSION,
                PLAN_FRAGMENT,
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.<ScheduledSplit>of(), true)),
                INITIAL_EMPTY_OUTPUT_BUFFERS.withNoMoreBufferIds());
        assertEquals(taskInfo.getState(), TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getState(), TaskState.FINISHED);
    }

    @Test
    public void testSimpleQuery()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                PLAN_FRAGMENT,
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(OUT, new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds());
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        BufferResult results = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE)).get();
        assertEquals(results.isBufferClosed(), false);
        assertEquals(results.getPages().size(), 1);
        assertEquals(results.getPages().get(0).getPositionCount(), 1);

        results = sqlTask.getTaskResults(OUT, results.getToken() + results.getPages().size(), new DataSize(1, MEGABYTE)).get();
        assertEquals(results.isBufferClosed(), true);
        assertEquals(results.getPages().size(), 0);

        taskInfo = sqlTask.getTaskInfo(taskInfo.getState()).get(1, SECONDS);
        assertEquals(taskInfo.getState(), TaskState.FINISHED);
        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getState(), TaskState.FINISHED);
    }

    @Test
    public void testCancel()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                PLAN_FRAGMENT,
                ImmutableList.<TaskSource>of(),
                INITIAL_EMPTY_OUTPUT_BUFFERS);
        assertEquals(taskInfo.getState(), TaskState.RUNNING);
        assertNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getState(), TaskState.RUNNING);
        assertNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTask.cancel();
        assertEquals(taskInfo.getState(), TaskState.CANCELED);
        assertNotNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getState(), TaskState.CANCELED);
        assertNotNull(taskInfo.getStats().getEndTime());
    }

    @Test
    public void testAbort()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                PLAN_FRAGMENT,
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(OUT, new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds());
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        sqlTask.abortTaskResults(OUT);

        taskInfo = sqlTask.getTaskInfo(taskInfo.getState()).get(1, SECONDS);
        assertEquals(taskInfo.getState(), TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getState(), TaskState.FINISHED);
    }

    @Test
    public void testBufferCloseOnFinish()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(OUT, new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds();
        updateTask(sqlTask, EMPTY_SOURCES, outputBuffers);

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        // finish the task by closing the sources (no splits will ever be added)
        updateTask(sqlTask, ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.<ScheduledSplit>of(), true)), outputBuffers);
        assertEquals(sqlTask.getTaskInfo().getState(), TaskState.FINISHED);

        // buffer will be closed by cancel event (wait for event to fire)
        assertTrue(bufferResult.get(1, SECONDS).isBufferClosed());
        assertEquals(sqlTask.getTaskInfo().getOutputBuffers().getState(), BufferState.FINISHED);

        // verify the buffer is closed
        bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertTrue(bufferResult.isDone());
        assertTrue(bufferResult.get().isBufferClosed());
    }

    @Test
    public void testBufferCloseOnCancel()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        updateTask(sqlTask, EMPTY_SOURCES, INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(OUT, new UnpartitionedPagePartitionFunction()));

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        sqlTask.cancel();
        assertEquals(sqlTask.getTaskInfo().getState(), TaskState.CANCELED);

        // buffer will be closed by cancel event.. the event is async so wait a bit for event to propagate
        assertTrue(bufferResult.get(1, SECONDS).isBufferClosed());

        bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertTrue(bufferResult.isDone());
        assertTrue(bufferResult.get().isBufferClosed());
    }

    @Test
    public void testBufferNotCloseOnFail()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        updateTask(sqlTask, EMPTY_SOURCES, INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(OUT, new UnpartitionedPagePartitionFunction()));

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        TaskState taskState = sqlTask.getTaskInfo().getState();
        sqlTask.failed(new Exception("test"));
        assertEquals(sqlTask.getTaskInfo(taskState).get(1, SECONDS).getState(), TaskState.FAILED);

        // buffer will not be closed by fail event.  event is async so wait a bit for event to fire
        try {
            assertTrue(bufferResult.get(1, SECONDS).isBufferClosed());
            fail("expected TimeoutException");
        }
        catch (TimeoutException expected) {
            // expected
        }
        assertFalse(sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE)).isDone());
    }

    public SqlTask createInitialTask()
    {
        TaskId taskId = new TaskId("query", "stage", "task" + nextTaskId.incrementAndGet());
        URI location = URI.create("fake://task/" + taskId);

        return new SqlTask(
                taskId,
                "test",
                location,
                new QueryContext(false, new DataSize(1, MEGABYTE), new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE), false), new MemoryPool(new MemoryPoolId("testSystem"), new DataSize(1, GIGABYTE), false), taskNotificationExecutor),
                sqlTaskExecutionFactory,
                taskNotificationExecutor,
                Functions.<SqlTask>identity(),
                new DataSize(32, MEGABYTE));
    }
}
