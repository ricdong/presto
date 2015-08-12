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

import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.memory.MemoryPoolId;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.STARTING;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestQueryStateMachine
{
    private static final QueryId QUERY_ID = new QueryId("query_id");
    private static final String QUERY = "sql";
    private static final URI LOCATION = URI.create("fake://fake-query");
    private static final SQLException FAILED_CAUSE = new SQLException("FAILED");
    private static final List<Input> INPUTS = ImmutableList.of(new Input("connector", "schema", "table", ImmutableList.of(new Column("a", "varchar", Optional.empty()))));
    private static final List<String> OUTPUT_FIELD_NAMES = ImmutableList.of("a", "b", "c");
    private static final String UPDATE_TYPE = "update type";
    private static final VersionedMemoryPoolId MEMORY_POOL = new VersionedMemoryPoolId(new MemoryPoolId("pool"), 42);
    private static final Map<String, String> SET_SESSION_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("fruit", "apple")
            .put("drink", "coffee")
            .build();
    private static final List<String> RESET_SESSION_PROPERTIES = ImmutableList.of("candy");

    private final ExecutorService executor = newCachedThreadPool();

    @AfterClass
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testBasicStateChanges()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertState(stateMachine, QUEUED);

        assertTrue(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, FINISHED);
    }

    @Test
    public void testQueued()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertState(stateMachine, QUEUED);

        assertTrue(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, FAILED);
    }

    @Test
    public void testPlanning()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, FAILED);
    }

    @Test
    public void testStarting()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, STARTING);

        assertFalse(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToStarting();
        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToStarting();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, FAILED);
    }

    @Test
    public void testRunning()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, RUNNING);

        assertFalse(stateMachine.transitionToStarting());
        assertState(stateMachine, RUNNING);

        assertFalse(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToRunning();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, FAILED);
    }

    @Test
    public void testFinished()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToFinished());
        assertFinalState(stateMachine, FINISHED);
    }

    @Test
    public void testFailed()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertFinalState(stateMachine, FAILED);
    }

    private static void assertFinalState(QueryStateMachine stateMachine, QueryState expectedState)
    {
        assertTrue(expectedState.isDone());
        assertState(stateMachine, expectedState);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, expectedState);

        assertFalse(stateMachine.transitionToStarting());
        assertState(stateMachine, expectedState);

        assertFalse(stateMachine.transitionToRunning());
        assertState(stateMachine, expectedState);

        assertFalse(stateMachine.transitionToFinished());
        assertState(stateMachine, expectedState);

        assertFalse(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, expectedState);

        // attempt to fail with another exception, which will fail
        assertFalse(stateMachine.transitionToFailed(new IOException("failure after finish")));
        assertState(stateMachine, expectedState);
    }

    private static void assertState(QueryStateMachine stateMachine, QueryState expectedState)
    {
        assertEquals(stateMachine.getQueryId(), QUERY_ID);
        assertSame(stateMachine.getSession(), TEST_SESSION);
        assertSame(stateMachine.getMemoryPool(), MEMORY_POOL);
        assertEquals(stateMachine.getSetSessionProperties(), SET_SESSION_PROPERTIES);
        assertEquals(stateMachine.getResetSessionProperties(), RESET_SESSION_PROPERTIES);

        QueryInfo queryInfo = stateMachine.getQueryInfo(null);
        assertEquals(queryInfo.getQueryId(), QUERY_ID);
        assertEquals(queryInfo.getSelf(), LOCATION);
        assertNull(queryInfo.getOutputStage());
        assertEquals(queryInfo.getQuery(), QUERY);
        assertEquals(queryInfo.getInputs(), INPUTS);
        assertEquals(queryInfo.getFieldNames(), OUTPUT_FIELD_NAMES);
        assertEquals(queryInfo.getUpdateType(), UPDATE_TYPE);
        assertEquals(queryInfo.getMemoryPool(), MEMORY_POOL.getId());

        QueryStats queryStats = queryInfo.getQueryStats();
        if (queryInfo.getState() == QUEUED) {
            assertNull(queryStats.getQueuedTime());
            assertNull(queryStats.getTotalPlanningTime());
            assertNull(queryStats.getExecutionStartTime());
            assertNull(queryStats.getEndTime());
        }
        else if (queryInfo.getState() == PLANNING) {
            assertNotNull(queryStats.getQueuedTime());
            assertNull(queryStats.getTotalPlanningTime());
            assertNull(queryStats.getExecutionStartTime());
            assertNull(queryStats.getEndTime());
        }
        else if (queryInfo.getState() == STARTING) {
            assertNotNull(queryStats.getQueuedTime());
            assertNotNull(queryStats.getTotalPlanningTime());
            assertNull(queryStats.getExecutionStartTime());
            assertNull(queryStats.getEndTime());
        }
        else if (queryInfo.getState() == RUNNING) {
            assertNotNull(queryStats.getQueuedTime());
            assertNotNull(queryStats.getTotalPlanningTime());
            assertNotNull(queryStats.getExecutionStartTime());
            assertNull(queryStats.getEndTime());
        }
        else {
            assertNotNull(queryStats.getQueuedTime());
            assertNotNull(queryStats.getTotalPlanningTime());
            assertNotNull(queryStats.getExecutionStartTime());
            assertNotNull(queryStats.getEndTime());
        }

        assertEquals(stateMachine.getQueryState(), expectedState);
        assertEquals(queryInfo.getState(), expectedState);
        assertEquals(stateMachine.isDone(), expectedState.isDone());

        if (expectedState == FAILED) {
            FailureInfo failure = queryInfo.getFailureInfo();
            assertNotNull(failure);
            assertEquals(failure.getMessage(), FAILED_CAUSE.getMessage());
            assertEquals(failure.getType(), FAILED_CAUSE.getClass().getName());
        }
        else {
            assertNull(queryInfo.getFailureInfo());
        }
    }

    @NotNull
    private QueryStateMachine createQueryStateMachine()
    {
        QueryStateMachine stateMachine = new QueryStateMachine(QUERY_ID, QUERY, TEST_SESSION, LOCATION, executor);
        stateMachine.setInputs(INPUTS);
        stateMachine.setOutputFieldNames(OUTPUT_FIELD_NAMES);
        stateMachine.setUpdateType(UPDATE_TYPE);
        stateMachine.setMemoryPool(MEMORY_POOL);
        for (Entry<String, String> entry : SET_SESSION_PROPERTIES.entrySet()) {
            stateMachine.addSetSessionProperties(entry.getKey(), entry.getValue());
        }
        RESET_SESSION_PROPERTIES.forEach(stateMachine::addResetSessionProperties);
        return stateMachine;
    }
}
