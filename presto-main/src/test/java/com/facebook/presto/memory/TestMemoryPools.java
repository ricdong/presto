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
package com.facebook.presto.memory;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMemoryPools
{
    private static final long TEN_MEGABYTES = new DataSize(10, MEGABYTE).toBytes();

    @Test
    public void testBlocking()
            throws Exception
    {
        Session session = TEST_SESSION
                .withSystemProperty("task_default_concurrency", "1");

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session);

        // add tpch
        InMemoryNodeManager nodeManager = localQueryRunner.getNodeManager();
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(nodeManager, 1), ImmutableMap.<String, String>of());

        // reserve all the memory in the pool
        MemoryPool pool = new MemoryPool(new MemoryPoolId("test"), new DataSize(10, MEGABYTE), true);
        assertTrue(pool.tryReserve(TEN_MEGABYTES));
        MemoryPool systemPool = new MemoryPool(new MemoryPoolId("testSystem"), new DataSize(10, MEGABYTE), true);

        QueryContext queryContext = new QueryContext(true, new DataSize(10, MEGABYTE), pool, systemPool, localQueryRunner.getExecutor());
        LocalQueryRunner.MaterializedOutputFactory outputFactory = new LocalQueryRunner.MaterializedOutputFactory();
        TaskContext taskContext = createTaskContext(queryContext, localQueryRunner.getExecutor(), session, new DataSize(10, MEGABYTE), new DataSize(0, BYTE));
        Driver driver = Iterables.getOnlyElement(localQueryRunner.createDrivers("SELECT COUNT(*), clerk FROM orders GROUP BY clerk", outputFactory, taskContext));

        // run driver, until it blocks
        while (!driver.isFinished()) {
            if (!driver.process().isDone()) {
                break;
            }
        }

        // driver should be blocked waiting for memory
        assertFalse(driver.isFinished());
        assertTrue(pool.getFreeBytes() <= 0);

        pool.free(TEN_MEGABYTES);
        do {
            // driver should not block
            assertTrue(driver.process().isDone());
        }
        while (!driver.isFinished());
    }
}
