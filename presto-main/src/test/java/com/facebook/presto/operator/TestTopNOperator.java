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

import com.facebook.presto.operator.TopNOperator.TopNOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.spi.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestTopNOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
        driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testSingleFieldKey()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .pageBreak()
                .row(5, 0.5)
                .row(4, 0.41)
                .row(6, 0.6)
                .pageBreak()
                .build();

        TopNOperatorFactory factory = new TopNOperatorFactory(
                0,
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                false);

        Operator operator = factory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE)
                .row(6, 0.6)
                .row(5, 0.5)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testMultiFieldKey()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1)
                .row("b", 2)
                .pageBreak()
                .row("f", 3)
                .row("a", 4)
                .pageBreak()
                .row("d", 5)
                .row("d", 7)
                .row("e", 6)
                .build();

        TopNOperatorFactory operatorFactory = new TopNOperatorFactory(
                0,
                ImmutableList.of(VARCHAR, BIGINT),
                3,
                ImmutableList.of(0, 1),
                ImmutableList.of(DESC_NULLS_LAST, DESC_NULLS_LAST),
                false);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("f", 3)
                .row("e", 6)
                .row("d", 7)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testReverseOrder()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .pageBreak()
                .row(5, 0.5)
                .row(4, 0.41)
                .row(6, 0.6)
                .pageBreak()
                .build();

        TopNOperatorFactory operatorFactory = new TopNOperatorFactory(
                0,
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                false);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE)
                .row(-1, -0.1)
                .row(1, 0.1)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testLimitZero()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT).row(1).build();

        TopNOperatorFactory factory = new TopNOperatorFactory(
                0,
                ImmutableList.of(BIGINT),
                0,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                false);

        Operator operator = factory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT).build();

        // assertOperatorEquals assumes operators do not start in finished state
        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);

        List<Page> pages = OperatorAssertion.toPages(operator, input.iterator());
        MaterializedResult actual = OperatorAssertion.toMaterializedResult(operator.getOperatorContext().getSession(), operator.getTypes(), pages);
        assertEquals(actual, expected);
    }
}
