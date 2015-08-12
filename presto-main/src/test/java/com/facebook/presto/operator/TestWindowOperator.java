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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.operator.WindowOperator.WindowOperatorFactory;
import com.facebook.presto.operator.window.FirstValueFunction;
import com.facebook.presto.operator.window.FrameInfo;
import com.facebook.presto.operator.window.LagFunction;
import com.facebook.presto.operator.window.LastValueFunction;
import com.facebook.presto.operator.window.LeadFunction;
import com.facebook.presto.operator.window.NthValueFunction;
import com.facebook.presto.operator.window.ReflectionWindowFunctionSupplier;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.operator.WindowFunctionDefinition.window;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.tree.WindowFrame.Type.RANGE;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestWindowOperator
{
    private static final List<WindowFunctionDefinition> ROW_NUMBER = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("row_number", BIGINT, ImmutableList.<Type>of(), RowNumberFunction.class), BIGINT)
    );

    private static final List<WindowFunctionDefinition> FIRST_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("first_value", VARCHAR, ImmutableList.<Type>of(VARCHAR), FirstValueFunction.class), VARCHAR, 1)
    );

    private static final List<WindowFunctionDefinition> LAST_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("last_value", VARCHAR, ImmutableList.<Type>of(VARCHAR), LastValueFunction.class), VARCHAR, 1)
    );

    private static final List<WindowFunctionDefinition> NTH_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("nth_value", VARCHAR, ImmutableList.of(VARCHAR, BIGINT), NthValueFunction.class), VARCHAR, 1, 3)
    );

    private static final List<WindowFunctionDefinition> LAG = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("lag", VARCHAR, ImmutableList.of(VARCHAR, BIGINT, VARCHAR), LagFunction.class), VARCHAR, 1, 3, 4)
    );

    private static final List<WindowFunctionDefinition> LEAD = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("lead", VARCHAR, ImmutableList.of(VARCHAR, BIGINT, VARCHAR), LeadFunction.class), VARCHAR, 1, 3, 4)
    );

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
    public void testRowNumber()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(2, 0.3)
                .row(4, 0.2)
                .row(6, 0.1)
                .pageBreak()
                .row(-1, -0.1)
                .row(5, 0.4)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(-0.1, -1, 1)
                .row(0.3, 2, 2)
                .row(0.2, 4, 3)
                .row(0.4, 5, 4)
                .row(0.1, 6, 5)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testRowNumberPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, DOUBLE, BOOLEAN)
                .row("b", -1, -0.1, true)
                .row("a", 2, 0.3, false)
                .row("a", 4, 0.2, true)
                .pageBreak()
                .row("b", 5, 0.4, false)
                .row("a", 6, 0.1, true)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, DOUBLE, BOOLEAN, BIGINT)
                .row("a", 2, 0.3, false, 1)
                .row("a", 4, 0.2, true, 2)
                .row("a", 6, 0.1, true, 3)
                .row("b", -1, -0.1, true, 1)
                .row("b", 5, 0.4, false, 2)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testRowNumberArbitrary()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1)
                .row(3)
                .row(5)
                .row(7)
                .pageBreak()
                .row(2)
                .row(4)
                .row(6)
                .row(8)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(),
                ImmutableList.copyOf(new SortOrder[] {}));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                .row(1, 1)
                .row(3, 2)
                .row(5, 3)
                .row(7, 4)
                .row(2, 5)
                .row(4, 6)
                .row(6, 7)
                .row(8, 8)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 10B")
    public void testMemoryLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .build();

        DriverContext driverContext = createTaskContext(executor, TEST_SESSION, new DataSize(10, Unit.BYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        Operator operator = operatorFactory.createOperator(driverContext);

        toPages(operator, input);
    }

    @Test
    public void testFirstValuePartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1, true, "")
                .row("a", "A2", 1, false, "")
                .row("a", "B1", 2, true, "")
                .pageBreak()
                .row("b", "C1", 2, false, "")
                .row("a", "C2", 3, true, "")
                .row("c", "A3", 1, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                FIRST_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1, false, "A2")
                .row("a", "B1", 2, true, "A2")
                .row("a", "C2", 3, true, "A2")
                .row("b", "A1", 1, true, "A1")
                .row("b", "C1", 2, false, "A1")
                .row("c", "A3", 1, true, "A3")
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testLastValuePartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1, true, "")
                .row("a", "A2", 1, false, "")
                .row("a", "B1", 2, true, "")
                .pageBreak()
                .row("b", "C1", 2, false, "")
                .row("a", "C2", 3, true, "")
                .row("c", "A3", 1, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                LAST_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1, false, "C2")
                .row("a", "B1", 2, true, "C2")
                .row("a", "C2", 3, true, "C2")
                .row("b", "A1", 1, true, "C1")
                .row("b", "C1", 2, false, "C1")
                .row("c", "A3", 1, true, "A3")
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testNthValuePartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1, 2, true, "")
                .row("a", "A2", 1, 3, false, "")
                .row("a", "B1", 2, 2, true, "")
                .pageBreak()
                .row("b", "C1", 2, 3, false, "")
                .row("a", "C2", 3, 1, true, "")
                .row("c", "A3", 1, null, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 4),
                NTH_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1, false, "C2")
                .row("a", "B1", 2, true, "B1")
                .row("a", "C2", 3, true, "A2")
                .row("b", "A1", 1, true, "C1")
                .row("b", "C1", 2, false, null)
                .row("c", "A3", 1, true, null)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testLagPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR)
                .row("b", "A1", 1, 1, "D", true, "")
                .row("a", "A2", 1, 2, "D", false, "")
                .row("a", "B1", 2, 2, "D", true, "")
                .pageBreak()
                .row("b", "C1", 2, 1, "D", false, "")
                .row("a", "C2", 3, 2, "D", true, "")
                .row("c", "A3", 1, 1, "D", true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 5),
                LAG,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1, false, "D")
                .row("a", "B1", 2, true, "D")
                .row("a", "C2", 3, true, "A2")
                .row("b", "A1", 1, true, "D")
                .row("b", "C1", 2, false, "A1")
                .row("c", "A3", 1, true, "D")
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testLeadPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR)
                .row("b", "A1", 1, 1, "D", true, "")
                .row("a", "A2", 1, 2, "D", false, "")
                .row("a", "B1", 2, 2, "D", true, "")
                .pageBreak()
                .row("b", "C1", 2, 1, "D", false, "")
                .row("a", "C2", 3, 2, "D", true, "")
                .row("c", "A3", 1, 1, "D", true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 5),
                LEAD,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1, false, "C2")
                .row("a", "B1", 2, true, "D")
                .row("a", "C2", 3, true, "D")
                .row("b", "A1", 1, true, "C1")
                .row("b", "C1", 2, false, "D")
                .row("c", "A3", 1, true, "D")
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testPartiallyPreGroupedPartitionWithEmptyInput()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .pageBreak()
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0, 1),
                Ints.asList(1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testPartiallyPreGroupedPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1, "a", 100, "A")
                .row(2, "a", 101, "B")
                .pageBreak()
                .row(3, "b", 102, "E")
                .row(1, "b", 103, "D")
                .pageBreak()
                .row(3, "b", 104, "C")
                .row(1, "c", 105, "F")
                .pageBreak()
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0, 1),
                Ints.asList(1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1, "a", 100, "A", 1)
                .row(2, "a", 101, "B", 1)
                .row(3, "b", 104, "C", 1)
                .row(3, "b", 102, "E", 2)
                .row(1, "b", 103, "D", 1)
                .row(1, "c", 105, "F", 1)
                .build();

        assertOperatorEqualsIgnoreOrder(operator, input, expected);
    }

    @Test
    public void testFullyPreGroupedPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1, "a", 100, "A")
                .pageBreak()
                .row(2, "a", 101, "B")
                .pageBreak()
                .row(2, "b", 102, "D")
                .row(2, "b", 103, "C")
                .row(1, "b", 104, "E")
                .pageBreak()
                .row(1, "b", 105, "F")
                .row(3, "c", 106, "G")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1, "a", 100, "A", 1)
                .row(2, "a", 101, "B", 1)
                .row(2, "b", 103, "C", 1)
                .row(2, "b", 102, "D", 2)
                .row(1, "b", 104, "E", 1)
                .row(1, "b", 105, "F", 2)
                .row(3, "c", 106, "G", 1)
                .build();

        assertOperatorEqualsIgnoreOrder(operator, input, expected);
    }

    @Test
    public void testFullyPreGroupedAndPartiallySortedPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1, "a", 100, "A")
                .pageBreak()
                .row(2, "a", 100, "A")
                .pageBreak()
                .row(2, "b", 102, "A")
                .row(2, "b", 101, "A")
                .row(2, "b", 100, "B")
                .row(1, "b", 101, "A")
                .pageBreak()
                .row(1, "b", 100, "A")
                .row(3, "c", 100, "A")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3, 2),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST, SortOrder.ASC_NULLS_LAST),
                1);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1, "a", 100, "A", 1)
                .row(2, "a", 100, "A", 1)
                .row(2, "b", 101, "A", 1)
                .row(2, "b", 102, "A", 2)
                .row(2, "b", 100, "B", 3)
                .row(1, "b", 100, "A", 1)
                .row(1, "b", 101, "A", 2)
                .row(3, "c", 100, "A", 1)
                .build();

        assertOperatorEqualsIgnoreOrder(operator, input, expected);
    }

    @Test
    public void testFullyPreGroupedAndFullySortedPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1, "a", 100, "A")
                .pageBreak()
                .row(2, "a", 101, "A")
                .pageBreak()
                .row(2, "b", 102, "A")
                .row(2, "b", 103, "A")
                .row(2, "b", 104, "B")
                .row(1, "b", 105, "A")
                .pageBreak()
                .row(1, "b", 106, "A")
                .row(3, "c", 107, "A")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                1);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1, "a", 100, "A", 1)
                .row(2, "a", 101, "A", 1)
                .row(2, "b", 102, "A", 1)
                .row(2, "b", 103, "A", 2)
                .row(2, "b", 104, "B", 3)
                .row(1, "b", 105, "A", 1)
                .row(1, "b", 106, "A", 2)
                .row(3, "c", 107, "A", 1)
                .build();

        // Since fully grouped and sorted already, should respect original input order
        assertOperatorEquals(operator, input, expected);
    }

    private static WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder)
    {
        return createFactoryUnbounded(
                sourceTypes,
                outputChannels,
                functions,
                partitionChannels,
                ImmutableList.of(),
                sortChannels,
                sortOrder,
                0);
    }

    private static WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix)
    {
        return new WindowOperatorFactory(
                0,
                sourceTypes,
                outputChannels,
                functions,
                partitionChannels,
                preGroupedChannels,
                sortChannels,
                sortOrder,
                preSortedChannelPrefix,
                new FrameInfo(RANGE, UNBOUNDED_PRECEDING, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty()),
                10);
    }
}
