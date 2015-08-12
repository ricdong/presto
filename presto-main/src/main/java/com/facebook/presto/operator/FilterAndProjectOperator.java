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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class FilterAndProjectOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> types;

    private final PageBuilder pageBuilder;
    private final PageProcessor processor;
    private Page currentPage;
    private int currentPosition;
    private boolean finishing;

    public FilterAndProjectOperator(OperatorContext operatorContext, Iterable<? extends Type> types, PageProcessor processor)
    {
        this.processor = checkNotNull(processor, "processor is null");
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.pageBuilder = new PageBuilder(getTypes());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public final List<Type> getTypes()
    {
        return types;
    }

    @Override
    public final void finish()
    {
        finishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        return finishing && pageBuilder.isEmpty() && currentPage == null;
    }

    @Override
    public final boolean needsInput()
    {
        return !finishing && !pageBuilder.isFull() && currentPage == null;
    }

    @Override
    public final void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkNotNull(page, "page is null");
        checkState(!pageBuilder.isFull(), "Page buffer is full");

        currentPage = page;
        currentPosition = 0;
    }

    @Override
    public final Page getOutput()
    {
        if (!pageBuilder.isFull() && currentPage != null) {
            currentPosition = processor.process(operatorContext.getSession().toConnectorSession(), currentPage, currentPosition, currentPage.getPositionCount(), pageBuilder);
            if (currentPosition == currentPage.getPositionCount()) {
                currentPage = null;
                currentPosition = 0;
            }
        }

        if (!finishing && !pageBuilder.isFull() || pageBuilder.isEmpty()) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    public static class FilterAndProjectOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PageProcessor processor;
        private final List<Type> types;
        private boolean closed;

        public FilterAndProjectOperatorFactory(int operatorId, PageProcessor processor, List<Type> types)
        {
            this.operatorId = operatorId;
            this.processor = processor;
            this.types = types;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, FilterAndProjectOperator.class.getSimpleName());
            return new FilterAndProjectOperator(operatorContext, types, processor);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }
}
