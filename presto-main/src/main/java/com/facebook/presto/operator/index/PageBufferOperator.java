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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class PageBufferOperator
        implements Operator
{
    public static class PageBufferOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PageBuffer pageBuffer;

        public PageBufferOperatorFactory(int operatorId, PageBuffer pageBuffer)
        {
            this.operatorId = operatorId;
            this.pageBuffer = checkNotNull(pageBuffer, "pageBuffer is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, PageBufferOperator.class.getSimpleName());
            return new PageBufferOperator(operatorContext, pageBuffer);
        }

        @Override
        public void close()
        {
        }
    }

    private final OperatorContext operatorContext;
    private final PageBuffer pageBuffer;
    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private boolean finished;

    public PageBufferOperator(OperatorContext operatorContext, PageBuffer pageBuffer)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.pageBuffer = checkNotNull(pageBuffer, "pageBuffer is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.of();
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        updateBlockedIfNecessary();
        return finished && blocked == NOT_BLOCKED;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        updateBlockedIfNecessary();
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        updateBlockedIfNecessary();
        return !finished && blocked == NOT_BLOCKED;
    }

    private void updateBlockedIfNecessary()
    {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(blocked == NOT_BLOCKED, "output is already blocked");
        ListenableFuture<?> future = pageBuffer.add(page);
        if (!future.isDone()) {
            this.blocked = future;
        }
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
