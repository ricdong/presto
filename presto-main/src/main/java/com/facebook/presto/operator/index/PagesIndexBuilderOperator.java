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

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class PagesIndexBuilderOperator
        implements Operator
{
    public static class PagesIndexBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final IndexSnapshotBuilder indexSnapshotBuilder;
        private boolean closed;

        public PagesIndexBuilderOperatorFactory(int operatorId, IndexSnapshotBuilder indexSnapshotBuilder)
        {
            this.operatorId = operatorId;
            this.indexSnapshotBuilder = checkNotNull(indexSnapshotBuilder, "indexSnapshotBuilder is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, PagesIndexBuilderOperator.class.getSimpleName());
            return new PagesIndexBuilderOperator(operatorContext, indexSnapshotBuilder);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final IndexSnapshotBuilder indexSnapshotBuilder;

    private boolean finished;

    public PagesIndexBuilderOperator(OperatorContext operatorContext, IndexSnapshotBuilder indexSnapshotBuilder)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.indexSnapshotBuilder = checkNotNull(indexSnapshotBuilder, "indexSnapshotBuilder is null");
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
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        if (!indexSnapshotBuilder.tryAddPage(page)) {
            finish();
            return;
        }
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
