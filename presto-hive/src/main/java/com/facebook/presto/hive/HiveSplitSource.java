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
package com.facebook.presto.hive;

import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.failedFuture;

class HiveSplitSource
        implements ConnectorSplitSource
{
    private final String connectorId;
    private final AsyncQueue<ConnectorSplit> queue = new AsyncQueue<>();
    private final AtomicInteger outstandingSplitCount = new AtomicInteger();
    private final AtomicReference<Throwable> throwable = new AtomicReference<>();
    private final int maxOutstandingSplits;
    private final HiveSplitLoader splitLoader;
    private volatile boolean closed;

    HiveSplitSource(String connectorId, int maxOutstandingSplits, HiveSplitLoader splitLoader)
    {
        this.connectorId = connectorId;
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.splitLoader = splitLoader;
    }

    int getOutstandingSplitCount()
    {
        return outstandingSplitCount.get();
    }

    void addToQueue(Iterable<? extends ConnectorSplit> splits)
    {
        for (ConnectorSplit split : splits) {
            addToQueue(split);
        }
    }

    void addToQueue(ConnectorSplit split)
    {
        if (throwable.get() == null) {
            outstandingSplitCount.incrementAndGet();
            queue.add(split);
        }
    }

    boolean isQueueFull()
    {
        return outstandingSplitCount.get() >= maxOutstandingSplits;
    }

    void finished()
    {
        if (throwable.get() == null) {
            queue.finish();
            splitLoader.stop();
        }
    }

    void fail(Throwable e)
    {
        // only record the first error message
        if (throwable.compareAndSet(null, e)) {
            // add finish the queue
            queue.finish();

            // no need to process any more jobs
            splitLoader.stop();
        }
    }

    @Override
    public String getDataSourceName()
    {
        return connectorId;
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
    {
        checkState(!closed, "Provider is already closed");

        CompletableFuture<List<ConnectorSplit>> future = queue.getBatchAsync(maxSize);

        // Before returning, check if there is a registered failure.
        // If so, we want to throw the error, instead of returning because the scheduler can block
        // while scheduling splits and wait for work to finish before continuing.  In this case,
        // we want to end the query as soon as possible and abort the work
        if (throwable.get() != null) {
            return failedFuture(throwable.get());
        }

        // when future completes, decrement the outstanding split count by the number of splits we took
        future.thenAccept(splits -> {
            if (outstandingSplitCount.addAndGet(-splits.size()) < maxOutstandingSplits) {
                // we are below the low water mark (and there isn't a failure) so resume scanning hdfs
                splitLoader.resume();
            }
        });

        return future;
    }

    @Override
    public boolean isFinished()
    {
        // the finished marker must be checked before checking the throwable
        // to avoid a race with the fail method
        boolean isFinished = queue.isFinished();
        if (throwable.get() != null) {
            throw propagatePrestoException(throwable.get());
        }
        return isFinished;
    }

    @Override
    public void close()
    {
        queue.finish();
        splitLoader.stop();

        closed = true;
    }

    private static RuntimeException propagatePrestoException(Throwable throwable)
    {
        if (throwable instanceof PrestoException) {
            throw (PrestoException) throwable;
        }
        if (throwable instanceof FileNotFoundException) {
            throw new PrestoException(HIVE_FILE_NOT_FOUND, throwable);
        }
        throw new PrestoException(HIVE_UNKNOWN_ERROR, throwable);
    }
}
