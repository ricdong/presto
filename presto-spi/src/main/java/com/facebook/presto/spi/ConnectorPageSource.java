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
package com.facebook.presto.spi;

import java.io.Closeable;
import java.io.IOException;

public interface ConnectorPageSource
        extends Closeable
{
    /**
     * Gets the total input bytes that will be processed by this page source.
     * This is normally the same size as the split.  If size is not available,
     * this method should return zero.
     */
    long getTotalBytes();

    /**
     * Gets the number of input bytes processed by this page source so far.
     * If size is not available, this method should return zero.
     */
    long getCompletedBytes();

    /**
     * Gets the wall time this page source spent reading data from the input.
     * If read time is not available, this method should return zero.
     */
    long getReadTimeNanos();

    /**
     * Will this page source product more pages?
     */
    boolean isFinished();

    /**
     * Gets the next page of data.  This method is allowed to return null.
     */
    Page getNextPage();

    /**
     * Immediately finishes this page source.  Presto will always call this method.
     */
    @Override
    void close()
            throws IOException;
}
