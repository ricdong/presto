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
import it.unimi.dsi.fastutil.longs.LongIterator;

import java.io.Closeable;

public interface LookupSource
        extends Closeable
{
    int getChannelCount();

    long getInMemorySizeInBytes();

    long getJoinPosition(int position, Page page, int rawHash);

    long getJoinPosition(int position, Page page);

    long getNextJoinPosition(long currentPosition);

    void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset);

    LongIterator getUnvisitedJoinPositions();

    @Override
    void close();
}
