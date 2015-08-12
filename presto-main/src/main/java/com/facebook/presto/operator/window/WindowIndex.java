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
package com.facebook.presto.operator.window;

import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;

public class WindowIndex
{
    private final PagesIndex pagesIndex;
    private final int start;
    private final int size;

    public WindowIndex(PagesIndex pagesIndex, int start, int end)
    {
        checkNotNull(pagesIndex, "pagesIndex is null");
        checkPositionIndex(start, pagesIndex.getPositionCount(), "start");
        checkPositionIndex(end, pagesIndex.getPositionCount(), "end");
        checkArgument(start < end, "start must be before end");

        this.pagesIndex = pagesIndex;
        this.start = start;
        this.size = end - start;
    }

    public int size()
    {
        return size;
    }

    public boolean isNull(int channel, int position)
    {
        return pagesIndex.isNull(channel, position(position));
    }

    public boolean getBoolean(int channel, int position)
    {
        return pagesIndex.getBoolean(channel, position(position));
    }

    public long getLong(int channel, int position)
    {
        return pagesIndex.getLong(channel, position(position));
    }

    public double getDouble(int channel, int position)
    {
        return pagesIndex.getDouble(channel, position(position));
    }

    public Slice getSlice(int channel, int position)
    {
        return pagesIndex.getSlice(channel, position(position));
    }

    public void appendTo(int channel, int position, BlockBuilder output)
    {
        pagesIndex.appendTo(channel, position(position), output);
    }

    private int position(int position)
    {
        checkElementIndex(position, size, "position");
        return position + start;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("size", size)
                .toString();
    }
}
