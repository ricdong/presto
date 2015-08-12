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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RowComparator
        implements Comparator<Block[]>
{
    private final List<Type> sortTypes;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;

    public RowComparator(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        this.sortTypes = ImmutableList.copyOf(checkNotNull(sortTypes, "sortTypes is null"));
        this.sortChannels = ImmutableList.copyOf(checkNotNull(sortChannels, "sortChannels is null"));
        this.sortOrders = ImmutableList.copyOf(checkNotNull(sortOrders, "sortOrders is null"));
        checkArgument(sortTypes.size() == sortChannels.size(), "sortTypes size (%s) doesn't match sortChannels size (%s)", sortTypes.size(), sortChannels.size());
        checkArgument(sortChannels.size() == sortOrders.size(), "sortFields size (%s) doesn't match sortOrders size (%s)", sortChannels.size(), sortOrders.size());
    }

    @Override
    public int compare(Block[] leftRow, Block[] rightRow)
    {
        for (int index = 0; index < sortChannels.size(); index++) {
            Type type = sortTypes.get(index);
            int channel = sortChannels.get(index);
            SortOrder sortOrder = sortOrders.get(index);

            Block left = leftRow[channel];
            Block right = rightRow[channel];

            int comparison = sortOrder.compareBlockValue(type, left, 0, right, 0);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }
}
