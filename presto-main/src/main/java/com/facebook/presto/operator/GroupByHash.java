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

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public interface GroupByHash
{
    static GroupByHash createGroupByHash(List<? extends Type> hashTypes, int[] hashChannels, Optional<Integer> maskChannel, Optional<Integer> inputHashChannel, int expectedSize)
    {
        if (hashTypes.size() == 1 && hashTypes.get(0).equals(BIGINT) && hashChannels.length == 1) {
            return new BigintGroupByHash(hashChannels[0], maskChannel, inputHashChannel.isPresent(), expectedSize);
        }
        return new MultiChannelGroupByHash(hashTypes, hashChannels, maskChannel, inputHashChannel, expectedSize);
    }

    long getEstimatedSize();

    List<Type> getTypes();

    int getGroupCount();

    void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset);

    void addPage(Page page);

    GroupByIdBlock getGroupIds(Page page);

    boolean contains(int position, Page page);

    int putIfAbsent(int position, Page page);
}
