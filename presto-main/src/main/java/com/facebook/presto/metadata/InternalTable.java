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
package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockUtils;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class InternalTable
{
    private final Map<String, Integer> columnIndexes;
    private final List<Page> pages;

    public InternalTable(Map<String, Integer> columnIndexes, Iterable<Page> pages)
    {
        this.columnIndexes = ImmutableMap.copyOf(checkNotNull(columnIndexes, "columnIndexes is null"));
        this.pages = ImmutableList.copyOf(checkNotNull(pages, "pages is null"));
    }

    public int getColumnIndex(String columnName)
    {
        Integer index = columnIndexes.get(columnName);
        checkArgument(index != null, "Column %s not found", columnName);
        return index;
    }

    public List<Page> getPages()
    {
        return pages;
    }

    public static Builder builder(ColumnMetadata... columns)
    {
        return builder(ImmutableList.copyOf(columns));
    }

    public static Builder builder(List<ColumnMetadata> columns)
    {
        ImmutableList.Builder<String> names = ImmutableList.builder();
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ColumnMetadata column : columns) {
            names.add(column.getName());
            types.add(column.getType());
        }
        return new Builder(names.build(), types.build());
    }

    public static class Builder
    {
        private final Map<String, Integer> columnIndexes;
        private final List<Type> types;
        private final List<Page> pages;
        private PageBuilder pageBuilder;

        public Builder(List<String> columnNames, List<Type> types)
        {
            checkNotNull(columnNames, "columnNames is null");

            ImmutableMap.Builder<String, Integer> columnIndexes = ImmutableMap.builder();
            int columnIndex = 0;
            for (String columnName : columnNames) {
                columnIndexes.put(columnName, columnIndex++);
            }
            this.columnIndexes = columnIndexes.build();

            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
            checkArgument(columnNames.size() == types.size(),
                    "Column name count does not match type count: columnNames=%s, types=%s", columnNames, types.size());

            pages = new ArrayList<>();
            pageBuilder = new PageBuilder(types);
        }

        public Builder add(Object... values)
        {
            pageBuilder.declarePosition();
            for (int i = 0; i < types.size(); i++) {
                BlockUtils.appendObject(types.get(i), pageBuilder.getBlockBuilder(i), values[i]);
            }

            if (pageBuilder.isFull()) {
                flushPage();
            }
            return this;
        }

        public InternalTable build()
        {
            flushPage();
            return new InternalTable(columnIndexes, pages);
        }

        private void flushPage()
        {
            if (!pageBuilder.isEmpty()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
    }
}
