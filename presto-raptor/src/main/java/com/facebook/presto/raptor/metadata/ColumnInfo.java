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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.spi.type.Type;

import static com.google.common.base.Preconditions.checkNotNull;

public class ColumnInfo
{
    private final long columnId;
    private final Type type;

    public ColumnInfo(long columnId, Type type)
    {
        this.columnId = columnId;
        this.type = checkNotNull(type, "type is null");
    }

    public long getColumnId()
    {
        return columnId;
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return columnId + ":" + type;
    }

    public static ColumnInfo fromHandle(RaptorColumnHandle handle)
    {
        return new ColumnInfo(handle.getColumnId(), handle.getColumnType());
    }
}
