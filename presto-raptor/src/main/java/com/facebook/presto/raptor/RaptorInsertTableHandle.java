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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RaptorInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final String connectorId;
    private final long tableId;
    private final List<RaptorColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    @Nullable
    private final String externalBatchId;
    private final List<RaptorColumnHandle> sortColumnHandles;
    private final List<SortOrder> sortOrders;

    @JsonCreator
    public RaptorInsertTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("columnHandles") List<RaptorColumnHandle> columnHandles,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("externalBatchId") @Nullable String externalBatchId,
            @JsonProperty("sortColumnHandles") List<RaptorColumnHandle> sortColumnHandles,
            @JsonProperty("sortOrders") List<SortOrder> sortOrders)
    {
        checkArgument(tableId > 0, "tableId must be greater than zero");

        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.tableId = tableId;
        this.columnHandles = ImmutableList.copyOf(checkNotNull(columnHandles, "columnHandles is null"));
        this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));
        this.externalBatchId = externalBatchId;

        this.sortOrders = ImmutableList.copyOf(checkNotNull(sortOrders, "sortOrders is null"));
        this.sortColumnHandles = ImmutableList.copyOf(checkNotNull(sortColumnHandles, "sortColumnHandles is null"));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public List<RaptorColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Nullable
    @JsonProperty
    public String getExternalBatchId()
    {
        return externalBatchId;
    }

    @JsonProperty
    public List<RaptorColumnHandle> getSortColumnHandles()
    {
        return sortColumnHandles;
    }

    @JsonProperty
    public List<SortOrder> getSortOrders()
    {
        return sortOrders;
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + tableId;
    }
}
