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
package com.facebook.presto.cassandra;

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraOutputTableHandle
         implements ConnectorOutputTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final boolean sampled;
    private final String tableOwner;

    @JsonCreator
    public CassandraOutputTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("sampled") boolean sampled,
            @JsonProperty("tableOwner") String tableOwner)
    {
        this.sampled = sampled;
        this.connectorId = checkNotNull(connectorId, "clientId is null");
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.tableOwner = checkNotNull(tableOwner, "tableOwner is null");

        checkNotNull(columnNames, "columnNames is null");
        checkNotNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @JsonProperty
    public boolean isSampled()
    {
        return sampled;
    }

    @JsonProperty
    public String getTableOwner()
    {
        return tableOwner;
    }

    @Override
    public String toString()
    {
        return "cassandra:" + schemaName + "." + tableName;
    }
}
