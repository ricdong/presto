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
package com.facebook.presto.connector.system;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;

import javax.inject.Inject;

import java.util.Map;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkNotNull;

public class CatalogSystemTable
        implements SystemTable
{
    public static final SchemaTableName CATALOG_TABLE_NAME = new SchemaTableName("metadata", "catalogs");

    public static final ConnectorTableMetadata CATALOG_TABLE = tableMetadataBuilder(CATALOG_TABLE_NAME)
            .column("catalog_name", VARCHAR)
            .column("connector_id", VARCHAR)
            .build();
    private final Metadata metadata;

    @Inject
    public CatalogSystemTable(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata);
    }

    @Override
    public boolean isDistributed()
    {
        return false;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return CATALOG_TABLE;
    }

    @Override
    public RecordCursor cursor()
    {
        Builder table = InMemoryRecordSet.builder(CATALOG_TABLE);
        for (Map.Entry<String, String> entry : metadata.getCatalogNames().entrySet()) {
            table.addRow(entry.getKey(), entry.getValue());
        }
        return table.build().cursor();
    }
}
