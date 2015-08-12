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
import com.facebook.presto.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

public class TablePropertiesSystemTable
        implements SystemTable
{
    public static final SchemaTableName TABLE_PROPERTIES_TABLE_NAME = new SchemaTableName("metadata", "table_properties");

    public static final ConnectorTableMetadata TABLE_PROPERTIES_TABLE = tableMetadataBuilder(TABLE_PROPERTIES_TABLE_NAME)
            .column("catalog_name", VARCHAR)
            .column("property_name", VARCHAR)
            .column("default_value", VARCHAR)
            .column("type", VARCHAR)
            .column("description", VARCHAR)
            .build();
    private final Metadata metadata;

    @Inject
    public TablePropertiesSystemTable(Metadata metadata)
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
        return TABLE_PROPERTIES_TABLE;
    }

    @Override
    public RecordCursor cursor()
    {
        Builder table = InMemoryRecordSet.builder(TABLE_PROPERTIES_TABLE);
        Map<String, Map<String, PropertyMetadata<?>>> tableProperties = new TreeMap<>(metadata.getTablePropertyManager().getAllTableProperties());
        for (Entry<String, Map<String, PropertyMetadata<?>>> entry : tableProperties.entrySet()) {
            String catalog = entry.getKey();
            Map<String, PropertyMetadata<?>> properties = new TreeMap<>(entry.getValue());
            for (PropertyMetadata<?> propertyMetadata : properties.values()) {
                table.addRow(
                        catalog,
                        propertyMetadata.getName(),
                        firstNonNull(propertyMetadata.getDefaultValue(), "").toString(),
                        propertyMetadata.getSqlType().toString(),
                        propertyMetadata.getDescription());
            }
        }
        return table.build().cursor();
    }
}
