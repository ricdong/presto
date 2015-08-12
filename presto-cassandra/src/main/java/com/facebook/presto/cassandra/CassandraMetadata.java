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

import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.cassandra.CassandraColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.cassandra.CassandraType.BIGINT;
import static com.facebook.presto.cassandra.CassandraType.toCassandraType;
import static com.facebook.presto.cassandra.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;

public class CassandraMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final CachingCassandraSchemaProvider schemaProvider;
    private final CassandraSession cassandraSession;
    private final boolean allowDropTable;

    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;

    @Inject
    public CassandraMetadata(CassandraConnectorId connectorId,
            CachingCassandraSchemaProvider schemaProvider,
            CassandraSession cassandraSession,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec,
            CassandraClientConfig config)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.schemaProvider = checkNotNull(schemaProvider, "schemaProvider is null");
        this.cassandraSession = checkNotNull(cassandraSession, "cassandraSession is null");
        this.allowDropTable = checkNotNull(config, "config is null").getAllowDropTable();
        this.extraColumnMetadataCodec = checkNotNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return schemaProvider.getAllSchemas();
    }

    @Override
    public CassandraTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        try {
            CassandraTableHandle tableHandle = schemaProvider.getTableHandle(tableName);
            schemaProvider.getTable(tableHandle);
            return tableHandle;
        }
        catch (NotFoundException e) {
            // table was not found
            return null;
        }
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, CassandraTableHandle.class, "tableHandle").getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(session, tableName);
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        CassandraTableHandle tableHandle = schemaProvider.getTableHandle(tableName);
        List<ColumnMetadata> columns = getColumnHandles(session, tableHandle).values().stream()
                .map(column -> checkType(column, CassandraColumnHandle.class, "columnHandle").getColumnMetadata())
                .collect(toList());
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            try {
                for (String tableName : schemaProvider.getAllTables(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName.toLowerCase(ENGLISH)));
                }
            }
            catch (SchemaNotFoundException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getColumnHandles(tableHandle, true).get(SAMPLE_WEIGHT_COLUMN_NAME);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getColumnHandles(tableHandle, false);
    }

    private Map<String, ColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle, boolean includeSampleWeight)
    {
        CassandraTable table = schemaProvider.getTable((CassandraTableHandle) tableHandle);
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (CassandraColumnHandle columnHandle : table.getColumns()) {
            if (includeSampleWeight || !columnHandle.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                columnHandles.put(CassandraCqlUtils.cqlNameToSqlName(columnHandle.getName()).toLowerCase(ENGLISH), columnHandle);
            }
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, CassandraTableHandle.class, "tableHandle");
        return checkType(columnHandle, CassandraColumnHandle.class, "columnHandle").getColumnMetadata();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .toString();
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return true;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "CREATE TABLE not yet supported for Cassandra");
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof CassandraTableHandle, "tableHandle is not an instance of CassandraTableHandle");

        if (!allowDropTable) {
            throw new PrestoException(PERMISSION_DENIED, "DROP TABLE is disabled in this Cassandra catalog");
        }

        CassandraTableHandle cassandraTableHandle = (CassandraTableHandle) tableHandle;
        String schemaName = cassandraTableHandle.getSchemaName();
        String tableName = cassandraTableHandle.getTableName();

        StringBuilder queryBuilder = new StringBuilder(String.format("DROP TABLE \"%s\".\"%s\"", schemaName, tableName));
        cassandraSession.executeQuery(schemaName, queryBuilder.toString());
        schemaProvider.flushTable(cassandraTableHandle.getSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "Renaming tables not yet supported for Cassandra");
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(!isNullOrEmpty(tableMetadata.getOwner()), "Table owner is null or empty");

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        ImmutableList.Builder<ExtraColumnMetadata> columnExtra = ImmutableList.builder();
        columnExtra.add(new ExtraColumnMetadata("id", true));
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
            columnExtra.add(new ExtraColumnMetadata(column.getName(), column.isHidden()));
        }

        // get the root directory for the database
        SchemaTableName table = tableMetadata.getTable();
        String schemaName = schemaProvider.getCaseSensitiveSchemaName(table.getSchemaName());
        String tableName = table.getTableName();
        List<String> columns = columnNames.build();
        List<Type> types = columnTypes.build();
        StringBuilder queryBuilder = new StringBuilder(String.format("CREATE TABLE \"%s\".\"%s\"(id uuid primary key", schemaName, tableName));
        if (tableMetadata.isSampled()) {
            queryBuilder.append(", ").append(SAMPLE_WEIGHT_COLUMN_NAME).append(" ").append(BIGINT.name().toLowerCase(ENGLISH));
            columnExtra.add(new ExtraColumnMetadata(SAMPLE_WEIGHT_COLUMN_NAME, true));
        }
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i);
            Type type = types.get(i);
            queryBuilder.append(", ")
                    .append(name)
                    .append(" ")
                    .append(toCassandraType(type).name().toLowerCase(ENGLISH));
        }
        queryBuilder.append(") ");

        // encode column ordering in the cassandra table comment field since there is no better place to store this
        String columnMetadata = extraColumnMetadataCodec.toJson(columnExtra.build());
        queryBuilder.append("WITH comment='").append(CassandraSession.PRESTO_COMMENT_METADATA).append(" ").append(columnMetadata).append("'");

        // We need create Cassandra table before commit because record need to be written to the table .
        cassandraSession.executeQuery(schemaName, queryBuilder.toString());
        return new CassandraOutputTableHandle(
                connectorId,
                schemaName,
                tableName,
                columnNames.build(),
                columnTypes.build(),
                tableMetadata.isSampled(),
                tableMetadata.getOwner());
    }

    @Override
    public void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        CassandraOutputTableHandle outputTableHandle = checkType(tableHandle, CassandraOutputTableHandle.class, "tableHandle");
        schemaProvider.flushTable(new SchemaTableName(outputTableHandle.getSchemaName(), outputTableHandle.getTableName()));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "INSERT not yet supported for Cassandra");
    }

    @Override
    public void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        throw new PrestoException(NOT_SUPPORTED, "CREATE VIEW not yet supported for Cassandra");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new PrestoException(NOT_SUPPORTED, "DROP VIEW not yet supported for Cassandra");
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }
}
