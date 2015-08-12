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

import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.cassandra.CassandraColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CassandraRecordSink
        implements RecordSink
{
    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    private final int fieldCount;
    private final CassandraSession cassandraSession;
    private final boolean sampled;
    private final String insertQuery;
    private final List<Object> values;
    private final String schemaName;
    private final List<Type> columnTypes;
    private int field = -1;

    @Inject
    public CassandraRecordSink(CassandraOutputTableHandle handle, CassandraSession cassandraSession)
    {
        this.fieldCount = checkNotNull(handle, "handle is null").getColumnNames().size();
        this.cassandraSession = checkNotNull(cassandraSession, "cassandraSession is null");
        this.sampled = handle.isSampled();

        schemaName = handle.getSchemaName();
        StringBuilder queryBuilder = new StringBuilder(String.format("INSERT INTO \"%s\".\"%s\"(", schemaName, handle.getTableName()));
        queryBuilder.append("id");

        if (sampled) {
            queryBuilder.append("," + SAMPLE_WEIGHT_COLUMN_NAME);
        }

        for (String columnName : handle.getColumnNames()) {
            queryBuilder.append(",").append(columnName);
        }
        queryBuilder.append(") VALUES (?");

        if (sampled) {
            queryBuilder.append(",?");
        }

        for (int i = 0; i < handle.getColumnNames().size(); i++) {
            queryBuilder.append(",?");
        }
        queryBuilder.append(")");

        insertQuery = queryBuilder.toString();
        values = new ArrayList<>();

        columnTypes = handle.getColumnTypes();
    }

    @Override
    public void beginRecord(long sampleWeight)
    {
        checkState(field == -1, "already in record");

        field = 0;
        values.clear();
        values.add(UUID.randomUUID());

        if (sampled) {
            values.add(sampleWeight);
        }
        else {
            checkArgument(sampleWeight == 1, "Sample weight must be 1 when sampling is disabled");
        }
    }

    @Override
    public void finishRecord()
    {
        checkState(field != -1, "not in record");
        checkState(field == fieldCount, "not all fields set");
        field = -1;
        cassandraSession.execute(schemaName, insertQuery, values.toArray());
    }

    @Override
    public void appendNull()
    {
        append(null);
    }

    @Override
    public void appendBoolean(boolean value)
    {
        append(value);
    }

    @Override
    public void appendLong(long value)
    {
        if (DATE.equals(columnTypes.get(field))) {
            append(DATE_FORMATTER.print(TimeUnit.DAYS.toMillis(value)));
        }
        else {
            append(value);
        }
    }

    @Override
    public void appendDouble(double value)
    {
        append(value);
    }

    @Override
    public void appendString(byte[] value)
    {
        append(new String(value, UTF_8));
    }

    @Override
    public void appendObject(Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Slice> commit()
    {
        checkState(field == -1, "record not finished");
        // the committer does not need any additional info
        return ImmutableList.of();
    }

    @Override
    public void rollback() {}

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    private void append(Object value)
    {
        checkState(field != -1, "not in record");
        checkState(field < fieldCount, "all fields already set");
        values.add(value);
        field++;
    }
}
