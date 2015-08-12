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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.DEFAULT_PRECISION;
import static com.facebook.presto.util.DateTimeUtils.parseDate;
import static com.facebook.presto.util.DateTimeUtils.parseTime;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZone;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;

public class TestingPrestoClient
        extends AbstractTestingPrestoClient<MaterializedResult>
{
    private static final Logger log = Logger.get("TestQueries");

    public TestingPrestoClient(TestingPrestoServer prestoServer, Session defaultSession)
    {
        super(prestoServer, defaultSession);
    }

    @Override
    protected ResultsSession<MaterializedResult> getResultSession(Session session)
    {
        return new MaterializedResultSession(session);
    }

    private class MaterializedResultSession
            implements ResultsSession<MaterializedResult>
    {
        private final ImmutableList.Builder<MaterializedRow> rows = ImmutableList.builder();
        private final AtomicBoolean loggedUri = new AtomicBoolean(false);

        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        private final TimeZoneKey timeZoneKey;

        private MaterializedResultSession(Session session)
        {
            this.timeZoneKey = session.getTimeZoneKey();
        }

        @Override
        public void addResults(QueryResults results)
        {
            if (!loggedUri.getAndSet(true)) {
                log.info("Query %s: %s?pretty", results.getId(), results.getInfoUri());
            }

            if (types.get() == null && results.getColumns() != null) {
                types.set(getTypes(results.getColumns()));
            }

            if (results.getData() != null) {
                checkState(types.get() != null, "data received without types");
                rows.addAll(transform(results.getData(), dataToRow(timeZoneKey, types.get())));
            }
        }

        @Override
        public MaterializedResult build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            checkState(types.get() != null, "never received types for the query");
            return new MaterializedResult(rows.build(), types.get(), setSessionProperties, resetSessionProperties);
        }
    }

    private static Function<List<Object>, MaterializedRow> dataToRow(final TimeZoneKey timeZoneKey, final List<Type> types)
    {
        return new Function<List<Object>, MaterializedRow>()
        {
            @Override
            public MaterializedRow apply(List<Object> data)
            {
                checkArgument(data.size() == types.size(), "columns size does not match types size");
                List<Object> row = new ArrayList<>();
                for (int i = 0; i < data.size(); i++) {
                    Object value = data.get(i);
                    if (value == null) {
                        row.add(null);
                        continue;
                    }

                    Type type = types.get(i);
                    if (BOOLEAN.equals(type)) {
                        row.add(value);
                    }
                    else if (BIGINT.equals(type)) {
                        row.add(((Number) value).longValue());
                    }
                    else if (DOUBLE.equals(type)) {
                        row.add(((Number) value).doubleValue());
                    }
                    else if (VARCHAR.equals(type)) {
                        row.add(value);
                    }
                    else if (VARBINARY.equals(type)) {
                        row.add(value);
                    }
                    else if (DATE.equals(type)) {
                        int days = parseDate((String) value);
                        row.add(new Date(TimeUnit.DAYS.toMillis(days)));
                    }
                    else if (TIME.equals(type)) {
                        row.add(new Time(parseTime(timeZoneKey, (String) value)));
                    }
                    else if (TIME_WITH_TIME_ZONE.equals(type)) {
                        row.add(new Time(unpackMillisUtc(parseTimeWithTimeZone((String) value))));
                    }
                    else if (TIMESTAMP.equals(type)) {
                        row.add(new Timestamp(parseTimestampWithoutTimeZone(timeZoneKey, (String) value)));
                    }
                    else if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
                        row.add(new Timestamp(unpackMillisUtc(parseTimestampWithTimeZone(timeZoneKey, (String) value))));
                    }
                    else {
                        throw new AssertionError("unhandled type: " + type);
                    }
                }
                return new MaterializedRow(DEFAULT_PRECISION, row);
            }
        };
    }
}
