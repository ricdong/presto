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
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.cassandra.util.Types.checkType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;

public class CassandraRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(CassandraRecordSetProvider.class);

    private final String connectorId;
    private final CassandraSession cassandraSession;

    @Inject
    public CassandraRecordSetProvider(CassandraConnectorId connectorId, CassandraSession cassandraSession)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.cassandraSession = checkNotNull(cassandraSession, "cassandraSession is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        CassandraSplit cassandraSplit = checkType(split, CassandraSplit.class, "split");

        List<CassandraColumnHandle> cassandraColumns = columns.stream()
                .map(column -> checkType(column, CassandraColumnHandle.class, "columnHandle"))
                .collect(toList());

        String selectCql = CassandraCqlUtils.selectFrom(cassandraSplit.getCassandraTableHandle(), cassandraColumns).getQueryString();
        StringBuilder sb = new StringBuilder(selectCql);
        if (sb.charAt(sb.length() - 1) == ';') {
            sb.setLength(sb.length() - 1);
        }
        sb.append(cassandraSplit.getWhereClause());
        String cql = sb.toString();
        log.debug("Creating record set: %s", cql);

        return new CassandraRecordSet(cassandraSession, cassandraSplit.getSchema(), cql, cassandraColumns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .toString();
    }
}
