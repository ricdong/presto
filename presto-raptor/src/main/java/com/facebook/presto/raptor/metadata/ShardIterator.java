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
import com.facebook.presto.raptor.util.CloseableIterator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.AbstractIterator;
import io.airlift.log.Logger;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.DBIException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Wrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.util.ArrayUtil.intArrayFromBytes;
import static com.facebook.presto.raptor.util.UuidUtil.uuidFromBytes;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

final class ShardIterator
        extends AbstractIterator<ShardNodes>
        implements CloseableIterator<ShardNodes>
{
    private static final Logger log = Logger.get(ShardIterator.class);
    private final Map<Integer, String> nodeMap = new HashMap<>();

    private final ShardManagerDao dao;
    private final Connection connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;

    public ShardIterator(long tableId, TupleDomain<RaptorColumnHandle> effectivePredicate, IDBI dbi)
    {
        ShardPredicate predicate = ShardPredicate.create(effectivePredicate);

        String sql = format(
                "SELECT shard_uuid, node_ids FROM %s WHERE %s",
                shardIndexTable(tableId),
                predicate.getPredicate());

        dao = dbi.onDemand(ShardManagerDao.class);
        fetchNodes();

        try {
            connection = dbi.open().getConnection();
            statement = connection.prepareStatement(sql);
            enableStreamingResults(statement);
            predicate.bind(statement);
            log.debug("Running query: %s", statement);
            resultSet = statement.executeQuery();
        }
        catch (SQLException e) {
            close();
            throw new PrestoException(RAPTOR_ERROR, e);
        }
    }

    @Override
    protected ShardNodes computeNext()
    {
        try {
            return compute();
        }
        catch (SQLException e) {
            throw new PrestoException(RAPTOR_ERROR, e);
        }
    }

    @SuppressWarnings({"UnusedDeclaration", "EmptyTryBlock"})
    @Override
    public void close()
    {
        // use try-with-resources to close everything properly
        try (ResultSet resultSet = this.resultSet;
                Statement statement = this.statement;
                Connection connection = this.connection) {
            // do nothing
        }
        catch (SQLException ignored) {
        }
    }

    private ShardNodes compute()
            throws SQLException
    {
        if (!resultSet.next()) {
            return endOfData();
        }

        UUID shardUuid = uuidFromBytes(resultSet.getBytes("shard_uuid"));
        List<Integer> nodeIds = intArrayFromBytes(resultSet.getBytes("node_ids"));

        Function<Integer, String> fetchNode = id -> fetchNode(id, shardUuid);
        Set<String> nodeIdentifiers = nodeIds.stream()
                .map(id -> nodeMap.computeIfAbsent(id, fetchNode))
                .collect(toSet());

        return new ShardNodes(shardUuid, nodeIdentifiers);
    }

    private String fetchNode(int id, UUID shardUuid)
    {
        String node = fetchNode(id);
        if (node == null) {
            throw new PrestoException(RAPTOR_ERROR, format("Missing node ID [%s] for shard: %s", id, shardUuid));
        }
        return node;
    }

    private String fetchNode(int id)
    {
        try {
            return dao.getNodeIdentifier(id);
        }
        catch (DBIException e) {
            throw new PrestoException(RAPTOR_ERROR, e);
        }
    }

    private void fetchNodes()
    {
        try {
            for (Node node : dao.getNodes()) {
                nodeMap.put(node.getNodeId(), node.getNodeIdentifier());
            }
        }
        catch (DBIException e) {
            throw new PrestoException(RAPTOR_ERROR, e);
        }
    }

    private static void enableStreamingResults(Statement statement)
            throws SQLException
    {
        if (isWrapperFor(statement, com.mysql.jdbc.Statement.class)) {
            statement.unwrap(com.mysql.jdbc.Statement.class).enableStreamingResults();
        }
    }

    private static boolean isWrapperFor(Wrapper wrapper, Class<?> clazz)
    {
        try {
            return wrapper.isWrapperFor(clazz);
        }
        catch (SQLException ignored) {
            return false;
        }
    }
}
