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

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

public class Node
{
    private final int nodeId;
    private final String nodeIdentifier;

    public Node(int nodeId, String nodeIdentifier)
    {
        this.nodeId = nodeId;
        this.nodeIdentifier = checkNotNull(nodeIdentifier, "nodeIdentifier is null");
    }

    public int getNodeId()
    {
        return nodeId;
    }

    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    public static class Mapper
            implements ResultSetMapper<Node>
    {
        @Override
        public Node map(int index, ResultSet rs, StatementContext ctx)
                throws SQLException
        {
            return new Node(
                    rs.getInt("node_id"),
                    rs.getString("node_identifier"));
        }
    }
}
