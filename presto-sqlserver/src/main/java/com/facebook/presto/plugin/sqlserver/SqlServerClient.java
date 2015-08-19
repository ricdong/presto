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
package com.facebook.presto.plugin.sqlserver;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;

import javax.inject.Inject;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import net.sourceforge.jtds.jdbc.Driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import static java.util.Locale.ENGLISH;

/**
 * Created by ricdong on 15-8-12.
 */
public class SqlServerClient extends BaseJdbcClient {

    @Inject
    public SqlServerClient(JdbcConnectorId connectorId,
                           BaseJdbcConfig config, SqlServerConfig sqlServerConfig) {
        super(connectorId, config, "\"", new Driver());

        connectionProperties.setProperty("dbType", TYPE_SQLSERVER);
    }

//    @Override
//    public Set<String> getSchemaNames()
//    {
//        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
//
//             ResultSet resultSet = connection.getMetaData().getCatalogs()) {
//            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
//            while (resultSet.next()) {
//                String schemaName = resultSet.getString("TABLE_CAT").toLowerCase(ENGLISH);
//                // skip internal schemas
//                if (!schemaName.equals("information_schema") && !schemaName.equals("mysql")) {
//                    schemaNames.add(schemaName);
//                }
//            }
//            return schemaNames.build();
//        }
//        catch (SQLException e) {
//            e.printStackTrace();
//            throw Throwables.propagate(e);
//        }
//
//    }
//
    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        // for sqlserver ,we use the schema name instead of catalog.
        System.out.println("this here, use the subclass");
        return connection.getMetaData().getTables(schemaName, null, null, new String[]{"TABLE"});
    }
}
