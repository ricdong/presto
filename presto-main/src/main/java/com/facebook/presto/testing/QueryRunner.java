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
package com.facebook.presto.testing;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.Plugin;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

public interface QueryRunner
        extends Closeable
{
    @Override
    void close();

    int getNodeCount();

    Session getDefaultSession();

    Metadata getMetadata();

    MaterializedResult execute(@Language("SQL") String sql);

    MaterializedResult execute(Session session, @Language("SQL") String sql);

    List<QualifiedTableName> listTables(Session session, String catalog, String schema);

    boolean tableExists(Session session, String table);

    void installPlugin(Plugin plugin);

    void createCatalog(String catalogName, String connectorName, Map<String, String> properties);
}
