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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.inject.Singleton;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class RaptorModule
        implements Module
{
    private final String connectorId;

    public RaptorModule(String connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(RaptorConnectorId.class).toInstance(new RaptorConnectorId(connectorId));
        binder.bind(RaptorConnector.class).in(Scopes.SINGLETON);
        binder.bind(RaptorMetadata.class).in(Scopes.SINGLETON);
        binder.bind(RaptorSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(RaptorPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(RaptorPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(RaptorHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(RaptorSessionProperties.class).in(Scopes.SINGLETON);

        binder.bind(ShardManager.class).to(DatabaseShardManager.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(ShardInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(ShardDelta.class);
    }

    @ForMetadata
    @Singleton
    @Provides
    public IDBI createDBI(@ForMetadata ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        DBI dbi = new DBI(connectionFactory);
        dbi.registerMapper(new TableColumn.Mapper(typeManager));
        return dbi;
    }
}
