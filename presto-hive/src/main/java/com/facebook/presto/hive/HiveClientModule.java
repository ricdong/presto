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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.hive.orc.DwrfPageSourceFactory;
import com.facebook.presto.hive.orc.DwrfRecordCursorProvider;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.hive.orc.OrcRecordCursorProvider;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HiveClientModule
        implements Module
{
    private final String connectorId;
    private final HiveMetastore metastore;
    private final TypeManager typeManager;

    public HiveClientModule(String connectorId, HiveMetastore metastore, TypeManager typeManager)
    {
        this.connectorId = connectorId;
        this.metastore = metastore;
        this.typeManager = typeManager;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(HiveConnectorId.class).toInstance(new HiveConnectorId(connectorId));

        binder.bind(HdfsConfigurationUpdater.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        binder.bind(DirectoryLister.class).to(HadoopDirectoryLister.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(HiveClientConfig.class);

        binder.bind(HiveSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveTableProperties.class).in(Scopes.SINGLETON);

        if (metastore != null) {
            binder.bind(HiveMetastore.class).toInstance(metastore);
        }
        else {
            binder.bind(HiveMetastore.class).to(CachingHiveMetastore.class).in(Scopes.SINGLETON);
            newExporter(binder).export(HiveMetastore.class)
                    .as(generatedNameOf(CachingHiveMetastore.class, connectorId));
        }

        binder.bind(NamenodeStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NamenodeStats.class).as(generatedNameOf(NamenodeStats.class));

        binder.bind(HiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(HiveCluster.class).to(StaticHiveCluster.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticMetastoreConfig.class);

        binder.bind(TypeManager.class).toInstance(typeManager);

        Multibinder<HiveRecordCursorProvider> recordCursorProviderBinder = Multibinder.newSetBinder(binder, HiveRecordCursorProvider.class);
        recordCursorProviderBinder.addBinding().to(OrcRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(ParquetRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(DwrfRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(ColumnarTextHiveRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(ColumnarBinaryHiveRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(GenericHiveRecordCursorProvider.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorMetadata.class).to(HiveMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(HiveSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(HivePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSinkProvider.class).to(HiveRecordSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorHandleResolver.class).to(HiveHandleResolver.class).in(Scopes.SINGLETON);

        Multibinder<HivePageSourceFactory> pageSourceFactoryBinder = Multibinder.newSetBinder(binder, HivePageSourceFactory.class);
        pageSourceFactoryBinder.addBinding().to(RcFilePageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(OrcPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(DwrfPageSourceFactory.class).in(Scopes.SINGLETON);

        binder.bind(PrestoS3FileSystemStats.class).toInstance(PrestoS3FileSystem.getFileSystemStats());
        newExporter(binder).export(PrestoS3FileSystemStats.class).as(generatedNameOf(PrestoS3FileSystem.class, connectorId));
    }

    @ForHiveClient
    @Singleton
    @Provides
    public ExecutorService createHiveClientExecutor(HiveConnectorId hiveClientId)
    {
        return newCachedThreadPool(daemonThreadsNamed("hive-" + hiveClientId + "-%s"));
    }

    @ForHiveMetastore
    @Singleton
    @Provides
    public ExecutorService createCachingHiveMetastoreExecutor(HiveConnectorId hiveClientId, HiveClientConfig hiveClientConfig)
    {
        return newFixedThreadPool(
                hiveClientConfig.getMaxMetastoreRefreshThreads(),
                daemonThreadsNamed("hive-metastore-" + hiveClientId + "-%s"));
    }
}
