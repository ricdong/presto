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

import com.facebook.presto.Session;
import com.facebook.presto.benchmark.BenchmarkSuite;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getOnlyElement;

public final class RaptorBenchmarkQueryRunner
{
    private static final String TPCH_CACHE_DIR = System.getProperty("tpchCacheDir", "/tmp/presto_tpch/data_cache");

    private RaptorBenchmarkQueryRunner()
    {
    }

    public static void main(String[] args)
            throws IOException
    {
        String outputDirectory = checkNotNull(System.getProperty("outputDirectory"), "Must specify -DoutputDirectory=...");
        try (LocalQueryRunner localQueryRunner = createLocalQueryRunner()) {
            new BenchmarkSuite(localQueryRunner, outputDirectory).runAllBenchmarks();
        }
    }

    public static LocalQueryRunner createLocalQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog("default")
                .setSchema("default")
                .build();
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session);

        // add tpch
        InMemoryNodeManager nodeManager = localQueryRunner.getNodeManager();
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(nodeManager, 1), ImmutableMap.<String, String>of());

        // add raptor
        ConnectorFactory raptorConnectorFactory = createRaptorConnectorFactory(TPCH_CACHE_DIR, nodeManager);
        localQueryRunner.createCatalog("default", raptorConnectorFactory, ImmutableMap.<String, String>of());

        Metadata metadata = localQueryRunner.getMetadata();
        if (!metadata.getTableHandle(session, new QualifiedTableName("default", "default", "orders")).isPresent()) {
            localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.sf1.orders");
        }
        if (!metadata.getTableHandle(session, new QualifiedTableName("default", "default", "lineitem")).isPresent()) {
            localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.sf1.lineitem");
        }
        return localQueryRunner;
    }

    private static ConnectorFactory createRaptorConnectorFactory(String cacheDir, NodeManager nodeManager)
    {
        try {
            File dataDir = new File(cacheDir);
            File databaseDir = new File(dataDir, "db");

            Map<String, String> config = ImmutableMap.<String, String>builder()
                    .put("metadata.db.type", "h2")
                    .put("metadata.db.filename", databaseDir.getAbsolutePath())
                    .put("storage.data-directory", dataDir.getAbsolutePath())
                    .put("storage.compress", "false")
                    .build();

            TypeManager typeManager = new TypeRegistry();
            BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager(typeManager);

            RaptorPlugin plugin = new RaptorPlugin();

            plugin.setOptionalConfig(config);
            plugin.setNodeManager(nodeManager);
            plugin.setBlockEncodingSerde(blockEncodingSerde);
            plugin.setTypeManager(typeManager);

            return getOnlyElement(plugin.getServices(ConnectorFactory.class));
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
