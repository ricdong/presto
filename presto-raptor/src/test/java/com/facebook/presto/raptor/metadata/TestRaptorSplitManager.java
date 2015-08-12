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

import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.RaptorMetadata;
import com.facebook.presto.raptor.RaptorSplitManager;
import com.facebook.presto.raptor.RaptorTableHandle;
import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.backup.FileBackupStore;
import com.facebook.presto.raptor.storage.FileStorageService;
import com.facebook.presto.raptor.storage.ShardRecoveryManager;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StorageService;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.shardInfo;
import static com.facebook.presto.raptor.storage.TestOrcStorageManager.createOrcStorageManager;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRaptorSplitManager
{
    private static final JsonCodec<ShardInfo> SHARD_INFO_CODEC = jsonCodec(ShardInfo.class);
    private static final JsonCodec<ShardDelta> SHARD_DELTA_CODEC = jsonCodec(ShardDelta.class);
    private static final ConnectorTableMetadata TEST_TABLE = TableMetadataBuilder.tableMetadataBuilder("demo", "test_table")
            .partitionKeyColumn("ds", VARCHAR)
            .column("foo", VARCHAR)
            .column("bar", BigintType.BIGINT)
            .build();

    private Handle dummyHandle;
    private File temporary;
    private RaptorSplitManager raptorSplitManager;
    private ConnectorTableHandle tableHandle;
    private ShardManager shardManager;
    private StorageManager storageManagerWithBackup;
    private long tableId;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        DBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dbi.registerMapper(new TableColumn.Mapper(typeRegistry));
        dummyHandle = dbi.open();
        temporary = createTempDir();
        shardManager = new DatabaseShardManager(dbi);
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        File dataDir = new File(temporary, "data");
        File backupDir = new File(temporary, "backup");
        FileBackupStore fileBackupStore = new FileBackupStore(backupDir);
        fileBackupStore.start();
        Optional<BackupStore> backupStore = Optional.of(fileBackupStore);

        StorageService storageService = new FileStorageService(dataDir);
        ShardRecoveryManager recoveryManager = new ShardRecoveryManager(storageService, Optional.empty(), new InMemoryNodeManager(), shardManager, new Duration(5, MINUTES), 10);
        StorageManager storageManager = createOrcStorageManager(storageService, Optional.empty(), recoveryManager);
        storageManagerWithBackup = createOrcStorageManager(storageService, backupStore, recoveryManager);

        String nodeName = UUID.randomUUID().toString();
        nodeManager.addNode("raptor", new PrestoNode(nodeName, new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN));

        RaptorConnectorId connectorId = new RaptorConnectorId("raptor");
        RaptorMetadata metadata = new RaptorMetadata(connectorId, dbi, shardManager, SHARD_INFO_CODEC, SHARD_DELTA_CODEC);

        metadata.createTable(SESSION, TEST_TABLE);
        tableHandle = metadata.getTableHandle(SESSION, TEST_TABLE.getTable());

        List<ShardInfo> shards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .build();

        tableId = checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getTableId();

        List<ColumnInfo> columns = metadata.getColumnHandles(SESSION, tableHandle).values().stream()
                .map(handle -> checkType(handle, RaptorColumnHandle.class, "columnHandle"))
                .map(ColumnInfo::fromHandle)
                .collect(toList());

        shardManager.commitShards(tableId, columns, shards, Optional.empty());

        raptorSplitManager = new RaptorSplitManager(connectorId, nodeManager, shardManager, storageManager);
    }

    @AfterMethod
    public void teardown()
    {
        dummyHandle.close();
        deleteRecursively(temporary);
    }

    @Test
    public void testSanity()
            throws InterruptedException
    {
        ConnectorPartitionResult partitionResult = raptorSplitManager.getPartitions(SESSION, tableHandle, TupleDomain.<ColumnHandle>all());
        assertEquals(partitionResult.getPartitions().size(), 1);
        assertTrue(partitionResult.getUndeterminedTupleDomain().isAll());

        List<ConnectorPartition> partitions = partitionResult.getPartitions();
        ConnectorPartition partition = getOnlyElement(partitions);
        TupleDomain<ColumnHandle> columnUnionedTupleDomain = TupleDomain.columnWiseUnion(partition.getTupleDomain(), partition.getTupleDomain());
        assertEquals(columnUnionedTupleDomain, TupleDomain.<ColumnHandle>all());

        ConnectorSplitSource splitSource = raptorSplitManager.getPartitionSplits(SESSION, tableHandle, partitions);
        int splitCount = 0;
        while (!splitSource.isFinished()) {
            splitCount += getFutureValue(splitSource.getNextBatch(1000)).size();
        }
        assertEquals(splitCount, 4);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No host for shard .* found: \\[\\]")
    public void testNoHostForShard()
            throws InterruptedException
    {
        deleteShardNodes();

        ConnectorPartitionResult result = raptorSplitManager.getPartitions(SESSION, tableHandle, TupleDomain.<ColumnHandle>all());

        ConnectorSplitSource splitSource = raptorSplitManager.getPartitionSplits(SESSION, tableHandle, result.getPartitions());
        getFutureValue(splitSource.getNextBatch(1000));
    }

    @Test
    public void testAssignRandomNodeWhenBackupAvailable()
            throws InterruptedException, URISyntaxException
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        PrestoNode node = new PrestoNode(UUID.randomUUID().toString(), new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN);
        nodeManager.addNode("fbraptor", node);
        RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(new RaptorConnectorId("fbraptor"), nodeManager, shardManager, storageManagerWithBackup);

        deleteShardNodes();

        ConnectorPartitionResult result = raptorSplitManagerWithBackup.getPartitions(SESSION, tableHandle, TupleDomain.<ColumnHandle>all());
        ConnectorSplitSource partitionSplit = raptorSplitManagerWithBackup.getPartitionSplits(SESSION, tableHandle, result.getPartitions());
        List<ConnectorSplit> batch = getFutureValue(partitionSplit.getNextBatch(1), PrestoException.class);
        assertEquals(getOnlyElement(getOnlyElement(batch).getAddresses()), node.getHostAndPort());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No nodes available to run query")
    public void testNoNodes()
            throws InterruptedException, URISyntaxException
    {
        deleteShardNodes();

        RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(new RaptorConnectorId("fbraptor"), new InMemoryNodeManager(), shardManager, storageManagerWithBackup);
        ConnectorPartitionResult result = raptorSplitManagerWithBackup.getPartitions(SESSION, tableHandle, TupleDomain.<ColumnHandle>all());
        ConnectorSplitSource splitSource = raptorSplitManagerWithBackup.getPartitionSplits(SESSION, tableHandle, result.getPartitions());
        getFutureValue(splitSource.getNextBatch(1000), PrestoException.class);
    }

    private void deleteShardNodes()
    {
        dummyHandle.execute("DELETE FROM shard_nodes");
        dummyHandle.execute(format("UPDATE %s SET node_ids = ''", shardIndexTable(tableId)));
    }
}
