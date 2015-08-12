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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ColumnStats;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.storage.OrcFileRewriter.OrcFileInfo;
import com.facebook.presto.raptor.util.CurrentNodeId;
import com.facebook.presto.raptor.util.PageBuffer;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_RECOVERY_ERROR;
import static com.facebook.presto.raptor.storage.ShardStats.computeColumnStats;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.Math.min;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static org.joda.time.DateTimeZone.UTC;

public class OrcStorageManager
        implements StorageManager
{
    private static final long MAX_ROWS = 1_000_000_000;

    private final String nodeId;
    private final StorageService storageService;
    private final Optional<BackupStore> backupStore;
    private final JsonCodec<ShardDelta> shardDeltaCodec;
    private final DataSize orcMaxMergeDistance;
    private final DataSize orcMaxReadSize;
    private final DataSize orcStreamBufferSize;
    private final ShardRecoveryManager recoveryManager;
    private final Duration recoveryTimeout;
    private final long maxShardRows;
    private final DataSize maxShardSize;
    private final DataSize maxBufferSize;
    private final StorageManagerStats stats;

    @Inject
    public OrcStorageManager(
            CurrentNodeId currentNodeId,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            JsonCodec<ShardDelta> shardDeltaCodec,
            StorageManagerConfig config,
            ShardRecoveryManager recoveryManager)
    {
        this(currentNodeId.toString(),
                storageService,
                backupStore,
                shardDeltaCodec,
                config.getOrcMaxMergeDistance(),
                config.getOrcMaxReadSize(),
                config.getOrcStreamBufferSize(),
                recoveryManager,
                config.getShardRecoveryTimeout(),
                config.getMaxShardRows(),
                config.getMaxShardSize(),
                config.getMaxBufferSize());
    }

    public OrcStorageManager(
            String nodeId,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            JsonCodec<ShardDelta> shardDeltaCodec,
            DataSize orcMaxMergeDistance,
            DataSize orcMaxReadSize,
            DataSize orcStreamBufferSize,
            ShardRecoveryManager recoveryManager,
            Duration shardRecoveryTimeout,
            long maxShardRows,
            DataSize maxShardSize,
            DataSize maxBufferSize)
    {
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
        this.storageService = checkNotNull(storageService, "storageService is null");
        this.backupStore = checkNotNull(backupStore, "backupStore is null");
        this.shardDeltaCodec = checkNotNull(shardDeltaCodec, "shardDeltaCodec is null");
        this.orcMaxMergeDistance = checkNotNull(orcMaxMergeDistance, "orcMaxMergeDistance is null");
        this.orcMaxReadSize = checkNotNull(orcMaxReadSize, "orcMaxReadSize is null");
        this.orcStreamBufferSize = checkNotNull(orcStreamBufferSize, "orcStreamBufferSize is null");

        this.recoveryManager = checkNotNull(recoveryManager, "recoveryManager is null");
        this.recoveryTimeout = checkNotNull(shardRecoveryTimeout, "shardRecoveryTimeout is null");

        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = min(maxShardRows, MAX_ROWS);
        this.maxShardSize = checkNotNull(maxShardSize, "maxShardSize is null");
        this.maxBufferSize = checkNotNull(maxBufferSize, "maxBufferSize is null");
        this.stats = new StorageManagerStats();
    }

    @Override
    public ConnectorPageSource getPageSource(UUID shardUuid, List<Long> columnIds, List<Type> columnTypes, TupleDomain<RaptorColumnHandle> effectivePredicate)
    {
        OrcDataSource dataSource = openShard(shardUuid);

        try {
            OrcReader reader = new OrcReader(dataSource, new OrcMetadataReader());

            Map<Long, Integer> indexMap = columnIdIndex(reader.getColumnNames());
            ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
            ImmutableList.Builder<Integer> columnIndexes = ImmutableList.builder();
            for (int i = 0; i < columnIds.size(); i++) {
                long columnId = columnIds.get(i);
                if (RaptorColumnHandle.isShardRowIdColumn(columnId)) {
                    columnIndexes.add(OrcPageSource.ROWID_COLUMN);
                    continue;
                }

                Integer index = indexMap.get(columnId);
                if (index == null) {
                    columnIndexes.add(OrcPageSource.NULL_COLUMN);
                }
                else {
                    columnIndexes.add(index);
                    includedColumns.put(index, columnTypes.get(i));
                }
            }

            OrcPredicate predicate = getPredicate(effectivePredicate, indexMap);

            OrcRecordReader recordReader = reader.createRecordReader(includedColumns.build(), predicate, UTC);

            ShardRewriter shardRewriter = rowsToDelete -> rewriteShard(shardUuid, rowsToDelete);

            return new OrcPageSource(shardRewriter, recordReader, dataSource, columnIds, columnTypes, columnIndexes.build());
        }
        catch (IOException | RuntimeException e) {
            try {
                dataSource.close();
            }
            catch (IOException ex) {
                e.addSuppressed(ex);
            }
            throw new PrestoException(RAPTOR_ERROR, "Failed to create page source for shard " + shardUuid, e);
        }
    }

    @Override
    public StoragePageSink createStoragePageSink(List<Long> columnIds, List<Type> columnTypes)
    {
        return new OrcStoragePageSink(columnIds, columnTypes);
    }

    private void writeShard(UUID shardUuid)
    {
        File stagingFile = storageService.getStagingFile(shardUuid);
        File storageFile = storageService.getStorageFile(shardUuid);

        storageService.createParents(storageFile);

        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to move shard file", e);
        }

        if (isBackupAvailable()) {
            long start = System.nanoTime();
            backupStore.get().backupShard(shardUuid, storageFile);
            stats.addCopyShardDataRate(new DataSize(storageFile.length(), BYTE), nanosSince(start));
        }
    }

    @Override
    public PageBuffer createPageBuffer()
    {
        return new PageBuffer(maxBufferSize.toBytes(), Integer.MAX_VALUE);
    }

    @Override
    public boolean isBackupAvailable()
    {
        return backupStore.isPresent();
    }

    @Managed
    @Flatten
    public StorageManagerStats getStats()
    {
        return stats;
    }

    @VisibleForTesting
    OrcDataSource openShard(UUID shardUuid)
    {
        File file = storageService.getStorageFile(shardUuid).getAbsoluteFile();

        if (!file.exists() && backupExists(shardUuid)) {
            try {
                Future<?> future = recoveryManager.recoverShard(shardUuid);
                future.get(recoveryTimeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (ExecutionException e) {
                throw new PrestoException(RAPTOR_RECOVERY_ERROR, "Error recovering shard " + shardUuid, e.getCause());
            }
            catch (TimeoutException e) {
                throw new PrestoException(RAPTOR_ERROR, "Shard is being recovered from backup. Please retry in a few minutes: " + shardUuid);
            }
        }

        try {
            return new FileOrcDataSource(file, orcMaxMergeDistance, orcMaxMergeDistance, orcMaxMergeDistance);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to open shard file: " + file, e);
        }
    }

    private boolean backupExists(UUID shardUuid)
    {
        return backupStore.isPresent() && backupStore.get().shardSize(shardUuid).isPresent();
    }

    private ShardInfo createShardInfo(UUID shardUuid, File file, Set<String> nodes, long rowCount, long uncompressedSize)
    {
        return new ShardInfo(shardUuid, nodes, computeShardStats(file), rowCount, file.length(), uncompressedSize);
    }

    private List<ColumnStats> computeShardStats(File file)
    {
        try (OrcDataSource dataSource = new FileOrcDataSource(file, orcMaxMergeDistance, orcMaxReadSize, orcStreamBufferSize)) {
            OrcReader reader = new OrcReader(dataSource, new OrcMetadataReader());

            ImmutableList.Builder<ColumnStats> list = ImmutableList.builder();
            for (ColumnInfo info : getColumnInfo(reader)) {
                computeColumnStats(reader, info.getColumnId(), info.getType()).ifPresent(list::add);
            }
            return list.build();
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to read file: " + file, e);
        }
    }

    private Collection<Slice> rewriteShard(UUID shardUuid, BitSet rowsToDelete)
    {
        if (rowsToDelete.isEmpty()) {
            return ImmutableList.of();
        }

        UUID newShardUuid = UUID.randomUUID();
        File input = storageService.getStorageFile(shardUuid);
        File output = storageService.getStagingFile(newShardUuid);

        OrcFileInfo info = rewriteFile(input, output, rowsToDelete);
        long rowCount = info.getRowCount();

        if (rowCount == 0) {
            return shardDelta(shardUuid, Optional.empty());
        }

        Set<String> nodes = ImmutableSet.of(nodeId);
        long uncompressedSize = info.getUncompressedSize();

        ShardInfo shard = createShardInfo(newShardUuid, output, nodes, rowCount, uncompressedSize);

        writeShard(newShardUuid);

        return shardDelta(shardUuid, Optional.of(shard));
    }

    private Collection<Slice> shardDelta(UUID oldShardUuid, Optional<ShardInfo> shardInfo)
    {
        List<ShardInfo> newShards = shardInfo.map(ImmutableList::of).orElse(ImmutableList.of());
        ShardDelta delta = new ShardDelta(ImmutableList.of(oldShardUuid), newShards);
        return ImmutableList.of(Slices.wrappedBuffer(shardDeltaCodec.toJsonBytes(delta)));
    }

    private static OrcFileInfo rewriteFile(File input, File output, BitSet rowsToDelete)
    {
        try {
            return OrcFileRewriter.rewrite(input, output, rowsToDelete);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to rewrite shard file: " + input, e);
        }
    }

    private static List<ColumnInfo> getColumnInfo(OrcReader reader)
    {
        // TODO: These should be stored as proper metadata.
        // XXX: Relying on ORC types will not work when more Presto types are supported.

        List<String> names = reader.getColumnNames();
        List<OrcType> types = reader.getFooter().getTypes();
        types = types.subList(1, types.size()); // skip struct
        if (names.size() != types.size()) {
            throw new PrestoException(RAPTOR_ERROR, "Column names and types do not match");
        }

        ImmutableList.Builder<ColumnInfo> list = ImmutableList.builder();
        for (int i = 0; i < names.size(); i++) {
            list.add(new ColumnInfo(Long.parseLong(names.get(i)), getType(types.get(i))));
        }
        return list.build();
    }

    private static Type getType(OrcType type)
    {
        switch (type.getOrcTypeKind()) {
            case BOOLEAN:
                return BOOLEAN;
            case LONG:
                return BIGINT;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return VARCHAR;
            case BINARY:
                return VARBINARY;
        }
        throw new PrestoException(RAPTOR_ERROR, "Unhandled ORC type: " + type);
    }

    private static OrcPredicate getPredicate(TupleDomain<RaptorColumnHandle> effectivePredicate, Map<Long, Integer> indexMap)
    {
        ImmutableList.Builder<ColumnReference<RaptorColumnHandle>> columns = ImmutableList.builder();
        for (RaptorColumnHandle column : effectivePredicate.getDomains().keySet()) {
            Integer index = indexMap.get(column.getColumnId());
            if (index != null) {
                columns.add(new ColumnReference<>(column, index, column.getColumnType()));
            }
        }
        return new TupleDomainOrcPredicate<>(effectivePredicate, columns.build());
    }

    private static Map<Long, Integer> columnIdIndex(List<String> columnNames)
    {
        ImmutableMap.Builder<Long, Integer> map = ImmutableMap.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(Long.valueOf(columnNames.get(i)), i);
        }
        return map.build();
    }

    private class OrcStoragePageSink
            implements StoragePageSink
    {
        private final List<Long> columnIds;
        private final List<Type> columnTypes;

        private final List<ShardInfo> shards = new ArrayList<>();

        private boolean committed;
        private OrcFileWriter writer;
        private UUID shardUuid;

        public OrcStoragePageSink(List<Long> columnIds, List<Type> columnTypes)
        {
            this.columnIds = ImmutableList.copyOf(checkNotNull(columnIds, "columnIds is null"));
            this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));
        }

        @Override
        public void appendPages(List<Page> pages)
        {
            createWriterIfNecessary();
            writer.appendPages(pages);
        }

        @Override
        public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
        {
            createWriterIfNecessary();
            writer.appendPages(inputPages, pageIndexes, positionIndexes);
        }

        @Override
        public void appendRow(Row row)
        {
            createWriterIfNecessary();
            writer.appendRow(row);
        }

        @Override
        public boolean isFull()
        {
            if (writer == null) {
                return false;
            }
            return (writer.getRowCount() >= maxShardRows) || (writer.getUncompressedSize() >= maxShardSize.toBytes());
        }

        @Override
        public void flush()
        {
            if (writer != null) {
                writer.close();

                File stagingFile = storageService.getStagingFile(shardUuid);

                Set<String> nodes = ImmutableSet.of(nodeId);
                long rowCount = writer.getRowCount();
                long uncompressedSize = writer.getUncompressedSize();

                shards.add(createShardInfo(shardUuid, stagingFile, nodes, rowCount, uncompressedSize));

                writer = null;
                shardUuid = null;
            }
        }

        @Override
        public List<ShardInfo> commit()
        {
            checkState(!committed, "already committed");
            committed = true;

            flush();
            for (ShardInfo shard : shards) {
                writeShard(shard.getShardUuid());
            }
            return ImmutableList.copyOf(shards);
        }

        @Override
        public void rollback()
        {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }

        private void createWriterIfNecessary()
        {
            if (writer == null) {
                shardUuid = UUID.randomUUID();
                File stagingFile = storageService.getStagingFile(shardUuid);
                storageService.createParents(stagingFile);
                writer = new OrcFileWriter(columnIds, columnTypes, stagingFile);
            }
        }
    }
}
