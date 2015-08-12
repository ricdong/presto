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

import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.util.PrioritizedFifoExecutor;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_RECOVERY_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.nanosSince;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

public class ShardRecoveryManager
{
    private static final Logger log = Logger.get(ShardRecoveryManager.class);

    private final StorageService storageService;
    private final Optional<BackupStore> backupStore;
    private final String nodeIdentifier;
    private final ShardManager shardManager;
    private final Duration missingShardDiscoveryInterval;

    private final AtomicBoolean started = new AtomicBoolean();
    private final MissingShardsQueue shardQueue;

    private final ScheduledExecutorService missingShardExecutor = newScheduledThreadPool(1, daemonThreadsNamed("missing-shard-discovery"));
    private final ExecutorService executorService = newCachedThreadPool(daemonThreadsNamed("shard-recovery-%s"));
    private final ShardRecoveryStats stats;

    @Inject
    public ShardRecoveryManager(
            StorageService storageService,
            Optional<BackupStore> backupStore,
            NodeManager nodeManager,
            ShardManager shardManager,
            StorageManagerConfig config)
    {
        this(storageService,
                backupStore,
                nodeManager,
                shardManager,
                config.getMissingShardDiscoveryInterval(),
                config.getRecoveryThreads());
    }

    public ShardRecoveryManager(
            StorageService storageService,
            Optional<BackupStore> backupStore,
            NodeManager nodeManager,
            ShardManager shardManager,
            Duration missingShardDiscoveryInterval,
            int recoveryThreads)
    {
        this.storageService = checkNotNull(storageService, "storageService is null");
        this.backupStore = checkNotNull(backupStore, "backupStore is null");
        this.nodeIdentifier = checkNotNull(nodeManager, "nodeManager is null").getCurrentNode().getNodeIdentifier();
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.missingShardDiscoveryInterval = checkNotNull(missingShardDiscoveryInterval, "missingShardDiscoveryInterval is null");
        this.shardQueue = new MissingShardsQueue(new PrioritizedFifoExecutor<>(executorService, recoveryThreads, new MissingShardComparator()));
        this.stats = new ShardRecoveryStats();
    }

    @PostConstruct
    public void start()
    {
        if (!backupStore.isPresent()) {
            return;
        }
        if (started.compareAndSet(false, true)) {
            enqueueMissingShards();
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdownNow();
        missingShardExecutor.shutdownNow();
    }

    private void enqueueMissingShards()
    {
        missingShardExecutor.scheduleWithFixedDelay(() -> {
            try {
                SECONDS.sleep(ThreadLocalRandom.current().nextInt(1, 30));
                for (UUID shard : getMissingShards()) {
                    stats.incrementBackgroundShardRecovery();
                    shardQueue.submit(MissingShard.createBackgroundMissingShard(shard));
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Throwable t) {
                log.error(t, "Error creating shard recovery tasks");
            }
        }, 0, missingShardDiscoveryInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private Set<UUID> getMissingShards()
    {
        return shardManager.getNodeShards(nodeIdentifier).stream()
                .filter(this::shardNeedsRecovery)
                .collect(toSet());
    }

    private boolean shardNeedsRecovery(UUID shardUuid)
    {
        File storageFile = storageService.getStorageFile(shardUuid);
        if (!storageFile.exists()) {
            return true;
        }
        OptionalLong backupLength = backupStore.get().shardSize(shardUuid);
        return backupLength.isPresent() && (storageFile.length() != backupLength.getAsLong());
    }

    public Future<?> recoverShard(UUID shardUuid)
            throws ExecutionException
    {
        checkNotNull(shardUuid, "shardUuid is null");
        stats.incrementActiveShardRecovery();
        return shardQueue.submit(MissingShard.createActiveMissingShard(shardUuid));
    }

    @VisibleForTesting
    void restoreFromBackup(UUID shardUuid)
    {
        File storageFile = storageService.getStorageFile(shardUuid);

        OptionalLong backupSize = backupStore.get().shardSize(shardUuid);
        if (!backupSize.isPresent()) {
            stats.incrementShardRecoveryBackupNotFound();
            throw new PrestoException(RAPTOR_RECOVERY_ERROR, "No backup file found for shard: " + shardUuid);
        }

        if (storageFile.exists()) {
            if (storageFile.length() == backupSize.getAsLong()) {
                return;
            }
            log.warn("Local shard file is corrupt. Deleting local file: %s", shardUuid);
            storageFile.delete();
        }

        // create a temporary file in the staging directory
        File stagingFile = temporarySuffix(storageService.getStagingFile(shardUuid));
        storageService.createParents(stagingFile);

        // copy to temporary file
        log.info("Copying shard %s from backup...", shardUuid);
        long start = System.nanoTime();

        try {
            backupStore.get().restoreShard(shardUuid, stagingFile);
        }
        catch (PrestoException e) {
            stats.incrementShardRecoveryFailure();
            throw e;
        }

        Duration duration = nanosSince(start);
        DataSize size = new DataSize(stagingFile.length(), BYTE);
        DataSize rate = dataRate(size, duration).convertToMostSuccinctDataSize();
        stats.addShardRecoveryDataRate(rate, size, duration);

        log.info("Copied shard %s from backup in %s (%s at %s/s)", shardUuid, duration, size, rate);

        // move to final location
        storageService.createParents(storageFile);
        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (FileAlreadyExistsException e) {
            // someone else already created it (should not happen, but safe to ignore)
        }
        catch (IOException e) {
            stats.incrementShardRecoveryFailure();
            throw new PrestoException(RAPTOR_RECOVERY_ERROR, "Failed to move shard: " + shardUuid, e);
        }

        if (!storageFile.exists() || (storageFile.length() != backupSize.getAsLong())) {
            stats.incrementShardRecoveryFailure();
            log.info("Files do not match after recovery. Deleting local file: " + shardUuid);
            storageFile.delete();
            throw new PrestoException(RAPTOR_RECOVERY_ERROR, "File not recovered correctly: " + shardUuid);
        }

        stats.incrementShardRecoverySuccess();
    }

    @VisibleForTesting
    static class MissingShardComparator
            implements Comparator<MissingShardRunnable>
    {
        @Override
        public int compare(MissingShardRunnable shard1, MissingShardRunnable shard2)
        {
            if (shard1.isActive() == shard2.isActive()) {
                return 0;
            }
            return shard1.isActive() ? -1 : 1;
        }
    }

    interface MissingShardRunnable
            extends Runnable
    {
        boolean isActive();
    }

    private class MissingShardRecovery
            implements MissingShardRunnable
    {
        private final UUID shardUuid;
        private final boolean active;

        public MissingShardRecovery(UUID shardUuid, boolean active)
        {
            this.shardUuid = checkNotNull(shardUuid, "shardUuid is null");
            this.active = active;
        }

        @Override
        public void run()
        {
            restoreFromBackup(shardUuid);
        }

        @Override
        public boolean isActive()
        {
            return active;
        }
    }

    private static final class MissingShard
    {
        private final UUID shardUuid;
        private final boolean active;

        private MissingShard(UUID shardUuid, boolean active)
        {
            this.shardUuid = checkNotNull(shardUuid, "shardUuid is null");
            this.active = active;
        }

        public static MissingShard createBackgroundMissingShard(UUID shardUuid)
        {
            return new MissingShard(shardUuid, false);
        }

        public static MissingShard createActiveMissingShard(UUID shardUuid)
        {
            return new MissingShard(shardUuid, true);
        }

        public UUID getShardUuid()
        {
            return shardUuid;
        }

        public boolean isActive()
        {
            return active;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MissingShard other = (MissingShard) o;
            return Objects.equals(this.active, other.active) &&
                    Objects.equals(this.shardUuid, other.shardUuid);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(shardUuid, active);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("shardUuid", shardUuid)
                    .add("active", active)
                    .toString();
        }
    }

    private class MissingShardsQueue
    {
        private final LoadingCache<MissingShard, Future<?>> queuedMissingShards;

        public MissingShardsQueue(PrioritizedFifoExecutor<MissingShardRunnable> shardRecoveryExecutor)
        {
            checkNotNull(shardRecoveryExecutor, "shardRecoveryExecutor is null");
            this.queuedMissingShards = CacheBuilder.newBuilder().build(new CacheLoader<MissingShard, Future<?>>()
            {
                @Override
                public Future<?> load(MissingShard missingShard)
                {
                    MissingShardRecovery task = new MissingShardRecovery(missingShard.getShardUuid(), missingShard.isActive());
                    ListenableFuture<?> future = shardRecoveryExecutor.submit(task);
                    future.addListener(() -> queuedMissingShards.invalidate(missingShard), directExecutor());
                    return future;
                }
            });
        }

        public Future<?> submit(MissingShard shard)
                throws ExecutionException
        {
            return queuedMissingShards.get(shard);
        }
    }

    static DataSize dataRate(DataSize size, Duration duration)
    {
        double rate = size.toBytes() / duration.getValue(SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            rate = 0;
        }
        return new DataSize(rate, BYTE).convertToMostSuccinctDataSize();
    }

    private static File temporarySuffix(File file)
    {
        return new File(file.getPath() + ".tmp-" + UUID.randomUUID());
    }

    @Managed
    @Flatten
    public ShardRecoveryStats getStats()
    {
        return stats;
    }
}
