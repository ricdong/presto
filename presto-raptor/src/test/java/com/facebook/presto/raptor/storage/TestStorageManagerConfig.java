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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestStorageManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StorageManagerConfig.class)
                .setDataDirectory(null)
                .setOrcMaxMergeDistance(new DataSize(1, MEGABYTE))
                .setOrcMaxReadSize(new DataSize(8, MEGABYTE))
                .setOrcStreamBufferSize(new DataSize(8, MEGABYTE))
                .setShardRecoveryTimeout(new Duration(30, SECONDS))
                .setMissingShardDiscoveryInterval(new Duration(5, MINUTES))
                .setCompactionInterval(new Duration(1, HOURS))
                .setRecoveryThreads(10)
                .setCompactionThreads(5)
                .setMaxShardRows(1_000_000)
                .setMaxShardSize(new DataSize(256, MEGABYTE))
                .setMaxBufferSize(new DataSize(256, MEGABYTE)));

    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("storage.data-directory", "/data")
                .put("storage.orc.max-merge-distance", "16kB")
                .put("storage.orc.max-read-size", "16kB")
                .put("storage.orc.stream-buffer-size", "16kB")
                .put("storage.shard-recovery-timeout", "1m")
                .put("storage.missing-shard-discovery-interval", "4m")
                .put("storage.compaction-interval", "4h")
                .put("storage.max-recovery-threads", "12")
                .put("storage.max-compaction-threads", "12")
                .put("storage.max-shard-rows", "10000")
                .put("storage.max-shard-size", "10MB")
                .put("storage.max-buffer-size", "512MB")
                .build();

        StorageManagerConfig expected = new StorageManagerConfig()
                .setDataDirectory(new File("/data"))
                .setOrcMaxMergeDistance(new DataSize(16, KILOBYTE))
                .setOrcMaxReadSize(new DataSize(16, KILOBYTE))
                .setOrcStreamBufferSize(new DataSize(16, KILOBYTE))
                .setShardRecoveryTimeout(new Duration(1, MINUTES))
                .setMissingShardDiscoveryInterval(new Duration(4, MINUTES))
                .setCompactionInterval(new Duration(4, HOURS))
                .setRecoveryThreads(12)
                .setCompactionThreads(12)
                .setMaxShardRows(10_000)
                .setMaxShardSize(new DataSize(10, MEGABYTE))
                .setMaxBufferSize(new DataSize(512, MEGABYTE));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidations()
    {
        assertFailsValidation(new StorageManagerConfig().setDataDirectory(null), "dataDirectory", "may not be null", NotNull.class);
    }
}
