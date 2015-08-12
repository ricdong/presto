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

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class TestHiveSplit
{
    private final JsonCodec<HiveSplit> codec = JsonCodec.jsonCodec(HiveSplit.class);

    @Test
    public void testJsonRoundTrip()
    {
        Properties schema = new Properties();
        schema.setProperty("foo", "bar");
        schema.setProperty("bar", "baz");

        ImmutableList<HivePartitionKey> partitionKeys = ImmutableList.of(new HivePartitionKey("a", HiveType.HIVE_STRING, "apple"), new HivePartitionKey("b", HiveType.HIVE_LONG, "42"));
        ImmutableList<HostAddress> addresses = ImmutableList.of(HostAddress.fromParts("127.0.0.1", 44), HostAddress.fromParts("127.0.0.1", 45));
        HiveSplit expected = new HiveSplit(
                "clientId",
                "db",
                "table",
                "partitionId",
                "path",
                42,
                88,
                schema,
                partitionKeys,
                addresses,
                true,
                TupleDomain.<HiveColumnHandle>all());

        String json = codec.toJson(expected);
        HiveSplit actual = codec.fromJson(json);

        assertEquals(actual.getClientId(), expected.getClientId());
        assertEquals(actual.getDatabase(), expected.getDatabase());
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getPartitionName(), expected.getPartitionName());
        assertEquals(actual.getPath(), expected.getPath());
        assertEquals(actual.getStart(), expected.getStart());
        assertEquals(actual.getLength(), expected.getLength());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getPartitionKeys(), expected.getPartitionKeys());
        assertEquals(actual.getAddresses(), expected.getAddresses());
        assertEquals(actual.isForceLocalScheduling(), expected.isForceLocalScheduling());
    }
}
