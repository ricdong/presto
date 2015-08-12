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
package com.facebook.presto.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.TestOperatorStats.assertExpectedOperatorStats;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestDriverStats
{
    public static final DriverStats EXPECTED = new DriverStats(
            new DateTime(1),
            new DateTime(2),
            new DateTime(3),

            new Duration(4, NANOSECONDS),
            new Duration(5, NANOSECONDS),

            new DataSize(6, BYTE),

            new Duration(7, NANOSECONDS),
            new Duration(8, NANOSECONDS),
            new Duration(9, NANOSECONDS),
            new Duration(10, NANOSECONDS),
            false,
            ImmutableSet.of(),

            new DataSize(11, BYTE),
            12,
            new Duration(13, NANOSECONDS),

            new DataSize(13, BYTE),
            14,

            new DataSize(15, BYTE),
            16,

            ImmutableList.of(TestOperatorStats.EXPECTED));

    @Test
    public void testJson()
    {
        JsonCodec<DriverStats> codec = JsonCodec.jsonCodec(DriverStats.class);

        String json = codec.toJson(EXPECTED);
        DriverStats actual = codec.fromJson(json);

        assertExpectedDriverStats(actual);
    }

    public static void assertExpectedDriverStats(DriverStats actual)
    {
        assertEquals(actual.getCreateTime(), new DateTime(1, UTC));
        assertEquals(actual.getStartTime(), new DateTime(2, UTC));
        assertEquals(actual.getEndTime(), new DateTime(3, UTC));
        assertEquals(actual.getQueuedTime(), new Duration(4, NANOSECONDS));
        assertEquals(actual.getElapsedTime(), new Duration(5, NANOSECONDS));

        assertEquals(actual.getMemoryReservation(), new DataSize(6, BYTE));

        assertEquals(actual.getTotalScheduledTime(), new Duration(7, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(8, NANOSECONDS));
        assertEquals(actual.getTotalUserTime(), new Duration(9, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(10, NANOSECONDS));

        assertEquals(actual.getRawInputDataSize(), new DataSize(11, BYTE));
        assertEquals(actual.getRawInputPositions(), 12);
        assertEquals(actual.getRawInputReadTime(), new Duration(13, NANOSECONDS));

        assertEquals(actual.getProcessedInputDataSize(), new DataSize(13, BYTE));
        assertEquals(actual.getProcessedInputPositions(), 14);

        assertEquals(actual.getOutputDataSize(), new DataSize(15, BYTE));
        assertEquals(actual.getOutputPositions(), 16);

        assertEquals(actual.getOperatorStats().size(), 1);
        assertExpectedOperatorStats(actual.getOperatorStats().get(0));
    }
}
