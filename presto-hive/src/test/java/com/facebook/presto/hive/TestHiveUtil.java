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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveUtil.parseHiveTimestamp;
import static org.testng.Assert.assertEquals;

public class TestHiveUtil
{
    @Test
    public void testParseHiveTimestamp()
    {
        DateTime time = new DateTime(2011, 5, 6, 7, 8, 9, 123, nonDefaultTimeZone());
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss"), unixTime(time, 0));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.S"), unixTime(time, 1));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSS"), unixTime(time, 3));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSS"), unixTime(time, 6));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"), unixTime(time, 7));
    }

    private static long parse(DateTime time, String pattern)
    {
        return parseHiveTimestamp(DateTimeFormat.forPattern(pattern).print(time), nonDefaultTimeZone());
    }

    private static long unixTime(DateTime time, int factionalDigits)
    {
        int factor = (int) Math.pow(10, Math.max(0, 3 - factionalDigits));
        return (time.getMillis() / factor) * factor;
    }

    static DateTimeZone nonDefaultTimeZone()
    {
        String defaultId = DateTimeZone.getDefault().getID();
        for (String id : DateTimeZone.getAvailableIDs()) {
            if (!id.equals(defaultId)) {
                DateTimeZone zone = DateTimeZone.forID(id);
                if (zone.getStandardOffset(0) != 0) {
                    return zone;
                }
            }
        }
        throw new IllegalStateException("no non-default timezone");
    }
}
