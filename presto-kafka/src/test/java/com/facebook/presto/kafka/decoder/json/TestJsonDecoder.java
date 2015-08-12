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
package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.kafka.decoder.util.DecoderTestUtil.checkIsNull;
import static com.facebook.presto.kafka.decoder.util.DecoderTestUtil.checkValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestJsonDecoder
{
    private static final JsonKafkaFieldDecoder DEFAULT_FIELD_DECODER = new JsonKafkaFieldDecoder();
    private static final ObjectMapperProvider PROVIDER = new ObjectMapperProvider();

    private static Map<KafkaColumnHandle, KafkaFieldDecoder<?>> buildMap(List<KafkaColumnHandle> columns)
    {
        ImmutableMap.Builder<KafkaColumnHandle, KafkaFieldDecoder<?>> map = ImmutableMap.builder();
        for (KafkaColumnHandle column : columns) {
            map.put(column, DEFAULT_FIELD_DECODER);
        }
        return map.build();
    }

    @Test
    public void testSimple()
            throws Exception
    {
        byte[] json = ByteStreams.toByteArray(TestJsonDecoder.class.getResourceAsStream("/decoder/json/message.json"));

        JsonKafkaRowDecoder rowDecoder = new JsonKafkaRowDecoder(PROVIDER.get());
        KafkaColumnHandle row1 = new KafkaColumnHandle("", 0, "row1", VarcharType.VARCHAR, "source", null, null, false, false, false);
        KafkaColumnHandle row2 = new KafkaColumnHandle("", 1, "row2", VarcharType.VARCHAR, "user/screen_name", null, null, false, false, false);
        KafkaColumnHandle row3 = new KafkaColumnHandle("", 2, "row3", BigintType.BIGINT, "id", null, null, false, false, false);
        KafkaColumnHandle row4 = new KafkaColumnHandle("", 3, "row4", BigintType.BIGINT, "user/statuses_count", null, null, false, false, false);
        KafkaColumnHandle row5 = new KafkaColumnHandle("", 4, "row5", BooleanType.BOOLEAN, "user/geo_enabled", null, null, false, false, false);

        List<KafkaColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5);
        Set<KafkaFieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(json, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        checkValue(providers, row1, "<a href=\"http://twitterfeed.com\" rel=\"nofollow\">twitterfeed</a>");
        checkValue(providers, row2, "EKentuckyNews");
        checkValue(providers, row3, 493857959588286460L);
        checkValue(providers, row4, 7630);
        checkValue(providers, row5, true);
    }

    @Test
    public void testNonExistent()
            throws Exception
    {
        byte[] json = "{}".getBytes(StandardCharsets.UTF_8);

        JsonKafkaRowDecoder rowDecoder = new JsonKafkaRowDecoder(PROVIDER.get());
        KafkaColumnHandle row1 = new KafkaColumnHandle("", 0, "row1", VarcharType.VARCHAR, "very/deep/varchar", null, null, false, false, false);
        KafkaColumnHandle row2 = new KafkaColumnHandle("", 1, "row2", BigintType.BIGINT, "no_bigint", null, null, false, false, false);
        KafkaColumnHandle row3 = new KafkaColumnHandle("", 2, "row3", DoubleType.DOUBLE, "double/is_missing", null, null, false, false, false);
        KafkaColumnHandle row4 = new KafkaColumnHandle("", 3, "row4", BooleanType.BOOLEAN, "hello", null, null, false, false, false);

        List<KafkaColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4);
        Set<KafkaFieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(json, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        checkIsNull(providers, row1);
        checkIsNull(providers, row2);
        checkIsNull(providers, row3);
        checkIsNull(providers, row4);
    }

    @Test
    public void testStringNumber()
            throws Exception
    {
        byte[] json = "{\"a_number\":481516,\"a_string\":\"2342\"}".getBytes(StandardCharsets.UTF_8);

        JsonKafkaRowDecoder rowDecoder = new JsonKafkaRowDecoder(PROVIDER.get());
        KafkaColumnHandle row1 = new KafkaColumnHandle("", 0, "row1", VarcharType.VARCHAR, "a_number", null, null, false, false, false);
        KafkaColumnHandle row2 = new KafkaColumnHandle("", 1, "row2", BigintType.BIGINT, "a_number", null, null, false, false, false);
        KafkaColumnHandle row3 = new KafkaColumnHandle("", 2, "row3", VarcharType.VARCHAR, "a_string", null, null, false, false, false);
        KafkaColumnHandle row4 = new KafkaColumnHandle("", 3, "row4", BigintType.BIGINT, "a_string", null, null, false, false, false);

        List<KafkaColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4);
        Set<KafkaFieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(json, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        checkValue(providers, row1, "481516");
        checkValue(providers, row2, 481516);
        checkValue(providers, row3, "2342");
        checkValue(providers, row4, 2342);
    }
}
