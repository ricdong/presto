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
package com.facebook.presto.spi;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.spi.TupleDomain.columnWiseUnion;
import static org.testng.Assert.assertEquals;

public class TestTupleDomain
{
    private static final ColumnHandle A = new TestingColumnHandle("a");
    private static final ColumnHandle B = new TestingColumnHandle("b");
    private static final ColumnHandle C = new TestingColumnHandle("c");
    private static final ColumnHandle D = new TestingColumnHandle("d");
    private static final ColumnHandle E = new TestingColumnHandle("e");
    private static final ColumnHandle F = new TestingColumnHandle("f");

    @Test
    public void testNone()
            throws Exception
    {
        Assert.assertTrue(TupleDomain.none().isNone());
        Assert.assertEquals(TupleDomain.<ColumnHandle>none(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        A, Domain.none(Long.class))));
        Assert.assertEquals(TupleDomain.<ColumnHandle>none(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        A, Domain.all(Long.class),
                        B, Domain.none(String.class))));
    }

    @Test
    public void testAll()
            throws Exception
    {
        Assert.assertTrue(TupleDomain.all().isAll());
        Assert.assertEquals(TupleDomain.<ColumnHandle>all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        A, Domain.all(Long.class))));
        Assert.assertEquals(TupleDomain.<ColumnHandle>all(),
                TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of()));
    }

    @Test
    public void testIntersection()
            throws Exception
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.all(String.class))
                        .put(B, Domain.notNull(Double.class))
                        .put(C, Domain.singleValue(1L))
                        .put(D, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(0.0)), true))
                        .build());

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue("value"))
                        .put(B, Domain.singleValue(0.0))
                        .put(C, Domain.singleValue(1L))
                        .put(D, Domain.create(SortedRangeSet.of(Range.lessThan(10.0)), false))
                        .build());

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue("value"))
                        .put(B, Domain.singleValue(0.0))
                        .put(C, Domain.singleValue(1L))
                        .put(D, Domain.create(SortedRangeSet.of(Range.range(0.0, true, 10.0, false)), false))
                        .build());

        Assert.assertEquals(tupleDomain1.intersect(tupleDomain2), expectedTupleDomain);
    }

    @Test
    public void testNoneIntersection()
            throws Exception
    {
        Assert.assertEquals(TupleDomain.none().intersect(TupleDomain.all()), TupleDomain.none());
        Assert.assertEquals(TupleDomain.all().intersect(TupleDomain.none()), TupleDomain.none());
        Assert.assertEquals(TupleDomain.none().intersect(TupleDomain.none()), TupleDomain.none());
        Assert.assertEquals(TupleDomain.withColumnDomains(
                ImmutableMap.of(A, Domain.onlyNull(Long.class)))
                .intersect(
                        TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.notNull(Long.class)))),
                TupleDomain.<ColumnHandle>none());
    }

    @Test
    public void testMismatchedColumnIntersection()
            throws Exception
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.all(Double.class),
                        B, Domain.singleValue("value")));

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(0.0)), true),
                        C, Domain.singleValue(1L)));

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                A, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(0.0)), true),
                B, Domain.singleValue("value"),
                C, Domain.singleValue(1L)));

        Assert.assertEquals(tupleDomain1.intersect(tupleDomain2), expectedTupleDomain);
    }

    @Test
    public void testColumnWiseUnion()
            throws Exception
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.all(String.class))
                        .put(B, Domain.notNull(Double.class))
                        .put(C, Domain.onlyNull(Long.class))
                        .put(D, Domain.singleValue(1L))
                        .put(E, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(0.0)), true))
                        .build());

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue("value"))
                        .put(B, Domain.singleValue(0.0))
                        .put(C, Domain.notNull(Long.class))
                        .put(D, Domain.singleValue(1L))
                        .put(E, Domain.create(SortedRangeSet.of(Range.lessThan(10.0)), false))
                        .build());

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.all(String.class))
                        .put(B, Domain.notNull(Double.class))
                        .put(C, Domain.all(Long.class))
                        .put(D, Domain.singleValue(1L))
                        .put(E, Domain.all(Double.class))
                        .build());

        Assert.assertEquals(columnWiseUnion(tupleDomain1, tupleDomain2), expectedTupleDomain);
    }

    @Test
    public void testNoneColumnWiseUnion()
            throws Exception
    {
        Assert.assertEquals(columnWiseUnion(TupleDomain.none(), TupleDomain.all()), TupleDomain.all());
        Assert.assertEquals(columnWiseUnion(TupleDomain.all(), TupleDomain.none()), TupleDomain.all());
        Assert.assertEquals(columnWiseUnion(TupleDomain.none(), TupleDomain.none()), TupleDomain.none());
        Assert.assertEquals(columnWiseUnion(
                TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.onlyNull(Long.class))),
                TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.notNull(Long.class)))),
                TupleDomain.<ColumnHandle>all());
    }

    @Test
    public void testMismatchedColumnWiseUnion()
            throws Exception
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.all(Double.class),
                        B, Domain.singleValue("value")));

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(0.0)), true),
                        C, Domain.singleValue(1L)));

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(Double.class)));

        Assert.assertEquals(columnWiseUnion(tupleDomain1, tupleDomain2), expectedTupleDomain);
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        Assert.assertTrue(overlaps(
                ImmutableMap.<ColumnHandle, Domain>of(),
                ImmutableMap.<ColumnHandle, Domain>of()));

        Assert.assertTrue(overlaps(
                ImmutableMap.<ColumnHandle, Domain>of(),
                ImmutableMap.of(A, Domain.singleValue(0L))));

        Assert.assertFalse(overlaps(
                ImmutableMap.<ColumnHandle, Domain>of(),
                ImmutableMap.of(A, Domain.none(Long.class))));

        Assert.assertFalse(overlaps(
                ImmutableMap.of(A, Domain.none(Long.class)),
                ImmutableMap.of(A, Domain.none(Long.class))));

        Assert.assertTrue(overlaps(
                ImmutableMap.of(A, Domain.all(Long.class)),
                ImmutableMap.of(A, Domain.all(Long.class))));

        Assert.assertTrue(overlaps(
                ImmutableMap.of(A, Domain.singleValue(1L)),
                ImmutableMap.of(B, Domain.singleValue("value"))));

        Assert.assertTrue(overlaps(
                ImmutableMap.of(A, Domain.singleValue(1L)),
                ImmutableMap.of(A, Domain.all(Long.class))));

        Assert.assertFalse(overlaps(
                ImmutableMap.of(A, Domain.singleValue(1L)),
                ImmutableMap.of(A, Domain.singleValue(2L))));

        Assert.assertFalse(overlaps(
                ImmutableMap.of(
                        A, Domain.singleValue(1L),
                        B, Domain.singleValue(1L)),
                ImmutableMap.of(
                        A, Domain.singleValue(1L),
                        B, Domain.singleValue(2L))));

        Assert.assertTrue(overlaps(
                ImmutableMap.of(
                        A, Domain.singleValue(1L),
                        B, Domain.all(Long.class)),
                ImmutableMap.of(
                        A, Domain.singleValue(1L),
                        B, Domain.singleValue(2L))));
    }

    @Test
    public void testContains()
            throws Exception
    {
        Assert.assertTrue(contains(
                ImmutableMap.<ColumnHandle, Domain>of(),
                ImmutableMap.<ColumnHandle, Domain>of()));

        Assert.assertTrue(contains(
                ImmutableMap.<ColumnHandle, Domain>of(),
                ImmutableMap.of(A, Domain.none(Long.class))));

        Assert.assertTrue(contains(
                ImmutableMap.<ColumnHandle, Domain>of(),
                ImmutableMap.of(A, Domain.all(Long.class))));

        Assert.assertTrue(contains(
                ImmutableMap.<ColumnHandle, Domain>of(),
                ImmutableMap.of(A, Domain.singleValue(0.0))));

        Assert.assertFalse(contains(
                ImmutableMap.of(A, Domain.none(Long.class)),
                ImmutableMap.<ColumnHandle, Domain>of()));

        Assert.assertTrue(contains(
                ImmutableMap.of(A, Domain.none(Long.class)),
                ImmutableMap.of(A, Domain.none(Long.class))));

        Assert.assertFalse(contains(
                ImmutableMap.of(A, Domain.none(Long.class)),
                ImmutableMap.of(A, Domain.all(Long.class))));

        Assert.assertFalse(contains(
                ImmutableMap.of(A, Domain.none(Long.class)),
                ImmutableMap.of(A, Domain.singleValue(0L))));

        Assert.assertTrue(contains(
                ImmutableMap.of(A, Domain.all(Long.class)),
                ImmutableMap.<ColumnHandle, Domain>of()));

        Assert.assertTrue(contains(
                ImmutableMap.of(A, Domain.all(Long.class)),
                ImmutableMap.of(A, Domain.none(Long.class))));

        Assert.assertTrue(contains(
                ImmutableMap.of(A, Domain.all(Long.class)),
                ImmutableMap.of(A, Domain.all(Long.class))));

        Assert.assertTrue(contains(
                ImmutableMap.of(A, Domain.all(Long.class)),
                ImmutableMap.of(A, Domain.singleValue(0L))));

        Assert.assertFalse(contains(
                ImmutableMap.of(A, Domain.singleValue(0L)),
                ImmutableMap.<ColumnHandle, Domain>of()));

        Assert.assertTrue(contains(
                ImmutableMap.of(A, Domain.singleValue(0L)),
                ImmutableMap.of(A, Domain.none(Long.class))));

        Assert.assertFalse(contains(
                ImmutableMap.of(A, Domain.singleValue(0L)),
                ImmutableMap.of(A, Domain.all(Long.class))));

        Assert.assertTrue(contains(
                ImmutableMap.of(A, Domain.singleValue(0L)),
                ImmutableMap.of(A, Domain.singleValue(0L))));

        Assert.assertFalse(contains(
                ImmutableMap.of(A, Domain.singleValue(0L)),
                ImmutableMap.of(B, Domain.singleValue("value"))));

        Assert.assertFalse(contains(
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.singleValue("value")),
                ImmutableMap.of(B, Domain.singleValue("value"))));

        Assert.assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.singleValue("value")),
                ImmutableMap.of(B, Domain.none(String.class))));

        Assert.assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.singleValue("value")),
                ImmutableMap.of(
                        A, Domain.singleValue(1L),
                        B, Domain.none(String.class))));

        Assert.assertTrue(contains(
                ImmutableMap.of(
                        B, Domain.singleValue("value")),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.singleValue("value"))));

        Assert.assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.all(Long.class),
                        B, Domain.singleValue("value")),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.singleValue("value"))));

        Assert.assertFalse(contains(
                ImmutableMap.of(
                        A, Domain.all(Long.class),
                        B, Domain.singleValue("value")),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.singleValue("value2"))));

        Assert.assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.all(Long.class),
                        B, Domain.singleValue("value")),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.singleValue("value2"),
                        C, Domain.none(String.class))));

        Assert.assertFalse(contains(
                ImmutableMap.of(
                        A, Domain.all(Long.class),
                        B, Domain.singleValue("value"),
                        C, Domain.none(String.class)),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.singleValue("value2"))));

        Assert.assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.all(Long.class),
                        B, Domain.singleValue("value"),
                        C, Domain.none(String.class)),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.none(String.class))));
    }

    @Test
    public void testEquals()
            throws Exception
    {
        Assert.assertTrue(equals(
                ImmutableMap.<ColumnHandle, Domain>of(),
                ImmutableMap.<ColumnHandle, Domain>of()));

        Assert.assertTrue(equals(
                ImmutableMap.<ColumnHandle, Domain>of(),
                ImmutableMap.of(A, Domain.all(Long.class))));

        Assert.assertFalse(equals(
                ImmutableMap.<ColumnHandle, Domain>of(),
                ImmutableMap.of(A, Domain.none(Long.class))));

        Assert.assertFalse(equals(
                ImmutableMap.<ColumnHandle, Domain>of(),
                ImmutableMap.of(A, Domain.singleValue(0L))));

        Assert.assertTrue(equals(
                ImmutableMap.of(A, Domain.all(Long.class)),
                ImmutableMap.of(A, Domain.all(Long.class))));

        Assert.assertFalse(equals(
                ImmutableMap.of(A, Domain.all(Long.class)),
                ImmutableMap.of(A, Domain.none(Long.class))));

        Assert.assertFalse(equals(
                ImmutableMap.of(A, Domain.all(Long.class)),
                ImmutableMap.of(A, Domain.singleValue(0L))));

        Assert.assertTrue(equals(
                ImmutableMap.of(A, Domain.none(Long.class)),
                ImmutableMap.of(A, Domain.none(Long.class))));

        Assert.assertFalse(equals(
                ImmutableMap.of(A, Domain.none(Long.class)),
                ImmutableMap.of(A, Domain.singleValue(0L))));

        Assert.assertTrue(equals(
                ImmutableMap.of(A, Domain.singleValue(0L)),
                ImmutableMap.of(A, Domain.singleValue(0L))));

        Assert.assertFalse(equals(
                ImmutableMap.of(A, Domain.singleValue(0L)),
                ImmutableMap.of(B, Domain.singleValue(0L))));

        Assert.assertFalse(equals(
                ImmutableMap.of(A, Domain.singleValue(0L)),
                ImmutableMap.of(A, Domain.singleValue(1L))));

        Assert.assertTrue(equals(
                ImmutableMap.of(A, Domain.all(Long.class)),
                ImmutableMap.of(B, Domain.all(String.class))));

        Assert.assertTrue(equals(
                ImmutableMap.of(A, Domain.none(Long.class)),
                ImmutableMap.of(B, Domain.none(String.class))));

        Assert.assertTrue(equals(
                ImmutableMap.of(A, Domain.none(Long.class)),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.none(String.class))));

        Assert.assertFalse(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(1L)),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.none(String.class))));

        Assert.assertTrue(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(1L),
                        C, Domain.none(Double.class)),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.none(String.class))));

        Assert.assertTrue(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.all(Double.class)),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.all(Double.class))));

        Assert.assertTrue(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.all(String.class)),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        C, Domain.all(Double.class))));

        Assert.assertFalse(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.all(String.class)),
                ImmutableMap.of(
                        A, Domain.singleValue(1L),
                        C, Domain.all(Double.class))));

        Assert.assertFalse(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        B, Domain.all(String.class)),
                ImmutableMap.of(
                        A, Domain.singleValue(0L),
                        C, Domain.singleValue(0.0))));
    }

    @Test
    public void testIsNone()
            throws Exception
    {
        Assert.assertFalse(TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of()).isNone());
        Assert.assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.singleValue(0L))).isNone());
        Assert.assertTrue(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.none(Long.class))).isNone());
        Assert.assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(Long.class))).isNone());
        Assert.assertTrue(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(Long.class), B, Domain.none(Long.class))).isNone());
    }

    @Test
    public void testIsAll()
            throws Exception
    {
        Assert.assertTrue(TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of()).isAll());
        Assert.assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.singleValue(0L))).isAll());
        Assert.assertTrue(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(Long.class))).isAll());
        Assert.assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.singleValue(0L), B, Domain.all(Long.class))).isAll());
    }

    @Test
    public void testExtractFixedValues()
            throws Exception
    {
        Assert.assertEquals(
                TupleDomain.withColumnDomains(
                        ImmutableMap.<ColumnHandle, Domain>builder()
                                .put(A, Domain.all(Double.class))
                                .put(B, Domain.singleValue("value"))
                                .put(C, Domain.onlyNull(Long.class))
                                .put(D, Domain.create(SortedRangeSet.of(Range.equal(1L)), true))
                                .build())
                        .extractFixedValues(),
                ImmutableMap.<ColumnHandle, Comparable<?>>of(B, "value"));
    }

    @Test
    public void testSingleValuesMapToDomain()
            throws Exception
    {
        Assert.assertEquals(
                TupleDomain.withFixedValues(
                        ImmutableMap.<ColumnHandle, Comparable<?>>builder()
                                .put(A, 1L)
                                .put(B, "value")
                                .put(C, 0.01)
                                .put(D, true)
                                .build()),
                TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue(1L))
                        .put(B, Domain.singleValue("value"))
                        .put(C, Domain.singleValue(0.01))
                        .put(D, Domain.singleValue(true))
                        .build()));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        final ObjectMapper mapper = new ObjectMapper();

        // Normally, Presto server takes care of registering plugin types with Jackson...
        // But since we know that ColumnHandle will always be a TestingColumnHandle in this test,
        // let's just always deserialize ColumnHandle as a TestingColumnHandle.
        mapper.registerModule(new SimpleModule().addDeserializer(ColumnHandle.class, new JsonDeserializer<ColumnHandle>()
        {
            @Override
            public ColumnHandle deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                    throws IOException
            {
                return mapper.readValue(jsonParser, TestingColumnHandle.class);
            }
        }));

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.all();
        Assert.assertEquals(tupleDomain, mapper.readValue(mapper.writeValueAsString(tupleDomain), new TypeReference<TupleDomain<ColumnHandle>>() {}));

        tupleDomain = TupleDomain.none();
        Assert.assertEquals(tupleDomain, mapper.readValue(mapper.writeValueAsString(tupleDomain), new TypeReference<TupleDomain<ColumnHandle>>() {}));

        tupleDomain = TupleDomain.withFixedValues(ImmutableMap.<ColumnHandle, Comparable<?>>of(A, 1L, B, "abc"));
        Assert.assertEquals(tupleDomain, mapper.readValue(mapper.writeValueAsString(tupleDomain), new TypeReference<TupleDomain<ColumnHandle>>() {}));
    }

    @Test
    public void testTransform()
            throws Exception
    {
        Map<Integer, Domain> domains = ImmutableMap.<Integer, Domain>builder()
                .put(1, Domain.singleValue(1))
                .put(2, Domain.singleValue(2))
                .put(3, Domain.singleValue(3))
                .build();

        TupleDomain<Integer> domain = TupleDomain.withColumnDomains(domains);
        TupleDomain<String> transformed = domain.transform(Object::toString);

        Map<String, Domain> expected = ImmutableMap.<String, Domain>builder()
                .put("1", Domain.singleValue(1))
                .put("2", Domain.singleValue(2))
                .put("3", Domain.singleValue(3))
                .build();

        assertEquals(transformed.getDomains(), expected);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTransformFailsWithNonUniqueMapping()
            throws Exception
    {
        Map<Integer, Domain> domains = ImmutableMap.<Integer, Domain>builder()
                .put(1, Domain.singleValue(1))
                .put(2, Domain.singleValue(2))
                .put(3, Domain.singleValue(3))
                .build();

        TupleDomain<Integer> domain = TupleDomain.withColumnDomains(domains);

        domain.transform(input -> "x");
    }

    private boolean overlaps(Map<ColumnHandle, Domain> domains1, Map<ColumnHandle, Domain> domains2)
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(domains1);
        TupleDomain<ColumnHandle> tupleDOmain2 = TupleDomain.withColumnDomains(domains2);
        return tupleDomain1.overlaps(tupleDOmain2);
    }

    private boolean contains(Map<ColumnHandle, Domain> superSet, Map<ColumnHandle, Domain> subSet)
    {
        TupleDomain<ColumnHandle> superSetTupleDomain = TupleDomain.withColumnDomains(superSet);
        TupleDomain<ColumnHandle> subSetTupleDomain = TupleDomain.withColumnDomains(subSet);
        return superSetTupleDomain.contains(subSetTupleDomain);
    }

    private boolean equals(Map<ColumnHandle, Domain> domains1, Map<ColumnHandle, Domain> domains2)
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(domains1);
        TupleDomain<ColumnHandle> tupleDOmain2 = TupleDomain.withColumnDomains(domains2);
        return tupleDomain1.equals(tupleDOmain2);
    }
}
