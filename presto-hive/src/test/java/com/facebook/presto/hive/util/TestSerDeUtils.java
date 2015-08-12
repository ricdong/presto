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
package com.facebook.presto.hive.util;

import com.facebook.presto.block.BlockSerdeUtil;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.io.BytesWritable;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static com.facebook.presto.hive.HiveTestUtils.arrayBlockOf;
import static com.facebook.presto.hive.HiveTestUtils.mapBlockOf;
import static com.facebook.presto.hive.HiveTestUtils.rowBlockOf;
import static com.facebook.presto.hive.util.SerDeUtils.getBlockObject;
import static com.facebook.presto.hive.util.SerDeUtils.serializeObject;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getReflectionObjectInspector;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("PackageVisibleField")
public class TestSerDeUtils
{
    private static class ListHolder
    {
        List<InnerStruct> array;
    }

    private static class InnerStruct
    {
        public InnerStruct(Integer intVal, Long longVal)
        {
            this.intVal = intVal;
            this.longVal = longVal;
        }

        Integer intVal;
        Long longVal;
    }

    private static class OuterStruct
    {
        Byte byteVal;
        Short shortVal;
        Integer intVal;
        Long longVal;
        Float floatVal;
        Double doubleVal;
        String stringVal;
        byte[] byteArray;
        List<InnerStruct> structArray;
        Map<String, InnerStruct> map;
        InnerStruct innerStruct;
    }

    private static ObjectInspector getInspector(Type type)
    {
        return getReflectionObjectInspector(type, ObjectInspectorOptions.JAVA);
    }

    @Test
    public void testPrimitiveSlice()
    {
        // boolean
        Block expectedBoolean = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1).writeByte(1).closeEntry().build();
        Block actualBoolean = toBinaryBlock(BOOLEAN, true, getInspector(Boolean.class));
        assertBlockEquals(actualBoolean, expectedBoolean);

        // byte
        Block expectedByte = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1).writeLong(5).closeEntry().build();
        Block actualByte = toBinaryBlock(BIGINT, (byte) 5, getInspector(Byte.class));
        assertBlockEquals(actualByte, expectedByte);

        // short
        Block expectedShort = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1).writeLong(2).closeEntry().build();
        Block actualShort = toBinaryBlock(BIGINT, (short) 2, getInspector(Short.class));
        assertBlockEquals(actualShort, expectedShort);

        // int
        Block expectedInt = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1).writeLong(1).closeEntry().build();
        Block actualInt = toBinaryBlock(BIGINT, 1, getInspector(Integer.class));
        assertBlockEquals(actualInt, expectedInt);

        // long
        Block expectedLong = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1).writeLong(10).closeEntry().build();
        Block actualLong = toBinaryBlock(BIGINT, 10L, getInspector(Long.class));
        assertBlockEquals(actualLong, expectedLong);

        // float
        Block expectedFloat = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1).writeDouble(20.0).closeEntry().build();
        Block actualFloat = toBinaryBlock(DOUBLE, 20.0f, getInspector(Float.class));
        assertBlockEquals(actualFloat, expectedFloat);

        // double
        Block expectedDouble = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1).writeDouble(30.12).closeEntry().build();
        Block actualDouble = toBinaryBlock(DOUBLE, 30.12d, getInspector(Double.class));
        assertBlockEquals(actualDouble, expectedDouble);

        // string
        Block expectedString = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1).writeBytes(utf8Slice("abdd"), 0, 4).closeEntry().build();
        Block actualString = toBinaryBlock(VARCHAR, "abdd", getInspector(String.class));
        assertBlockEquals(actualString, expectedString);

        // timestamp
        DateTime dateTime = new DateTime(2008, 10, 28, 16, 7, 15, 0);
        Block expectedTimestamp = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1).writeLong(dateTime.getMillis()).closeEntry().build();
        Block actualTimestamp = toBinaryBlock(BIGINT, new Timestamp(dateTime.getMillis()), getInspector(Timestamp.class));
        assertBlockEquals(actualTimestamp, expectedTimestamp);

        // binary
        byte[] byteArray = {81, 82, 84, 85};
        Block expectedBinary = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1).writeBytes(Slices.wrappedBuffer(byteArray), 0, 4).closeEntry().build();
        Block actualBinary = toBinaryBlock(VARCHAR, byteArray, getInspector(byte[].class));
        assertBlockEquals(actualBinary, expectedBinary);
    }

    @Test
    public void testListBlock()
    {
        List<InnerStruct> array = new ArrayList<>(2);
        array.add(new InnerStruct(8, 9L));
        array.add(new InnerStruct(10, 11L));
        ListHolder listHolder = new ListHolder();
        listHolder.array = array;

        com.facebook.presto.spi.type.Type rowType = new RowType(ImmutableList.of(BIGINT, BIGINT), Optional.empty());
        com.facebook.presto.spi.type.Type arrayOfRowType = new RowType(ImmutableList.of(new ArrayType(rowType)), Optional.empty());
        Block actual = toBinaryBlock(arrayOfRowType, listHolder, getInspector(ListHolder.class));
        BlockBuilder blockBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), 1024);
        rowType.writeObject(blockBuilder, rowBlockOf(ImmutableList.of(BIGINT, BIGINT), 8, 9L));
        rowType.writeObject(blockBuilder, rowBlockOf(ImmutableList.of(BIGINT, BIGINT), 10, 11L));
        Block expected = rowBlockOf(ImmutableList.of(new ArrayType(rowType)), blockBuilder.build());

        assertBlockEquals(actual, expected);
    }

    private static class MapHolder
    {
        Map<String, InnerStruct> map;
    }

    @Test
    public void testMapBlock()
    {
        MapHolder holder = new MapHolder();
        holder.map = new TreeMap<>();
        holder.map.put("twelve", new InnerStruct(13, 14L));
        holder.map.put("fifteen", new InnerStruct(16, 17L));

        com.facebook.presto.spi.type.Type rowType = new RowType(ImmutableList.of(BIGINT, BIGINT), Optional.empty());
        com.facebook.presto.spi.type.Type mapOfVarcharRowType = new RowType(ImmutableList.of(new MapType(VARCHAR, rowType)), Optional.empty());
        Block actual = toBinaryBlock(mapOfVarcharRowType, holder, getInspector(MapHolder.class));

        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(VARCHAR, rowType), new BlockBuilderStatus(), 1024);
        VARCHAR.writeString(blockBuilder, "fifteen");
        rowType.writeObject(blockBuilder, rowBlockOf(ImmutableList.of(BIGINT, BIGINT), 16, 17L));
        VARCHAR.writeString(blockBuilder, "twelve");
        rowType.writeObject(blockBuilder, rowBlockOf(ImmutableList.of(BIGINT, BIGINT), 13, 14L));
        Block expected = rowBlockOf(ImmutableList.of(new MapType(VARCHAR, rowType)), blockBuilder);

        assertBlockEquals(actual, expected);
    }

    @Test
    public void testStructBlock()
    {
        // test simple structs
        InnerStruct innerStruct = new InnerStruct(13, 14L);

        com.facebook.presto.spi.type.Type rowType = new RowType(ImmutableList.of(BIGINT, BIGINT), Optional.empty());
        Block actual = toBinaryBlock(rowType, innerStruct, getInspector(InnerStruct.class));

        Block expected = rowBlockOf(ImmutableList.of(BIGINT, BIGINT), 13, 14L);
        assertBlockEquals(actual, expected);

        // test complex structs
        OuterStruct outerStruct = new OuterStruct();
        outerStruct.byteVal = 1;
        outerStruct.shortVal = 2;
        outerStruct.intVal = 3;
        outerStruct.longVal = 4L;
        outerStruct.floatVal = 5.01f;
        outerStruct.doubleVal = 6.001d;
        outerStruct.stringVal = "seven";
        outerStruct.byteArray = new byte[] {'2'};
        InnerStruct is1 = new InnerStruct(2, -5L);
        InnerStruct is2 = new InnerStruct(-10, 0L);
        outerStruct.structArray = new ArrayList<>(2);
        outerStruct.structArray.add(is1);
        outerStruct.structArray.add(is2);
        outerStruct.map = new TreeMap<>();
        outerStruct.map.put("twelve", new InnerStruct(0, 5L));
        outerStruct.map.put("fifteen", new InnerStruct(-5, -10L));
        outerStruct.innerStruct = new InnerStruct(18, 19L);

        com.facebook.presto.spi.type.Type innerRowType = new RowType(ImmutableList.of(BIGINT, BIGINT), Optional.empty());
        com.facebook.presto.spi.type.Type arrayOfInnerRowType = new ArrayType(innerRowType);
        com.facebook.presto.spi.type.Type mapOfInnerRowType = new MapType(VARCHAR, innerRowType);
        List<com.facebook.presto.spi.type.Type> outerRowParameterTypes = ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE, DOUBLE, VARCHAR, VARCHAR, arrayOfInnerRowType, mapOfInnerRowType, innerRowType);
        com.facebook.presto.spi.type.Type outerRowType = new RowType(outerRowParameterTypes, Optional.empty());

        actual = toBinaryBlock(outerRowType, outerStruct, getInspector(OuterStruct.class));

        ImmutableList.Builder<Object> outerRowValues = ImmutableList.builder();
        outerRowValues.add(1);
        outerRowValues.add(2);
        outerRowValues.add(3);
        outerRowValues.add(4L);
        outerRowValues.add(5.01f);
        outerRowValues.add(6.001d);
        outerRowValues.add("seven");
        outerRowValues.add(new byte[] {'2'});
        outerRowValues.add(arrayBlockOf(innerRowType, rowBlockOf(ImmutableList.of(BIGINT, BIGINT), 2, -5L), rowBlockOf(ImmutableList.of(BIGINT, BIGINT), -10, 0)));
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(VARCHAR, innerRowType), new BlockBuilderStatus(), 1024);
        VARCHAR.writeString(blockBuilder, "fifteen");
        innerRowType.writeObject(blockBuilder, rowBlockOf(ImmutableList.of(BIGINT, BIGINT), -5, -10L));
        VARCHAR.writeString(blockBuilder, "twelve");
        innerRowType.writeObject(blockBuilder, rowBlockOf(ImmutableList.of(BIGINT, BIGINT), 0, 5L));
        outerRowValues.add(blockBuilder.build());
        outerRowValues.add(rowBlockOf(ImmutableList.of(BIGINT, BIGINT), 18, 19L));

        assertBlockEquals(actual, rowBlockOf(outerRowParameterTypes, outerRowValues.build().toArray()));
    }

    @Test
    public void testReuse()
            throws Exception
    {
        BytesWritable value = new BytesWritable();

        byte[] first = "hello world".getBytes(UTF_8);
        value.set(first, 0, first.length);

        byte[] second = "bye".getBytes(UTF_8);
        value.set(second, 0, second.length);

        Type type = new TypeToken<Map<BytesWritable, Integer>>() {}.getType();
        ObjectInspector inspector = getReflectionObjectInspector(type, ObjectInspectorOptions.JAVA);

        Block actual = getBlockObject(new MapType(VARCHAR, BIGINT), ImmutableMap.of(value, 0), inspector);
        Block expected = mapBlockOf(VARCHAR, BIGINT, "bye", 0);

        assertBlockEquals(actual, expected);
    }

    private static void assertBlockEquals(Block actual, Block expected)
    {
        assertEquals(blockToSlice(actual), blockToSlice(expected));
    }

    private static Slice blockToSlice(Block block)
    {
        // This function is strictly for testing use only
        SliceOutput sliceOutput = new DynamicSliceOutput(1000);
        BlockSerdeUtil.writeBlock(sliceOutput, block.copyRegion(0, block.getPositionCount()));
        return sliceOutput.slice();
    }

    private static Block toBinaryBlock(com.facebook.presto.spi.type.Type type, Object object, ObjectInspector inspector)
    {
        if (inspector.getCategory() == Category.PRIMITIVE) {
            return getPrimitiveBlock(type, object, inspector);
        }
        return getBlockObject(type, object, inspector);
    }

    private static Block getPrimitiveBlock(com.facebook.presto.spi.type.Type type, Object object, ObjectInspector inspector)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1);
        serializeObject(type, builder, object, inspector);
        return builder.build();
    }
}
