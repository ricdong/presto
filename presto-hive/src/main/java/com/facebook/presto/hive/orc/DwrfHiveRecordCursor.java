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
package com.facebook.presto.hive.orc;

import com.facebook.hive.orc.RecordReader;
import com.facebook.hive.orc.lazy.OrcLazyObject;
import com.facebook.hive.orc.lazy.OrcLazyRow;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.HiveRecordCursor;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveUtil;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveType.HIVE_BINARY;
import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveType.HIVE_TIMESTAMP;
import static com.facebook.presto.hive.HiveUtil.bigintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.booleanPartitionKey;
import static com.facebook.presto.hive.HiveUtil.datePartitionKey;
import static com.facebook.presto.hive.HiveUtil.doublePartitionKey;
import static com.facebook.presto.hive.HiveUtil.getTableObjectInspector;
import static com.facebook.presto.hive.HiveUtil.isStructuralType;
import static com.facebook.presto.hive.HiveUtil.timestampPartitionKey;
import static com.facebook.presto.hive.util.SerDeUtils.getBlockObject;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DwrfHiveRecordCursor
        extends HiveRecordCursor
{
    private final RecordReader recordReader;

    @SuppressWarnings("FieldCanBeLocal") // include names for debugging
    private final String[] names;
    private final Type[] types;
    private final HiveType[] hiveTypes;

    private final ObjectInspector[] fieldInspectors; // only used for structured types

    private final int[] hiveColumnIndexes;

    private final boolean[] isPartitionColumn;

    private OrcLazyRow value;

    private final boolean[] loaded;
    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final Slice[] slices;
    private final Object[] objects;
    private final boolean[] nulls;

    private final long totalBytes;
    private long completedBytes;
    private boolean closed;
    private final long timeZoneCorrection;

    public DwrfHiveRecordCursor(RecordReader recordReader,
            long totalBytes,
            Properties splitSchema,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        checkNotNull(recordReader, "recordReader is null");
        checkArgument(totalBytes >= 0, "totalBytes is negative");
        checkNotNull(splitSchema, "splitSchema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(columns, "columns is null");
        checkNotNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");

        this.recordReader = recordReader;
        this.totalBytes = totalBytes;

        int size = columns.size();

        this.names = new String[size];
        this.types = new Type[size];
        this.hiveTypes = new HiveType[size];

        this.fieldInspectors = new ObjectInspector[size];

        this.hiveColumnIndexes = new int[size];

        this.isPartitionColumn = new boolean[size];

        this.loaded = new boolean[size];
        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.slices = new Slice[size];
        this.objects = new Object[size];
        this.nulls = new boolean[size];

        // DWRF uses an epoch sensitive to the JVM default timezone, so we need to correct for this
        long hiveStorageCorrection = new DateTime(2015, 1, 1, 0, 0, hiveStorageTimeZone).getMillis() - new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        long jvmCorrection = new DateTime(2015, 1, 1, 0, 0).getMillis() - new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        timeZoneCorrection = hiveStorageCorrection - jvmCorrection;

        // initialize data columns
        StructObjectInspector rowInspector = getTableObjectInspector(splitSchema);

        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);

            names[i] = column.getName();
            types[i] = typeManager.getType(column.getTypeSignature());
            hiveTypes[i] = column.getHiveType();

            if (!column.isPartitionKey()) {
                fieldInspectors[i] = rowInspector.getStructFieldRef(column.getName()).getFieldObjectInspector();
            }

            hiveColumnIndexes[i] = column.getHiveColumnIndex();
            isPartitionColumn[i] = column.isPartitionKey();
        }

        // parse requested partition columns
        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey::getName);
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(column.getName());
                checkArgument(partitionKey != null, "Unknown partition key %s", column.getName());

                byte[] bytes = partitionKey.getValue().getBytes(UTF_8);

                String name = names[columnIndex];
                Type type = types[columnIndex];
                if (HiveUtil.isHiveNull(bytes)) {
                    nulls[columnIndex] = true;
                }
                else if (BOOLEAN.equals(type)) {
                    booleans[columnIndex] = booleanPartitionKey(partitionKey.getValue(), name);
                }
                else if (BIGINT.equals(type)) {
                    longs[columnIndex] = bigintPartitionKey(partitionKey.getValue(), name);
                }
                else if (DOUBLE.equals(type)) {
                    doubles[columnIndex] = doublePartitionKey(partitionKey.getValue(), name);
                }
                else if (VARCHAR.equals(type)) {
                    slices[columnIndex] = Slices.wrappedBuffer(Arrays.copyOf(bytes, bytes.length));
                }
                else if (DATE.equals(type)) {
                    longs[columnIndex] = datePartitionKey(partitionKey.getValue(), name);
                }
                else if (TIMESTAMP.equals(type)) {
                    longs[columnIndex] = timestampPartitionKey(partitionKey.getValue(), hiveStorageTimeZone, name);
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for partition key: %s", type.getDisplayName(), name));
                }
            }
        }
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        if (!closed) {
            updateCompletedBytes();
        }
        return completedBytes;
    }

    private void updateCompletedBytes()
    {
        try {
            long newCompletedBytes = (long) (totalBytes * recordReader.getProgress());
            completedBytes = min(totalBytes, max(completedBytes, newCompletedBytes));
        }
        catch (IOException ignored) {
        }
    }

    @Override
    public Type getType(int field)
    {
        return types[field];
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            if (closed || !recordReader.hasNext()) {
                close();
                return false;
            }

            value = (OrcLazyRow) recordReader.next(value);

            // reset loaded flags
            // partition keys are already loaded, but everything else is not
            System.arraycopy(isPartitionColumn, 0, loaded, 0, isPartitionColumn.length);

            return true;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public boolean getBoolean(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, boolean.class);
        if (!loaded[fieldId]) {
            parseBooleanColumn(fieldId);
        }
        return booleans[fieldId];
    }

    private void parseBooleanColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPartitionColumn[column], "Column is a partition key");

        loaded[column] = true;

        Object object = getMaterializedValue(column);

        if (object == null) {
            nulls[column] = true;
        }
        else {
            nulls[column] = false;
            BooleanWritable booleanWritable = checkWritable(object, BooleanWritable.class);
            booleans[column] = booleanWritable.get();
        }
    }

    @Override
    public long getLong(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, long.class);
        if (!loaded[fieldId]) {
            parseLongColumn(fieldId);
        }
        return longs[fieldId];
    }

    private void parseLongColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPartitionColumn[column], "Column is a partition key");

        loaded[column] = true;
        Object object = getMaterializedValue(column);
        if (object == null) {
            nulls[column] = true;
        }
        else {
            nulls[column] = false;

            HiveType type = hiveTypes[column];
            if (hiveTypes[column].equals(HIVE_SHORT)) {
                ShortWritable shortWritable = checkWritable(object, ShortWritable.class);
                longs[column] = shortWritable.get();
            }
            else if (hiveTypes[column].equals(HIVE_TIMESTAMP)) {
                TimestampWritable timestampWritable = (TimestampWritable) object;
                long seconds = timestampWritable.getSeconds();
                int nanos = timestampWritable.getNanos();
                longs[column] = (seconds * 1000) + (nanos / 1_000_000) + timeZoneCorrection;
            }
            else if (hiveTypes[column].equals(HIVE_BYTE)) {
                ByteWritable byteWritable = checkWritable(object, ByteWritable.class);
                longs[column] = byteWritable.get();
            }
            else if (hiveTypes[column].equals(HIVE_INT)) {
                IntWritable intWritable = checkWritable(object, IntWritable.class);
                longs[column] = intWritable.get();
            }
            else if (hiveTypes[column].equals(HIVE_LONG)) {
                LongWritable longWritable = checkWritable(object, LongWritable.class);
                longs[column] = longWritable.get();
            }
            else {
                throw new RuntimeException(String.format("%s is not a valid LONG type", type));
            }
        }
    }

    @Override
    public double getDouble(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, double.class);
        if (!loaded[fieldId]) {
            parseDoubleColumn(fieldId);
        }
        return doubles[fieldId];
    }

    private void parseDoubleColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPartitionColumn[column], "Column is a partition key");

        loaded[column] = true;
        Object object = getMaterializedValue(column);
        if (object == null) {
            nulls[column] = true;
        }
        else {
            nulls[column] = false;

            HiveType type = hiveTypes[column];
            if (hiveTypes[column].equals(HIVE_FLOAT)) {
                FloatWritable floatWritable = checkWritable(object, FloatWritable.class);
                doubles[column] = floatWritable.get();
            }
            else if (hiveTypes[column].equals(HIVE_DOUBLE)) {
                DoubleWritable doubleWritable = checkWritable(object, DoubleWritable.class);
                doubles[column] = doubleWritable.get();
            }
            else {
                throw new RuntimeException(String.format("%s is not a valid DOUBLE type", type));
            }
        }
    }

    @Override
    public Slice getSlice(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Slice.class);
        if (!loaded[fieldId]) {
            parseStringColumn(fieldId);
        }
        return slices[fieldId];
    }

    private void parseStringColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPartitionColumn[column], "Column is a partition key");

        loaded[column] = true;
        nulls[column] = false;

        OrcLazyObject lazyObject = getRawValue(column);
        if (lazyObject == null) {
            nulls[column] = true;
            return;
        }

        Object value = materializeValue(lazyObject);
        if (value == null) {
            nulls[column] = true;
            return;
        }

        HiveType type = hiveTypes[column];
        if (type.equals(HIVE_STRING)) {
            Text text = checkWritable(value, Text.class);
            slices[column] = Slices.copyOf(Slices.wrappedBuffer(text.getBytes()), 0, text.getLength());
        }
        else if (type.equals(HIVE_BINARY)) {
            BytesWritable bytesWritable = checkWritable(value, BytesWritable.class);
            slices[column] = Slices.copyOf(Slices.wrappedBuffer(bytesWritable.getBytes()), 0, bytesWritable.getLength());
        }
        else {
            throw new RuntimeException(String.format("%s is not a valid STRING type", type));
        }
    }

    @Override
    public Object getObject(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Block.class);
        if (!loaded[fieldId]) {
            parseObjectColumn(fieldId);
        }
        return objects[fieldId];
    }

    private void parseObjectColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPartitionColumn[column], "Column is a partition key");

        loaded[column] = true;
        nulls[column] = false;

        OrcLazyObject lazyObject = getRawValue(column);
        if (lazyObject == null) {
            nulls[column] = true;
            return;
        }

        Object value = materializeValue(lazyObject);
        if (value == null) {
            nulls[column] = true;
            return;
        }

        objects[column] = getBlockObject(types[column], lazyObject, fieldInspectors[column]);
    }

    @Override
    public boolean isNull(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        if (!loaded[fieldId]) {
            parseColumn(fieldId);
        }
        return nulls[fieldId];
    }

    private void parseColumn(int column)
    {
        if (types[column].equals(BOOLEAN)) {
            parseBooleanColumn(column);
        }
        else if (types[column].equals(BIGINT)) {
            parseLongColumn(column);
        }
        else if (types[column].equals(DOUBLE)) {
            parseDoubleColumn(column);
        }
        else if (types[column].equals(VARCHAR) || types[column].equals(VARBINARY)) {
            parseStringColumn(column);
        }
        else if (isStructuralType(hiveTypes[column])) {
            parseObjectColumn(column);
        }
        else if (types[column].equals(TIMESTAMP)) {
            parseLongColumn(column);
        }
        else {
            throw new UnsupportedOperationException("Unsupported column type: " + types[column]);
        }
    }

    private void validateType(int fieldId, Class<?> javaType)
    {
        if (types[fieldId].getJavaType() != javaType) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(String.format("Expected field to be %s, actual %s (field %s)", javaType.getName(), types[fieldId].getJavaType().getName(), fieldId));
        }
    }

    private static Object materializeValue(OrcLazyObject object)
    {
        try {
            return object.materialize();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private OrcLazyObject getRawValue(int column)
    {
        return this.value.getFieldValue(hiveColumnIndexes[column]);
    }

    private Object getMaterializedValue(int column)
    {
        OrcLazyObject value = getRawValue(column);
        return (value == null) ? null : materializeValue(value);
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        updateCompletedBytes();

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static <T extends Writable> T checkWritable(Object object, Class<T> clazz)
    {
        return checkType(object, clazz, HIVE_BAD_DATA, "materialized object");
    }
}
