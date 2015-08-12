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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import parquet.column.Dictionary;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.ParquetRecordReader;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.util.ContextUtil;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveUtil.bigintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.booleanPartitionKey;
import static com.facebook.presto.hive.HiveUtil.doublePartitionKey;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
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
import static java.util.stream.Collectors.toList;
import static parquet.schema.OriginalType.LIST;
import static parquet.schema.OriginalType.MAP;
import static parquet.schema.OriginalType.MAP_KEY_VALUE;

class ParquetHiveRecordCursor
        extends HiveRecordCursor
{
    private final ParquetRecordReader<FakeParquetRecord> recordReader;

    @SuppressWarnings("FieldCanBeLocal") // include names for debugging
    private final String[] names;
    private final Type[] types;

    private final boolean[] isPartitionColumn;

    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final Slice[] slices;
    private final Object[] objects;
    private final boolean[] nulls;
    private final boolean[] nullsRowDefault;

    private final long totalBytes;
    private long completedBytes;
    private boolean closed;

    public ParquetHiveRecordCursor(
            Configuration configuration,
            Path path,
            long start,
            long length,
            Properties splitSchema,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            boolean useParquetColumnNames,
            TypeManager typeManager)
    {
        checkNotNull(path, "path is null");
        checkArgument(length >= 0, "totalBytes is negative");
        checkNotNull(splitSchema, "splitSchema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(columns, "columns is null");

        this.totalBytes = length;

        int size = columns.size();

        this.names = new String[size];
        this.types = new Type[size];

        this.isPartitionColumn = new boolean[size];

        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.slices = new Slice[size];
        this.objects = new Object[size];
        this.nulls = new boolean[size];
        this.nullsRowDefault = new boolean[size];

        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);

            names[i] = column.getName();
            types[i] = typeManager.getType(column.getTypeSignature());

            isPartitionColumn[i] = column.isPartitionKey();
            nullsRowDefault[i] = !column.isPartitionKey();
        }

        this.recordReader = createParquetRecordReader(configuration, path, start, length, columns, useParquetColumnNames);

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
                    nullsRowDefault[columnIndex] = true;
                }
                else if (type.equals(BOOLEAN)) {
                    booleans[columnIndex] = booleanPartitionKey(partitionKey.getValue(), name);
                }
                else if (type.equals(BIGINT)) {
                    longs[columnIndex] = bigintPartitionKey(partitionKey.getValue(), name);
                }
                else if (type.equals(DOUBLE)) {
                    doubles[columnIndex] = doublePartitionKey(partitionKey.getValue(), name);
                }
                else if (type.equals(VARCHAR)) {
                    slices[columnIndex] = Slices.wrappedBuffer(bytes);
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
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
            // reset null flags
            System.arraycopy(nullsRowDefault, 0, nulls, 0, isPartitionColumn.length);

            if (closed || !recordReader.nextKeyValue()) {
                close();
                return false;
            }

            return true;
        }
        catch (IOException | RuntimeException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public boolean getBoolean(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, boolean.class);
        return booleans[fieldId];
    }

    @Override
    public long getLong(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, long.class);
        return longs[fieldId];
    }

    @Override
    public double getDouble(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, double.class);
        return doubles[fieldId];
    }

    @Override
    public Slice getSlice(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Slice.class);
        return slices[fieldId];
    }

    @Override
    public Object getObject(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Block.class);
        return objects[fieldId];
    }

    @Override
    public boolean isNull(int fieldId)
    {
        checkState(!closed, "Cursor is closed");
        return nulls[fieldId];
    }

    private void validateType(int fieldId, Class<?> javaType)
    {
        if (types[fieldId].getJavaType() != javaType) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(String.format("Expected field to be %s, actual %s (field %s)", javaType.getName(), types[fieldId].getJavaType().getName(), fieldId));
        }
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

    private ParquetRecordReader<FakeParquetRecord> createParquetRecordReader(
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<HiveColumnHandle> columns,
            boolean useParquetColumnNames)
    {
        try {
            ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(configuration, path);
            List<BlockMetaData> blocks = parquetMetadata.getBlocks();
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();

            PrestoReadSupport readSupport = new PrestoReadSupport(useParquetColumnNames, columns, fileMetaData.getSchema());
            ReadContext readContext = readSupport.init(configuration, fileMetaData.getKeyValueMetaData(), fileMetaData.getSchema());

            List<BlockMetaData> splitGroup = new ArrayList<>();
            long splitStart = start;
            long splitLength = length;
            for (BlockMetaData block : blocks) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
                    splitGroup.add(block);
                }
            }

            ParquetInputSplit split;

            split = new ParquetInputSplit(path,
                    splitStart,
                    splitLength,
                    null,
                    splitGroup,
                    readContext.getRequestedSchema().toString(),
                    fileMetaData.getSchema().toString(),
                    fileMetaData.getKeyValueMetaData(),
                    readContext.getReadSupportMetadata());

            TaskAttemptContext taskContext = ContextUtil.newTaskAttemptContext(configuration, new TaskAttemptID());
            ParquetRecordReader<FakeParquetRecord> realReader = new PrestoParquetRecordReader(readSupport);
            realReader.initialize(split, taskContext);
            return realReader;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    public class PrestoParquetRecordReader
            extends ParquetRecordReader<FakeParquetRecord>
    {
        public PrestoParquetRecordReader(PrestoReadSupport readSupport)
        {
            super(readSupport);
        }
    }

    public final class PrestoReadSupport
            extends ReadSupport<FakeParquetRecord>
    {
        private final boolean useParquetColumnNames;
        private final List<HiveColumnHandle> columns;
        private final List<Converter> converters;

        public PrestoReadSupport(boolean useParquetColumnNames, List<HiveColumnHandle> columns, MessageType messageType)
        {
            this.columns = columns;
            this.useParquetColumnNames = useParquetColumnNames;

            ImmutableList.Builder<Converter> converters = ImmutableList.builder();
            for (int i = 0; i < columns.size(); i++) {
                HiveColumnHandle column = columns.get(i);
                if (!column.isPartitionKey()) {
                    parquet.schema.Type parquetType = getParquetType(column, messageType);
                    if (parquetType == null) {
                        continue;
                    }
                    if (parquetType.isPrimitive()) {
                        converters.add(new ParquetPrimitiveColumnConverter(i));
                    }
                    else {
                        GroupType groupType = parquetType.asGroupType();
                        switch (column.getTypeSignature().getBase()) {
                            case ARRAY:
                                ParquetColumnConverter listConverter = new ParquetColumnConverter(new ParquetListConverter(types[i], groupType.getName(), groupType), i);
                                converters.add(listConverter);
                                break;
                            case StandardTypes.MAP:
                                ParquetColumnConverter mapConverter = new ParquetColumnConverter(new ParquetMapConverter(types[i], groupType.getName(), groupType), i);
                                converters.add(mapConverter);
                                break;
                            case ROW:
                                ParquetColumnConverter rowConverter = new ParquetColumnConverter(new ParquetStructConverter(types[i], groupType.getName(), groupType), i);
                                converters.add(rowConverter);
                                break;
                            default:
                                throw new IllegalArgumentException("Group column " + groupType.getName() + " type " + groupType.getOriginalType() + " not supported");
                        }
                    }
                }
            }
            this.converters = converters.build();
        }

        @Override
        @SuppressWarnings("deprecation")
        public ReadContext init(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType messageType)
        {
            List<parquet.schema.Type> fields = columns.stream()
                    .filter(column -> !column.isPartitionKey())
                    .map(column -> getParquetType(column, messageType))
                    .filter(Objects::nonNull)
                    .collect(toList());
            MessageType requestedProjection = new MessageType(messageType.getName(), fields);
            return new ReadContext(requestedProjection);
        }

        @Override
        public RecordMaterializer<FakeParquetRecord> prepareForRead(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType fileSchema,
                ReadContext readContext)
        {
            return new ParquetRecordConverter(converters);
        }

        private parquet.schema.Type getParquetType(HiveColumnHandle column, MessageType messageType)
        {
            if (useParquetColumnNames) {
                if (messageType.containsField(column.getName())) {
                    return messageType.getType(column.getName());
                }
                return null;
            }

            if (column.getHiveColumnIndex() < messageType.getFieldCount()) {
                return messageType.getType(column.getHiveColumnIndex());
            }
            return null;
        }
    }

    private static class ParquetRecordConverter
            extends RecordMaterializer<FakeParquetRecord>
    {
        private final ParquetGroupConverter groupConverter;

        public ParquetRecordConverter(List<Converter> converters)
        {
            groupConverter = new ParquetGroupConverter(converters);
        }

        @Override
        public FakeParquetRecord getCurrentRecord()
        {
            // Parquet skips the record if it is null, so we need non-null record
            return FakeParquetRecord.MATERIALIZE_RECORD;
        }

        @Override
        public GroupConverter getRootConverter()
        {
            return groupConverter;
        }
    }

    private enum FakeParquetRecord
    {
        MATERIALIZE_RECORD
    }

    public static class ParquetGroupConverter
            extends GroupConverter
    {
        private final List<Converter> converters;

        public ParquetGroupConverter(List<Converter> converters)
        {
            this.converters = converters;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return converters.get(fieldIndex);
        }

        @Override
        public void start()
        {
        }

        @Override
        public void end()
        {
        }
    }

    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    private class ParquetPrimitiveColumnConverter
            extends PrimitiveConverter
    {
        private final int fieldIndex;

        private ParquetPrimitiveColumnConverter(int fieldIndex)
        {
            this.fieldIndex = fieldIndex;
        }

        @Override
        public boolean isPrimitive()
        {
            return true;
        }

        @Override
        public PrimitiveConverter asPrimitiveConverter()
        {
            return this;
        }

        @Override
        public boolean hasDictionarySupport()
        {
            return false;
        }

        @Override
        public void setDictionary(Dictionary dictionary)
        {
        }

        @Override
        public void addValueFromDictionary(int dictionaryId)
        {
        }

        @Override
        public void addBoolean(boolean value)
        {
            nulls[fieldIndex] = false;
            booleans[fieldIndex] = value;
        }

        @Override
        public void addDouble(double value)
        {
            nulls[fieldIndex] = false;
            doubles[fieldIndex] = value;
        }

        @Override
        public void addLong(long value)
        {
            nulls[fieldIndex] = false;
            longs[fieldIndex] = value;
        }

        @Override
        public void addBinary(Binary value)
        {
            nulls[fieldIndex] = false;
            slices[fieldIndex] = Slices.wrappedBuffer(value.getBytes());
        }

        @Override
        public void addFloat(float value)
        {
            nulls[fieldIndex] = false;
            doubles[fieldIndex] = value;
        }

        @Override
        public void addInt(int value)
        {
            nulls[fieldIndex] = false;
            longs[fieldIndex] = value;
        }
    }

    public class ParquetColumnConverter
            extends GroupConverter
    {
        private final GroupedConverter groupedConverter;
        private final int fieldIndex;

        public ParquetColumnConverter(GroupedConverter groupedConverter, int fieldIndex)
        {
            this.groupedConverter = groupedConverter;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return groupedConverter.getConverter(fieldIndex);
        }

        @Override
        public void start()
        {
            groupedConverter.beforeValue(null);
            groupedConverter.start();
        }

        @Override
        public void end()
        {
            groupedConverter.afterValue();
            groupedConverter.end();

            nulls[fieldIndex] = false;

            objects[fieldIndex] = groupedConverter.getBlock();
        }
    }

    private interface BlockConverter
    {
        void beforeValue(BlockBuilder builder);

        void afterValue();
    }

    private abstract static class GroupedConverter
            extends GroupConverter
            implements BlockConverter
    {
        public abstract Block getBlock();
    }

    private static BlockConverter createConverter(Type prestoType, String columnName, parquet.schema.Type type)
    {
        if (type.isPrimitive()) {
            return new ParquetPrimitiveConverter();
        }
        else if (type.getOriginalType() == LIST) {
            return new ParquetListConverter(prestoType, columnName, type.asGroupType());
        }
        else if (type.getOriginalType() == MAP) {
            return new ParquetMapConverter(prestoType, columnName, type.asGroupType());
        }
        else if (type.getOriginalType() == null) {
            // struct does not have an original type
            return new ParquetStructConverter(prestoType, columnName, type.asGroupType());
        }
        throw new IllegalArgumentException("Unsupported type " + type);
    }

    private static class ParquetStructConverter
            extends GroupedConverter
    {
        private static final int NULL_BUILDER_POSITIONS_THRESHOLD = 100;
        private static final int NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD = 32768;

        private final Type rowType;

        private final List<BlockConverter> converters;
        private BlockBuilder builder;
        private BlockBuilder nullBuilder; // used internally when builder is set to null
        private BlockBuilder currentEntryBuilder;

        public ParquetStructConverter(Type prestoType, String columnName, GroupType entryType)
        {
            checkArgument(ROW.equals(prestoType.getTypeSignature().getBase()));
            List<Type> prestoTypeParameters = prestoType.getTypeParameters();
            List<parquet.schema.Type> fieldTypes = entryType.getFields();
            checkArgument(prestoTypeParameters.size() == fieldTypes.size());

            this.rowType = prestoType;

            ImmutableList.Builder<BlockConverter> converters = ImmutableList.builder();
            for (int i = 0; i < prestoTypeParameters.size(); i++) {
                parquet.schema.Type fieldType = fieldTypes.get(i);
                converters.add(createConverter(prestoTypeParameters.get(i), columnName + "." + fieldType.getName(), fieldType));
            }
            this.converters = converters.build();
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return (Converter) converters.get(fieldIndex);
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        public void start()
        {
            if (builder == null) {
                if (nullBuilder == null || (nullBuilder.getPositionCount() >= NULL_BUILDER_POSITIONS_THRESHOLD && nullBuilder.getSizeInBytes() >= NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD)) {
                    nullBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), NULL_BUILDER_POSITIONS_THRESHOLD);
                }
                currentEntryBuilder = nullBuilder.beginBlockEntry();
            }
            else {
                currentEntryBuilder = builder.beginBlockEntry();
            }
            for (BlockConverter converter : converters) {
                converter.beforeValue(currentEntryBuilder);
            }
        }

        @Override
        public void end()
        {
            for (BlockConverter converter : converters) {
                converter.afterValue();
            }
            if (builder == null) {
                nullBuilder.closeEntry();
            }
            else {
                builder.closeEntry();
            }
        }

        @Override
        public void afterValue()
        {
        }

        @Override
        public Block getBlock()
        {
            checkState(builder == null && nullBuilder != null); // check that user requested a result block (builder == null), and the program followed the request (nullBuilder != null)
            return nullBuilder.getObject(nullBuilder.getPositionCount() - 1, Block.class);
        }
    }

    private static class ParquetListConverter
            extends GroupedConverter
    {
        private static final int NULL_BUILDER_POSITIONS_THRESHOLD = 100;
        private static final int NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD = 32768;

        private final Type arrayType;

        private final BlockConverter elementConverter;
        private BlockBuilder builder;
        private BlockBuilder nullBuilder; // used internally when builder is set to null
        private BlockBuilder currentEntryBuilder;

        public ParquetListConverter(Type prestoType, String columnName, GroupType listType)
        {
            checkArgument(listType.getFieldCount() == 1,
                    "Expected LIST column '%s' to only have one field, but has %s fields",
                    columnName,
                    listType.getFieldCount());
            checkArgument(ARRAY.equals(prestoType.getTypeSignature().getBase()));

            this.arrayType = prestoType;

            // The Parquet specification requires that the element value of a
            // LIST type be wrapped in an inner repeated group, like so:
            //
            // optional group listField (LIST) {
            //   repeated group list {
            //     optional int element
            //   }
            // }
            //
            // However, some parquet libraries don't follow this spec. The
            // compatibility rules used here are specified in the Parquet
            // documentation at http://git.io/vf3wG.
            parquet.schema.Type elementType = listType.getType(0);
            if (elementType.isPrimitive() ||
                    elementType.asGroupType().getFieldCount() > 1 ||
                    elementType.getName().equals("array") ||
                    elementType.getName().equals(listType.getName() + "_tuple")) {
                elementConverter = createConverter(prestoType.getTypeParameters().get(0), columnName + ".element", elementType);
            }
            else {
                elementConverter = new ParquetListEntryConverter(prestoType.getTypeParameters().get(0), columnName, elementType.asGroupType());
            }
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return (Converter) elementConverter;
            }
            throw new IllegalArgumentException("LIST field must be 0 not " + fieldIndex);
        }

        @Override
        public void start()
        {
            if (builder == null) {
                if (nullBuilder == null || (nullBuilder.getPositionCount() >= NULL_BUILDER_POSITIONS_THRESHOLD && nullBuilder.getSizeInBytes() >= NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD)) {
                    nullBuilder = arrayType.createBlockBuilder(new BlockBuilderStatus(), NULL_BUILDER_POSITIONS_THRESHOLD);
                }
                currentEntryBuilder = nullBuilder.beginBlockEntry();
            }
            else {
                currentEntryBuilder = builder.beginBlockEntry();
            }
            elementConverter.beforeValue(currentEntryBuilder);
        }

        @Override
        public void end()
        {
            elementConverter.afterValue();

            if (builder == null) {
                nullBuilder.closeEntry();
            }
            else {
                builder.closeEntry();
            }
        }

        @Override
        public void afterValue()
        {
        }

        @Override
        public Block getBlock()
        {
            checkState(builder == null && nullBuilder != null); // check that user requested a result block (builder == null), and the program followed the request (nullBuilder != null)
            return nullBuilder.getObject(nullBuilder.getPositionCount() - 1, Block.class);
        }
    }

    private static class ParquetListEntryConverter
            extends GroupConverter
            implements BlockConverter
    {
        private final BlockConverter elementConverter;

        private BlockBuilder builder;

        public ParquetListEntryConverter(Type prestoType, String columnName, GroupType elementType)
        {
            checkArgument(elementType.getOriginalType() == null,
                    "Expected LIST column '%s' field to be type STRUCT, but is %s",
                    columnName,
                    elementType);

            checkArgument(elementType.getFieldCount() == 1,
                    "Expected LIST column '%s' element to have one field, but has %s fields",
                    columnName,
                    elementType.getFieldCount());

            checkArgument(elementType.getFieldName(0).equals("array_element"),
                    "Expected LIST column '%s' entry field 0 to be named 'array_element', but is named %s",
                    columnName,
                    elementType.getFieldName(0));

            elementConverter = createConverter(prestoType, columnName + ".element", elementType.getType(0));
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return (Converter) elementConverter;
            }
            throw new IllegalArgumentException("LIST entry field must be 0 not " + fieldIndex);
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        public void start()
        {
            elementConverter.beforeValue(builder);
        }

        @Override
        public void end()
        {
            elementConverter.afterValue();
        }

        @Override
        public void afterValue()
        {
        }
    }

    private static class ParquetMapConverter
            extends GroupedConverter
    {
        private static final int NULL_BUILDER_POSITIONS_THRESHOLD = 100;
        private static final int NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD = 32768;

        private final Type mapType;

        private final ParquetMapEntryConverter entryConverter;
        private BlockBuilder builder;
        private BlockBuilder nullBuilder; // used internally when builder is set to null
        private BlockBuilder currentEntryBuilder;

        public ParquetMapConverter(Type type, String columnName, GroupType mapType)
        {
            checkArgument(mapType.getFieldCount() == 1,
                    "Expected MAP column '%s' to only have one field, but has %s fields",
                    mapType.getName(),
                    mapType.getFieldCount());

            this.mapType = type;

            parquet.schema.Type entryType = mapType.getFields().get(0);

            // original versions of parquet had map end entry swapped
            if (mapType.getOriginalType() != MAP_KEY_VALUE) {
                checkArgument(entryType.getOriginalType() == MAP_KEY_VALUE,
                        "Expected MAP column '%s' field to be type %s, but is %s",
                        mapType.getName(),
                        MAP_KEY_VALUE,
                        entryType);
            }

            entryConverter = new ParquetMapEntryConverter(type, columnName + ".entry", entryType.asGroupType());
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return entryConverter;
            }
            throw new IllegalArgumentException("Map field must be 0 not " + fieldIndex);
        }

        @Override
        public void start()
        {
            if (builder == null) {
                if (nullBuilder == null || (nullBuilder.getPositionCount() >= NULL_BUILDER_POSITIONS_THRESHOLD && nullBuilder.getSizeInBytes() >= NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD)) {
                    nullBuilder = mapType.createBlockBuilder(new BlockBuilderStatus(), NULL_BUILDER_POSITIONS_THRESHOLD);
                }
                currentEntryBuilder = nullBuilder.beginBlockEntry();
            }
            else {
                currentEntryBuilder = builder.beginBlockEntry();
            }
            entryConverter.beforeValue(currentEntryBuilder);
        }

        @Override
        public void end()
        {
            entryConverter.afterValue();

            if (builder == null) {
                nullBuilder.closeEntry();
            }
            else {
                builder.closeEntry();
            }
        }

        @Override
        public void afterValue()
        {
        }

        @Override
        public Block getBlock()
        {
            checkState(builder == null && nullBuilder != null); // check that user requested a result block (builder == null), and the program followed the request (nullBuilder != null)
            return nullBuilder.getObject(nullBuilder.getPositionCount() - 1, Block.class);
        }
    }

    private static class ParquetMapEntryConverter
            extends GroupConverter
            implements BlockConverter
    {
        private final BlockConverter keyConverter;
        private final BlockConverter valueConverter;

        private BlockBuilder builder;

        public ParquetMapEntryConverter(Type prestoType, String columnName, GroupType entryType)
        {
            checkArgument(StandardTypes.MAP.equals(prestoType.getTypeSignature().getBase()));
            // original version of parquet used null for entry due to a bug
            if (entryType.getOriginalType() != null) {
                checkArgument(entryType.getOriginalType() == MAP_KEY_VALUE,
                        "Expected MAP column '%s' field to be type %s, but is %s",
                        columnName,
                        MAP_KEY_VALUE,
                        entryType);
            }

            GroupType entryGroupType = entryType.asGroupType();
            checkArgument(entryGroupType.getFieldCount() == 2,
                    "Expected MAP column '%s' entry to have two fields, but has %s fields",
                    columnName,
                    entryGroupType.getFieldCount());
            checkArgument(entryGroupType.getFieldName(0).equals("key"),
                    "Expected MAP column '%s' entry field 0 to be named 'key', but is named %s",
                    columnName,
                    entryGroupType.getFieldName(0));
            checkArgument(entryGroupType.getFieldName(1).equals("value"),
                    "Expected MAP column '%s' entry field 1 to be named 'value', but is named %s",
                    columnName,
                    entryGroupType.getFieldName(1));
            checkArgument(entryGroupType.getType(0).isPrimitive(),
                    "Expected MAP column '%s' entry field 0 to be primitive, but is named %s",
                    columnName,
                    entryGroupType.getType(0));

            keyConverter = createConverter(prestoType.getTypeParameters().get(0), columnName + ".key", entryGroupType.getFields().get(0));
            valueConverter = createConverter(prestoType.getTypeParameters().get(1), columnName + ".value", entryGroupType.getFields().get(1));
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return (Converter) keyConverter;
            }
            if (fieldIndex == 1) {
                return (Converter) valueConverter;
            }
            throw new IllegalArgumentException("Map entry field must be 0 or 1 not " + fieldIndex);
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        public void start()
        {
            keyConverter.beforeValue(builder);
            valueConverter.beforeValue(builder);
        }

        @Override
        public void end()
        {
            keyConverter.afterValue();
            valueConverter.afterValue();
        }

        @Override
        public void afterValue()
        {
        }
    }

    private static class ParquetPrimitiveConverter
            extends PrimitiveConverter
            implements BlockConverter
    {
        private BlockBuilder builder;
        private boolean wroteValue;

        public ParquetPrimitiveConverter()
        {
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = checkNotNull(builder, "parent builder is null");
            wroteValue = false;
        }

        @Override
        public void afterValue()
        {
            if (wroteValue) {
                return;
            }

            builder.appendNull();
        }

        @Override
        public boolean isPrimitive()
        {
            return true;
        }

        @Override
        public PrimitiveConverter asPrimitiveConverter()
        {
            return this;
        }

        @Override
        public boolean hasDictionarySupport()
        {
            return false;
        }

        @Override
        public void setDictionary(Dictionary dictionary)
        {
        }

        @Override
        public void addValueFromDictionary(int dictionaryId)
        {
        }

        @Override
        public void addBoolean(boolean value)
        {
            BOOLEAN.writeBoolean(builder, value);
            wroteValue = true;
        }

        @Override
        public void addDouble(double value)
        {
            DOUBLE.writeDouble(builder, value);
            wroteValue = true;
        }

        @Override
        public void addLong(long value)
        {
            BIGINT.writeLong(builder, value);
            wroteValue = true;
        }

        @Override
        public void addBinary(Binary value)
        {
            VARBINARY.writeSlice(builder, Slices.wrappedBuffer(value.getBytes()));
            wroteValue = true;
        }

        @Override
        public void addFloat(float value)
        {
            DOUBLE.writeDouble(builder, value);
            wroteValue = true;
        }

        @Override
        public void addInt(int value)
        {
            BIGINT.writeLong(builder, value);
            wroteValue = true;
        }
    }
}
