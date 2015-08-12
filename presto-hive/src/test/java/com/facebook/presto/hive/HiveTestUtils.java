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

import com.facebook.presto.hive.orc.DwrfPageSourceFactory;
import com.facebook.presto.hive.orc.DwrfRecordCursorProvider;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.hive.orc.OrcRecordCursorProvider;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.type.TypeUtils.appendToBlockBuilder;
import static java.util.Locale.ENGLISH;

public final class HiveTestUtils
{
    private HiveTestUtils()
    {
    }

    public static final ConnectorSession SESSION = new TestingConnectorSession(
            "user",
            UTC_KEY,
            ENGLISH,
            System.currentTimeMillis(),
            new HiveSessionProperties(new HiveClientConfig()).getSessionProperties(),
            ImmutableMap.of());

    public static final TypeRegistry TYPE_MANAGER = new TypeRegistry();

    public static final ImmutableSet<HivePageSourceFactory> DEFAULT_HIVE_DATA_STREAM_FACTORIES = ImmutableSet.<HivePageSourceFactory>builder()
            .add(new RcFilePageSourceFactory(TYPE_MANAGER))
            .add(new OrcPageSourceFactory(TYPE_MANAGER))
            .add(new DwrfPageSourceFactory(TYPE_MANAGER))
            .build();

    public static final ImmutableSet<HiveRecordCursorProvider> DEFAULT_HIVE_RECORD_CURSOR_PROVIDER = ImmutableSet.<HiveRecordCursorProvider>builder()
            .add(new OrcRecordCursorProvider())
            .add(new ParquetRecordCursorProvider(false))
            .add(new DwrfRecordCursorProvider())
            .add(new ColumnarTextHiveRecordCursorProvider())
            .add(new ColumnarBinaryHiveRecordCursorProvider())
            .add(new GenericHiveRecordCursorProvider())
            .build();

    public static List<Type> getTypes(List<? extends ColumnHandle> columnHandles)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles) {
            types.add(TYPE_MANAGER.getType(((HiveColumnHandle) columnHandle).getTypeSignature()));
        }
        return types.build();
    }

    public static Block arrayBlockOf(Type elementType, Object... values)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), 1024);
        for (Object value : values) {
            appendToBlockBuilder(elementType, value, blockBuilder);
        }
        return blockBuilder.build();
    }

    public static Block mapBlockOf(Type keyType, Type valueType, Object key, Object value)
    {
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueType), new BlockBuilderStatus(), 1024);
        appendToBlockBuilder(keyType, key, blockBuilder);
        appendToBlockBuilder(valueType, value, blockBuilder);
        return blockBuilder.build();
    }

    public static Block rowBlockOf(List<Type> parameterTypes, Object... values)
    {
        InterleavedBlockBuilder blockBuilder = new InterleavedBlockBuilder(parameterTypes, new BlockBuilderStatus(), 1024);
        for (int i = 0; i < values.length; i++) {
            appendToBlockBuilder(parameterTypes.get(i), values[i], blockBuilder);
        }
        return blockBuilder.build();
    }
}
