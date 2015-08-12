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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.operator.aggregation.KeyValuePairs;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.MapType;

public class KeyValuePairStateSerializer
        implements AccumulatorStateSerializer<KeyValuePairsState>
{
    private final MapType mapType;

    public KeyValuePairStateSerializer(Type keyType, Type valueType)
    {
        this.mapType = new MapType(keyType, valueType);
    }

    @Override
    public Type getSerializedType()
    {
        return mapType;
    }

    @Override
    public void serialize(KeyValuePairsState state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            mapType.writeObject(out, state.get().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, KeyValuePairsState state)
    {
        if (!block.isNull(index)) {
            state.set(new KeyValuePairs(mapType.getObject(block, index), mapType.getKeyType(), mapType.getValueType()));
        }
    }
}
