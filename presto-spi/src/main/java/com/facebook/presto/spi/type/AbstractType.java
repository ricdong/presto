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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractType
        implements Type
{
    private final TypeSignature signature;
    private final Class<?> javaType;

    protected AbstractType(TypeSignature signature, Class<?> javaType)
    {
        this.signature = signature;
        this.javaType = javaType;
    }

    @Override
    public final TypeSignature getTypeSignature()
    {
        return signature;
    }

    @Override
    public String getDisplayName()
    {
        return signature.toString();
    }

    @Override
    public final Class<?> getJavaType()
    {
        return javaType;
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return Collections.unmodifiableList(new ArrayList<>());
    }

    @Override
    public boolean isComparable()
    {
        return false;
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

    @Override
    public int hash(Block block, int position)
    {
        throw new UnsupportedOperationException(getTypeSignature() + " type is not comparable");
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException(getTypeSignature() + " type is not comparable");
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException(getTypeSignature() + " type is not ordered");
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final String toString()
    {
        return getTypeSignature().toString();
    }

    @Override
    public final boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return this.getTypeSignature().equals(((Type) o).getTypeSignature());
    }

    @Override
    public final int hashCode()
    {
        return getClass().hashCode();
    }
}
