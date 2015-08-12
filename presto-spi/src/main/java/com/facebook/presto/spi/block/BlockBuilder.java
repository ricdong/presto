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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;

public interface BlockBuilder
        extends Block
{
    /**
     * Write a byte to the current entry;
     */
    BlockBuilder writeByte(int value);

    /**
     * Write a short to the current entry;
     */
    BlockBuilder writeShort(int value);

    /**
     * Write a int to the current entry;
     */
    BlockBuilder writeInt(int value);

    /**
     * Write a long to the current entry;
     */
    BlockBuilder writeLong(long value);

    /**
     * Write a float to the current entry;
     */
    BlockBuilder writeFloat(float v);

    /**
     * Write a double to the current entry;
     */
    BlockBuilder writeDouble(double value);

    /**
     * Write a byte sequences to the current entry;
     */
    BlockBuilder writeBytes(Slice source, int sourceIndex, int length);

    /**
     * Write an object to the current entry;
     */
    default BlockBuilder writeObject(Object value)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Return a writer to the current entry. The caller can operate on the returned caller to incrementally build the object. This is generally more efficient than
     * building the object elsewhere and call writeObject afterwards because a large chunk of memory could potentially be unnecessarily copied in this process.
     */
    default BlockBuilder beginBlockEntry()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Write a byte to the current entry;
     */
    BlockBuilder closeEntry();

    /**
     * Appends a null value to the block.
     */
    BlockBuilder appendNull();

    /**
     * Builds the block. This method can be called multiple times.
     */
    Block build();
}
