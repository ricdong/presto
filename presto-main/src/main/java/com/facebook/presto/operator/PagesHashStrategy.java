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
package com.facebook.presto.operator;

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;

public interface PagesHashStrategy
{
    /**
     * Gets the total of the columns held in in this PagesHashStrategy.  This includes both the hashed
     * and non-hashed columns.
     */
    int getChannelCount();

    /**
     * Appends all values at the specified position to the page builder starting at {@code outputChannelOffset}.
     */
    void appendTo(int blockIndex, int position, PageBuilder pageBuilder, int outputChannelOffset);

    /**
     * Calculates the hash code the hashed columns in this PagesHashStrategy at the specified position.
     */
    int hashPosition(int blockIndex, int position);

    /**
     * Calculates the hash code at {@code position} in {@code blocks}. Blocks must have the same number of
     * entries as the hashed columns and each entry is expected to be the same type.
     */
    int hashRow(int position, Block... blocks);

    /**
     * Compares the values in the specified blocks.  The values are compared positionally, so {@code leftBlocks}
     * and {@code rightBlocks} must have the same number of entries as the hashed columns and each entry
     * is expected to be the same type.
     */
    boolean rowEqualsRow(int leftPosition, Block[] leftBlocks, int rightPosition, Block[] rightBlocks);

    /**
     * Compares the hashed columns in this PagesHashStrategy to the values in the specified blocks.  The
     * values are compared positionally, so {@code rightBlocks} must have the same number of entries as
     * the hashed columns and each entry is expected to be the same type.
     */
    boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, Block... rightBlocks);

    /**
     * Compares the hashed columns in this PagesHashStrategy at the specified positions.
     */
    boolean positionEqualsPosition(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition);
}
