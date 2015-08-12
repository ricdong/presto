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
package com.facebook.presto.orc.reader;

import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;
import java.util.List;

public interface StreamReader
{
    void readBatch(Object vector)
            throws IOException;

    default void readBatch(Type type, Object vector)
            throws IOException
    {
        readBatch(vector);
    }

    void prepareNextRead(int batchSize);

    void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException;

    void startRowGroup(StreamSources dataStreamSources)
            throws IOException;
}
