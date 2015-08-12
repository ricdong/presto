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
package com.facebook.presto.split;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordPageSource;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class RecordPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private ConnectorRecordSetProvider recordSetProvider;

    public RecordPageSourceProvider(ConnectorRecordSetProvider recordSetProvider)
    {
        this.recordSetProvider = checkNotNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        return new RecordPageSource(recordSetProvider.getRecordSet(session, split, columns));
    }
}
