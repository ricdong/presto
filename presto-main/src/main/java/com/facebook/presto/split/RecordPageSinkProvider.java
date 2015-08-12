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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordPageSink;

import static com.google.common.base.Preconditions.checkNotNull;

public class RecordPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final ConnectorRecordSinkProvider recordSinkProvider;

    public RecordPageSinkProvider(ConnectorRecordSinkProvider recordSinkProvider)
    {
        this.recordSinkProvider = checkNotNull(recordSinkProvider, "recordSinkProvider is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        return new RecordPageSink(recordSinkProvider.getRecordSink(session, outputTableHandle));
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return new RecordPageSink(recordSinkProvider.getRecordSink(session, insertTableHandle));
    }
}
