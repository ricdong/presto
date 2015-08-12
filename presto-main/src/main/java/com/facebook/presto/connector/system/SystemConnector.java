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
package com.facebook.presto.connector.system;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SystemTable;

import javax.inject.Inject;

import java.util.Set;

public class SystemConnector
        implements Connector
{
    public static final String NAME = "system";

    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorRecordSetProvider recordSetProvider;

    @Inject
    public SystemConnector(NodeManager nodeManager, Set<SystemTable> tables)
    {
        metadata = new SystemTablesMetadata(tables);
        splitManager = new SystemSplitManager(nodeManager, tables);
        recordSetProvider = new SystemRecordSetProvider(tables);
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new SystemHandleResolver();
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }
}
