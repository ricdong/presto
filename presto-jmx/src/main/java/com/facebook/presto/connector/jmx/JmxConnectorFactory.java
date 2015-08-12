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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.NodeManager;

import javax.management.MBeanServer;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class JmxConnectorFactory
        implements ConnectorFactory
{
    private final MBeanServer mbeanServer;
    private final NodeManager nodeManager;

    public JmxConnectorFactory(MBeanServer mbeanServer, NodeManager nodeManager)
    {
        this.mbeanServer = requireNonNull(mbeanServer, "mbeanServer is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public String getName()
    {
        return "jmx";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> properties)
    {
        return new Connector()
        {
            @Override
            public ConnectorHandleResolver getHandleResolver()
            {
                return new JmxHandleResolver();
            }

            @Override
            public ConnectorMetadata getMetadata()
            {
                return new JmxMetadata(connectorId, mbeanServer);
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new JmxSplitManager(connectorId, nodeManager);
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new JmxRecordSetProvider(mbeanServer, nodeManager.getCurrentNode().getNodeIdentifier());
            }
        };
    }
}
