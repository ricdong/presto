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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;

public class RaptorPlugin
        implements Plugin
{
    private final String name;
    private final Module metadataModule;
    private final Map<String, Module> backupProviders;

    private Map<String, String> optionalConfig = ImmutableMap.of();
    private NodeManager nodeManager;
    private PageSorter pageSorter;
    private BlockEncodingSerde blockEncodingSerde;
    private TypeManager typeManager;

    public RaptorPlugin()
    {
        this(getPluginInfo());
    }

    private RaptorPlugin(PluginInfo info)
    {
        this(info.getName(), info.getMetadataModule(), info.getBackupProviders());
    }

    public RaptorPlugin(String name, Module metadataModule, Map<String, Module> backupProviders)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.metadataModule = checkNotNull(metadataModule, "metadataModule is null");
        this.backupProviders = ImmutableMap.copyOf(checkNotNull(backupProviders, "backupProviders is null"));
    }

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Inject
    public void setNodeManager(NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    @Inject
    public void setPageSorter(PageSorter pageSorter)
    {
        this.pageSorter = pageSorter;
    }

    @Inject
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        this.blockEncodingSerde = checkNotNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    @Inject
    public void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        checkState(nodeManager != null, "NodeManager has not been set");
        checkState(blockEncodingSerde != null, "BlockEncodingSerde has not been set");
        checkState(typeManager != null, "TypeManager has not been set");

        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(new RaptorConnectorFactory(
                    name,
                    metadataModule,
                    backupProviders,
                    optionalConfig,
                    nodeManager,
                    pageSorter,
                    blockEncodingSerde,
                    typeManager)));
        }
        return ImmutableList.of();
    }

    private static PluginInfo getPluginInfo()
    {
        ClassLoader classLoader = RaptorPlugin.class.getClassLoader();
        ServiceLoader<PluginInfo> loader = ServiceLoader.load(PluginInfo.class, classLoader);
        List<PluginInfo> list = ImmutableList.copyOf(loader);
        return list.isEmpty() ? new PluginInfo() : getOnlyElement(list);
    }
}
