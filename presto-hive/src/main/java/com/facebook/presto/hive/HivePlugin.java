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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

public class HivePlugin
        implements Plugin
{
    private final String name;
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private HiveMetastore metastore;
    private TypeManager typeManager;

    public HivePlugin(String name)
    {
        this(name, null);
    }

    public HivePlugin(String name, HiveMetastore metastore)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.metastore = metastore;
    }

    @Inject
    public void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(new HiveConnectorFactory(name, optionalConfig, getClassLoader(), metastore, typeManager)));
        }
        return ImmutableList.of();
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = HivePlugin.class.getClassLoader();
        }
        return classLoader;
    }
}
