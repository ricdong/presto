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
package com.facebook.presto.server;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ParametricType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Injector;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.sonatype.aether.artifact.Artifact;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class PluginManager
{
    private static final List<String> HIDDEN_CLASSES = ImmutableList.<String>builder()
            .add("org.slf4j")
            .build();

    private static final ImmutableList<String> PARENT_FIRST_CLASSES = ImmutableList.<String>builder()
            .add("com.facebook.presto")
            .add("com.fasterxml.jackson")
            .add("io.airlift.slice")
            .add("javax.inject")
            .add("javax.annotation")
            .add("java.")
            .build();

    private static final Logger log = Logger.get(PluginManager.class);

    private final Injector injector;
    private final ConnectorManager connectorManager;
    private final Metadata metadata;
    private final BlockEncodingManager blockEncodingManager;
    private final TypeRegistry typeRegistry;
    private final ArtifactResolver resolver;
    private final File installedPluginsDir;
    private final List<String> plugins;
    private final Map<String, String> optionalConfig;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();

    @Inject
    public PluginManager(Injector injector,
            NodeInfo nodeInfo,
            HttpServerInfo httpServerInfo,
            PluginManagerConfig config,
            ConnectorManager connectorManager,
            ConfigurationFactory configurationFactory,
            Metadata metadata,
            BlockEncodingManager blockEncodingManager,
            TypeRegistry typeRegistry)
    {
        checkNotNull(injector, "injector is null");
        checkNotNull(nodeInfo, "nodeInfo is null");
        checkNotNull(httpServerInfo, "httpServerInfo is null");
        checkNotNull(config, "config is null");
        checkNotNull(configurationFactory, "configurationFactory is null");

        this.injector = injector;
        installedPluginsDir = config.getInstalledPluginsDir();
        if (config.getPlugins() == null) {
            this.plugins = ImmutableList.of();
        }
        else {
            this.plugins = ImmutableList.copyOf(config.getPlugins());
        }
        this.resolver = new ArtifactResolver(config.getMavenLocalRepository(), config.getMavenRemoteRepository());

        Map<String, String> optionalConfig = new TreeMap<>(configurationFactory.getProperties());
        optionalConfig.put("node.id", nodeInfo.getNodeId());
        // TODO: make this work with and without HTTP and HTTPS
        optionalConfig.put("http-server.http.port", Integer.toString(httpServerInfo.getHttpUri().getPort()));
        this.optionalConfig = ImmutableMap.copyOf(optionalConfig);

        this.connectorManager = checkNotNull(connectorManager, "connectorManager is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.blockEncodingManager = checkNotNull(blockEncodingManager, "blockEncodingManager is null");
        this.typeRegistry = checkNotNull(typeRegistry, "typeRegistry is null");
    }

    public boolean arePluginsLoaded()
    {
        return pluginsLoaded.get();
    }

    public void loadPlugins()
            throws Exception
    {
        if (!pluginsLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(installedPluginsDir)) {
            if (file.isDirectory()) {
                loadPlugin(file.getAbsolutePath());
            }
        }

        for (String plugin : plugins) {
            loadPlugin(plugin);
        }

        pluginsLoaded.set(true);
    }

    private void loadPlugin(String plugin)
            throws Exception
    {
        log.info("-- Loading plugin %s --", plugin);
        URLClassLoader pluginClassLoader = buildClassLoader(plugin);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader);
        }
        log.info("-- Finished loading plugin %s --", plugin);
    }

    private void loadPlugin(URLClassLoader pluginClassLoader)
            throws Exception
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);

        if (plugins.isEmpty()) {
            log.warn("No service providers of type %s", Plugin.class.getName());
        }

        for (Plugin plugin : plugins) {
            log.info("Installing %s", plugin.getClass().getName());
            installPlugin(plugin);
        }
    }

    public void installPlugin(Plugin plugin)
    {
        injector.injectMembers(plugin);

        plugin.setOptionalConfig(optionalConfig);

        for (BlockEncodingFactory<?> blockEncodingFactory : plugin.getServices(BlockEncodingFactory.class)) {
            log.info("Registering block encoding %s", blockEncodingFactory.getName());
            blockEncodingManager.addBlockEncodingFactory(blockEncodingFactory);
        }

        for (Type type : plugin.getServices(Type.class)) {
            log.info("Registering type %s", type.getTypeSignature());
            typeRegistry.addType(type);
        }

        for (ParametricType parametricType : plugin.getServices(ParametricType.class)) {
            log.info("Registering parametric type %s", parametricType.getName());
            typeRegistry.addParametricType(parametricType);
        }

        for (ConnectorFactory connectorFactory : plugin.getServices(ConnectorFactory.class)) {
            log.info("Registering connector %s", connectorFactory.getName());
            connectorManager.addConnectorFactory(connectorFactory);
        }

        for (FunctionFactory functionFactory : plugin.getServices(FunctionFactory.class)) {
            log.info("Registering functions from %s", functionFactory.getClass().getName());
            metadata.addFunctions(functionFactory.listFunctions());
        }
    }

    private URLClassLoader buildClassLoader(String plugin)
            throws Exception
    {
        File file = new File(plugin);
        if (file.isFile() && (file.getName().equals("pom.xml") || file.getName().endsWith(".pom"))) {
            return buildClassLoaderFromPom(file);
        }
        if (file.isDirectory()) {
            return buildClassLoaderFromDirectory(file);
        }
        return buildClassLoaderFromCoordinates(plugin);
    }

    private URLClassLoader buildClassLoaderFromPom(File pomFile)
            throws Exception
    {
        List<Artifact> artifacts = resolver.resolvePom(pomFile);
        return createClassLoader(artifacts, pomFile.getPath());
    }

    private URLClassLoader buildClassLoaderFromDirectory(File dir)
            throws Exception
    {
        log.debug("Classpath for %s:", dir.getName());
        List<URL> urls = new ArrayList<>();
        for (File file : listFiles(dir)) {
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader buildClassLoaderFromCoordinates(String coordinates)
            throws Exception
    {
        Artifact rootArtifact = new DefaultArtifact(coordinates);
        List<Artifact> artifacts = resolver.resolveArtifacts(rootArtifact);
        return createClassLoader(artifacts, rootArtifact.toString());
    }

    private URLClassLoader createClassLoader(List<Artifact> artifacts, String name)
            throws IOException
    {
        log.debug("Classpath for %s:", name);
        List<URL> urls = new ArrayList<>();
        for (Artifact artifact : sortedArtifacts(artifacts)) {
            if (artifact.getFile() == null) {
                throw new RuntimeException("Could not resolve artifact: " + artifact);
            }
            File file = artifact.getFile().getCanonicalFile();
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader createClassLoader(List<URL> urls)
    {
        ClassLoader parent = getClass().getClassLoader();
        return new PluginClassLoader(urls, parent, HIDDEN_CLASSES, PARENT_FIRST_CLASSES);
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                Arrays.sort(files);
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = Lists.newArrayList(artifacts);
        Collections.sort(list, Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }
}
