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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import kafka.javaapi.consumer.SimpleConsumer;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Manages connections to the Kafka nodes. A worker may connect to multiple Kafka nodes depending on the segments and partitions
 * it needs to process. According to the Kafka source code, a Kafka {@link kafka.javaapi.consumer.SimpleConsumer} is thread-safe.
 */
public class KafkaSimpleConsumerManager
{
    private static final Logger log = Logger.get(KafkaSimpleConsumerManager.class);

    private final LoadingCache<HostAddress, SimpleConsumer> consumerCache;

    private final String connectorId;
    private final NodeManager nodeManager;
    private final int connectTimeoutMillis;
    private final int bufferSizeBytes;

    @Inject
    public KafkaSimpleConsumerManager(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            NodeManager nodeManager)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");

        checkNotNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.connectTimeoutMillis = Ints.checkedCast(kafkaConnectorConfig.getKafkaConnectTimeout().toMillis());
        this.bufferSizeBytes = Ints.checkedCast(kafkaConnectorConfig.getKafkaBufferSize().toBytes());

        this.consumerCache = CacheBuilder.newBuilder().build(new SimpleConsumerCacheLoader());
    }

    @PreDestroy
    public void tearDown()
    {
        for (Map.Entry<HostAddress, SimpleConsumer> entry : consumerCache.asMap().entrySet()) {
            try {
                entry.getValue().close();
            }
            catch (Exception e) {
                log.warn(e, "While closing consumer %s:", entry.getKey());
            }
        }
    }

    public SimpleConsumer getConsumer(HostAddress host)
    {
        checkNotNull(host, "host is null");
        try {
            return consumerCache.get(host);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private class SimpleConsumerCacheLoader
            extends CacheLoader<HostAddress, SimpleConsumer>
    {
        @Override
        public SimpleConsumer load(HostAddress host)
                throws Exception
        {
            log.info("Creating new Consumer for %s", host);
            return new SimpleConsumer(host.getHostText(),
                    host.getPort(),
                    connectTimeoutMillis,
                    bufferSizeBytes,
                    format("presto-kafka-%s-%s", connectorId, nodeManager.getCurrentNode().getNodeIdentifier()));
        }
    }
}
