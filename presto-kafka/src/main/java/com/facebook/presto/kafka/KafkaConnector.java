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

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Kafka specific implementation of the Presto Connector SPI. This is a read only connector.
 */
public class KafkaConnector
        implements Connector
{
    private static final Logger log = Logger.get(KafkaConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final KafkaMetadata metadata;
    private final KafkaSplitManager splitManager;
    private final KafkaRecordSetProvider recordSetProvider;
    private final KafkaHandleResolver handleResolver;

    @Inject
    public KafkaConnector(
            LifeCycleManager lifeCycleManager,
            KafkaHandleResolver handleResolver,
            KafkaMetadata metadata,
            KafkaSplitManager splitManager,
            KafkaRecordSetProvider recordSetProvider)
    {
        this.lifeCycleManager = checkNotNull(lifeCycleManager, "lifeCycleManager is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.recordSetProvider = checkNotNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return handleResolver;
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
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
