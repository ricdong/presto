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
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates Kafka Connectors based off connectorId and specific configuration.
 */
public class KafkaConnectorFactory
        implements ConnectorFactory
{
    private final TypeManager typeManager;
    private final NodeManager nodeManager;
    private final Optional<Supplier<Map<SchemaTableName, KafkaTopicDescription>>> tableDescriptionSupplier;
    private final Map<String, String> optionalConfig;

    KafkaConnectorFactory(TypeManager typeManager,
            NodeManager nodeManager,
            Optional<Supplier<Map<SchemaTableName, KafkaTopicDescription>>> tableDescriptionSupplier,
            Map<String, String> optionalConfig)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.optionalConfig = checkNotNull(optionalConfig, "optionalConfig is null");
        this.tableDescriptionSupplier = checkNotNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
    }

    @Override
    public String getName()
    {
        return "kafka";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new KafkaConnectorModule(),
                    binder -> {
                        binder.bind(KafkaConnectorId.class).toInstance(new KafkaConnectorId(connectorId));
                        binder.bind(TypeManager.class).toInstance(typeManager);
                        binder.bind(NodeManager.class).toInstance(nodeManager);

                        if (tableDescriptionSupplier.isPresent()) {
                            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KafkaTopicDescription>>>() {}).toInstance(tableDescriptionSupplier.get());
                        }
                        else {
                            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KafkaTopicDescription>>>() {}).to(KafkaTableDescriptionSupplier.class).in(Scopes.SINGLETON);
                        }
                    }
            );

            Injector injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(KafkaConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
