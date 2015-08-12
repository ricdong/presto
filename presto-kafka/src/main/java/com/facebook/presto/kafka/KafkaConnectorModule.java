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

import com.facebook.presto.kafka.decoder.KafkaDecoderModule;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import javax.inject.Inject;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

/**
 * Guice module for the Apache Kafka connector.
 */
public class KafkaConnectorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(KafkaConnector.class).in(Scopes.SINGLETON);

        binder.bind(KafkaHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(KafkaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KafkaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(KafkaRecordSetProvider.class).in(Scopes.SINGLETON);

        binder.bind(KafkaSimpleConsumerManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(KafkaConnectorConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(KafkaTopicDescription.class);

        binder.install(new KafkaDecoderModule());

        for (KafkaInternalFieldDescription internalFieldDescription : KafkaInternalFieldDescription.getInternalFields()) {
            bindInternalColumn(binder, internalFieldDescription);
        }
    }

    private static void bindInternalColumn(Binder binder, KafkaInternalFieldDescription fieldDescription)
    {
        Multibinder<KafkaInternalFieldDescription> fieldDescriptionBinder = Multibinder.newSetBinder(binder, KafkaInternalFieldDescription.class);
        fieldDescriptionBinder.addBinding().toInstance(fieldDescription);
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private static final long serialVersionUID = 1L;

        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = checkNotNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
