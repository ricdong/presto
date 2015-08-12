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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Groups the field descriptions for message or key.
 */
public class KafkaTopicFieldGroup
{
    private final String dataFormat;
    private final List<KafkaTopicFieldDescription> fields;

    @JsonCreator
    public KafkaTopicFieldGroup(
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("fields") List<KafkaTopicFieldDescription> fields)
    {
        this.dataFormat = checkNotNull(dataFormat, "dataFormat is null");
        this.fields = ImmutableList.copyOf(checkNotNull(fields, "fields is null"));
    }

    @JsonProperty
    public String getDataFormat()
    {
        return dataFormat;
    }

    @JsonProperty
    public List<KafkaTopicFieldDescription> getFields()
    {
        return fields;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataFormat", dataFormat)
                .add("fields", fields)
                .toString();
    }
}
