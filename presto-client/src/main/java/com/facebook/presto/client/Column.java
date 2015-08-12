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
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class Column
{
    private final String name;
    private final String type;
    private final ClientTypeSignature typeSignature;

    @JsonCreator
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("typeSignature") ClientTypeSignature typeSignature)
    {
        this.name = checkNotNull(name, "name is null");
        this.type = checkNotNull(type, "type is null");
        this.typeSignature = typeSignature;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public ClientTypeSignature getTypeSignature()
    {
        return typeSignature;
    }
}
