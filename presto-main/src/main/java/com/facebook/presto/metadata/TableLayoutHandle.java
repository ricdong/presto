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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public final class TableLayoutHandle
{
    private final String connectorId;
    private final ConnectorTableLayoutHandle layout;

    @JsonCreator
    public TableLayoutHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("connectorHandle") ConnectorTableLayoutHandle layout)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(layout, "layout is null");

        this.connectorId = connectorId;
        this.layout = layout;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ConnectorTableLayoutHandle getConnectorHandle()
    {
        return layout;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableLayoutHandle that = (TableLayoutHandle) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(layout, that.layout);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, layout);
    }
}
