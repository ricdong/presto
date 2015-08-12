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
package com.facebook.presto.sql.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public final class Use
        extends Statement
{
    private final Optional<String> catalog;
    private final String schema;

    public Use(Optional<String> catalog, String schema)
    {
        checkNotNull(catalog, "catalog is null");
        checkNotNull(schema, "schema is null");
        this.catalog = catalog;
        this.schema = schema;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitUse(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, schema);
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

        Use use = (Use) o;

        if (!catalog.equals(use.catalog)) {
            return false;
        }
        if (!schema.equals(use.schema)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).toString();
    }
}
