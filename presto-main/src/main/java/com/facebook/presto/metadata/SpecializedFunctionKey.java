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

import com.facebook.presto.spi.type.Type;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class SpecializedFunctionKey
{
    private final ParametricFunction function;
    private final Map<String, Type> boundTypeParameters;
    private final int arity;

    public SpecializedFunctionKey(ParametricFunction function, Map<String, Type> boundTypeParameters, int arity)
    {
        this.function = checkNotNull(function, "function is null");
        this.boundTypeParameters = checkNotNull(boundTypeParameters, "boundTypeParameters is null");
        this.arity = arity;
    }

    public ParametricFunction getFunction()
    {
        return function;
    }

    public Map<String, Type> getBoundTypeParameters()
    {
        return boundTypeParameters;
    }

    public int getArity()
    {
        return arity;
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

        SpecializedFunctionKey that = (SpecializedFunctionKey) o;

        return Objects.equals(arity, that.arity) &&
                Objects.equals(boundTypeParameters, that.boundTypeParameters) &&
                Objects.equals(function.getSignature(), that.function.getSignature());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function.getSignature(), boundTypeParameters, arity);
    }
}
