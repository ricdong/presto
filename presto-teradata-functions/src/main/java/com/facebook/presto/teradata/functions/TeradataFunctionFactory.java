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
package com.facebook.presto.teradata.functions;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;

public class TeradataFunctionFactory
        implements FunctionFactory
{
    private final TypeManager typeManager;

    public TeradataFunctionFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public List<ParametricFunction> listFunctions()
    {
        return new FunctionListBuilder(typeManager)
                .scalar(TeradataStringFunctions.class)
                .scalar(TeradataDateFunctions.class)
                .getFunctions();
    }
}
