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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.OperatorType;

public class MaxAggregation
        extends AbstractMinMaxAggregation
{
    private static final OperatorType OPERATOR_TYPE = OperatorType.GREATER_THAN;
    private static final String NAME = "max";

    public static final MaxAggregation MAX_AGGREGATION = new MaxAggregation();

    public MaxAggregation()
    {
        super(NAME, OPERATOR_TYPE);
    }

    @Override
    public String getDescription()
    {
        return "Returns the maximum value of the argument";
    }
}
