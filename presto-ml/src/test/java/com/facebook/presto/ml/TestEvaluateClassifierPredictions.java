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
package com.facebook.presto.ml;

import com.facebook.presto.RowPageBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.ml.type.ClassifierParametricType;
import com.facebook.presto.ml.type.ModelType;
import com.facebook.presto.ml.type.RegressorType;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestEvaluateClassifierPredictions
{
    private final Metadata metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testEvaluateClassifierPredictions()
            throws Exception
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        typeRegistry.addParametricType(new ClassifierParametricType());
        typeRegistry.addType(RegressorType.REGRESSOR);
        typeRegistry.addType(ModelType.MODEL);
        metadata.addFunctions(new MLFunctionFactory(typeRegistry).listFunctions());
        InternalAggregationFunction aggregation = metadata.getExactFunction(new Signature("evaluate_classifier_predictions", StandardTypes.VARCHAR, StandardTypes.BIGINT, StandardTypes.BIGINT)).getAggregationFunction();
        Accumulator accumulator = aggregation.bind(ImmutableList.of(0, 1), Optional.empty(), Optional.empty(), 1.0).createAccumulator();
        accumulator.addInput(getPage());
        BlockBuilder finalOut = accumulator.getFinalType().createBlockBuilder(new BlockBuilderStatus(), 1);
        accumulator.evaluateFinal(finalOut);
        Block block = finalOut.build();

        String output = VARCHAR.getSlice(block, 0).toStringUtf8();
        List<String> parts = ImmutableList.copyOf(Splitter.on('\n').omitEmptyStrings().split(output));
        assertEquals(parts.size(), 7, output);
        assertEquals(parts.get(0), "Accuracy: 1/2 (50.00%)");
    }

    private static Page getPage()
            throws JsonProcessingException
    {
        return RowPageBuilder.rowPageBuilder(BIGINT, BIGINT)
                .row(1, 1)
                .row(1, 0)
                .build();
    }
}
