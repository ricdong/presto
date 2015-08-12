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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

public class TestDeterminismEvaluator
{
    @Test
    public void testSanity()
            throws Exception
    {
        Assert.assertFalse(DeterminismEvaluator.isDeterministic(function("rand")));
        Assert.assertFalse(DeterminismEvaluator.isDeterministic(function("random")));
        Assert.assertTrue(DeterminismEvaluator.isDeterministic(function("abs", input("symbol"))));
        Assert.assertFalse(DeterminismEvaluator.isDeterministic(function("abs", function("rand"))));
        Assert.assertTrue(DeterminismEvaluator.isDeterministic(function("abs", function("abs", input("symbol")))));
    }

    private static FunctionCall function(String name, Expression... inputs)
    {
        return new FunctionCall(new QualifiedName(name), Arrays.asList(inputs));
    }

    private static QualifiedNameReference input(String symbol)
    {
        return new QualifiedNameReference(QualifiedName.of(symbol));
    }
}
