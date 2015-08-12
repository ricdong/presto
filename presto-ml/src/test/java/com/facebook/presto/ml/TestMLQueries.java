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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.ParametricType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestMLQueries
        extends AbstractTestQueryFramework
{
    public TestMLQueries()
    {
        super(createLocalQueryRunner());
    }

    @Test
    public void testPrediction()
            throws Exception
    {
        assertQuery("SELECT classify(features(1, 2), model) " +
                "FROM (SELECT learn_classifier(labels, features) AS model FROM (VALUES (1, features(1, 2))) t(labels, features)) t2", "SELECT 1");
    }

    @Test
    public void testVarcharPrediction()
            throws Exception
    {
        assertQuery("SELECT classify(features(1, 2), model) " +
                "FROM (SELECT learn_classifier(labels, features) AS model FROM (VALUES ('cat', features(1, 2))) t(labels, features)) t2", "SELECT 'cat'");
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog(),
                new TpchConnectorFactory(localQueryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());

        MLPlugin plugin = new MLPlugin();
        plugin.setTypeManager(localQueryRunner.getTypeManager());
        for (Type type : plugin.getServices(Type.class)) {
            localQueryRunner.getTypeManager().addType(type);
        }
        for (ParametricType parametricType : plugin.getServices(ParametricType.class)) {
            localQueryRunner.getTypeManager().addParametricType(parametricType);
        }
        localQueryRunner.getMetadata().getFunctionRegistry().addFunctions(Iterables.getOnlyElement(plugin.getServices(FunctionFactory.class)).listFunctions());

        return localQueryRunner;
    }
}
