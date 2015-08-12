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

import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.kafka.KafkaQueryRunner.createKafkaQueryRunner;
import static com.facebook.presto.kafka.util.EmbeddedKafka.createEmbeddedKafka;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

@Test
public class TestKafkaDistributed
        extends AbstractTestDistributedQueries
{
    private final EmbeddedKafka embeddedKafka;

    public TestKafkaDistributed()
            throws Exception
    {
        this(createEmbeddedKafka());
    }

    public TestKafkaDistributed(EmbeddedKafka embeddedKafka)
            throws Exception
    {
        super(createKafkaQueryRunner(embeddedKafka, TpchTable.getTables()));
        this.embeddedKafka = embeddedKafka;
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws IOException
    {
        closeAllRuntimeException(queryRunner, embeddedKafka);
    }

    //
    // Kafka connector does not support table creation.
    //

    @Override
    public void testCreateTable()
            throws Exception
    {
    }

    @Override
    public void testCreateTableAsSelect()
            throws Exception
    {
    }

    @Override
    public void testCreateTableAsSelectSampled()
            throws Exception
    {
    }

    @Override
    public void testSymbolAliasing()
            throws Exception
    {
    }

    //
    // Kafka connector does not support views.
    //

    @Override
    public void testView()
            throws Exception
    {
    }

    @Override
    public void testViewMetadata()
            throws Exception
    {
    }

    //
    // Kafka connector does not insert.
    //

    @Override
    public void testInsert()
            throws Exception
    {
    }

    //
    // Kafka connector does not delete.
    //

    @Override
    public void testDelete()
            throws Exception
    {
    }

    @Override
    public void testDeleteSemiJoin()
            throws Exception
    {
    }

    //
    // Kafka connector does not table rename.
    //

    @Override
    public void testRenameTable()
            throws Exception
    {
    }

    //
    // Kafka connector does not table column.
    //

    @Override
    public void testRenameColumn()
            throws Exception
    {
    }
}
