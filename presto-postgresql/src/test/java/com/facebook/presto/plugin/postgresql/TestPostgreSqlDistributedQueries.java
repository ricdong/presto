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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.tests.AbstractTestQueries;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestPostgreSqlDistributedQueries
        extends AbstractTestQueries
{
    private final TestingPostgreSqlServer postgreSqlServer;

    public TestPostgreSqlDistributedQueries()
            throws Exception
    {
        this(new TestingPostgreSqlServer("testuser", "tpch"));
    }

    public TestPostgreSqlDistributedQueries(TestingPostgreSqlServer postgreSqlServer)
            throws Exception
    {
        super(createPostgreSqlQueryRunner(postgreSqlServer, TpchTable.getTables()));
        this.postgreSqlServer = postgreSqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        closeAllRuntimeException(postgreSqlServer);
    }

    @Test
    public void testDropTable()
            throws Exception
    {
        assertQueryTrue("CREATE TABLE test_drop AS SELECT 123 x");
        assertTrue(queryRunner.tableExists(getSession(), "test_drop"));

        assertQueryTrue("DROP TABLE test_drop");
        assertFalse(queryRunner.tableExists(getSession(), "test_drop"));
    }
}
