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
package com.facebook.presto.benchmark.driver;

import com.facebook.presto.client.ClientSession;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class BenchmarkDriver
        implements Closeable
{
    private final ClientSession clientSession;
    private final List<BenchmarkQuery> queries;
    private final BenchmarkResultsStore resultsStore;
    private final BenchmarkQueryRunner queryRunner;

    public BenchmarkDriver(BenchmarkResultsStore resultsStore,
            ClientSession clientSession,
            Iterable<BenchmarkQuery> queries,
            int warm,
            int runs,
            boolean debug,
            int maxFailures,
            Optional<HostAndPort> socksProxy)
    {
        this.resultsStore = checkNotNull(resultsStore, "resultsStore is null");
        this.clientSession = checkNotNull(clientSession, "clientSession is null");
        this.queries = ImmutableList.copyOf(checkNotNull(queries, "queries is null"));

        queryRunner = new BenchmarkQueryRunner(warm, runs, debug, maxFailures, clientSession.getServer(), socksProxy);
    }

    public void run(Suite suite)
            throws Exception
    {
        // select queries to run
        List<BenchmarkQuery> queries = suite.selectQueries(this.queries);
        if (queries.isEmpty()) {
            return;
        }

        ClientSession session = ClientSession.withSessionProperties(clientSession, suite.getSessionProperties());

        // select schemas to use
        List<BenchmarkSchema> benchmarkSchemas;
        if (!suite.getSchemaNameTemplates().isEmpty()) {
            List<String> schemas = queryRunner.getSchemas(session);
            benchmarkSchemas = suite.selectSchemas(schemas);
        }
        else {
            benchmarkSchemas = ImmutableList.of(new BenchmarkSchema(session.getSchema()));
        }
        if (benchmarkSchemas.isEmpty()) {
            return;
        }

        for (BenchmarkSchema benchmarkSchema : benchmarkSchemas) {
            for (BenchmarkQuery benchmarkQuery : queries) {
                session = ClientSession.withCatalogAndSchema(session, session.getCatalog(), benchmarkSchema.getName());
                BenchmarkQueryResult result = queryRunner.execute(suite, session, benchmarkQuery);

                resultsStore.store(benchmarkSchema, result);
            }
        }
    }

    @Override
    public void close()
    {
        queryRunner.close();
    }
}
