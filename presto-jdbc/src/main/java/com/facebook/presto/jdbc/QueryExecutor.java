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
package com.facebook.presto.jdbc;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.jetty.JettyIoPool;
import io.airlift.http.client.jetty.JettyIoPoolConfig;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.json.JsonCodec.jsonCodec;

class QueryExecutor
        implements Closeable
{
    private final JsonCodec<QueryResults> queryInfoCodec;
    private final HttpClient httpClient;

    private QueryExecutor(String userAgent, JsonCodec<QueryResults> queryResultsCodec, HostAndPort socksProxy)
    {
        checkNotNull(userAgent, "userAgent is null");
        checkNotNull(queryResultsCodec, "queryResultsCodec is null");

        this.queryInfoCodec = queryResultsCodec;
        this.httpClient = new JettyHttpClient(
                new HttpClientConfig()
                        .setConnectTimeout(new Duration(10, TimeUnit.SECONDS))
                        .setSocksProxy(socksProxy),
                new JettyIoPool("presto-jdbc", new JettyIoPoolConfig()),
                ImmutableSet.of(new UserAgentRequestFilter(userAgent)));
    }

    public StatementClient startQuery(ClientSession session, String query)
    {
        return new StatementClient(httpClient, queryInfoCodec, session, query);
    }

    @Override
    public void close()
    {
        httpClient.close();
    }

    // TODO: replace this with a phantom reference
    @SuppressWarnings("FinalizeDeclaration")
    @Override
    protected void finalize()
    {
        close();
    }

    static QueryExecutor create(String userAgent)
    {
        return new QueryExecutor(userAgent, jsonCodec(QueryResults.class), getSystemSocksProxy());
    }

    @Nullable
    private static HostAndPort getSystemSocksProxy()
    {
        URI uri = URI.create("socket://0.0.0.0:80");
        for (Proxy proxy : ProxySelector.getDefault().select(uri)) {
            if (proxy.type() == Proxy.Type.SOCKS) {
                if (proxy.address() instanceof InetSocketAddress) {
                    InetSocketAddress address = (InetSocketAddress) proxy.address();
                    return HostAndPort.fromParts(address.getHostString(), address.getPort());
                }
            }
        }
        return null;
    }
}
