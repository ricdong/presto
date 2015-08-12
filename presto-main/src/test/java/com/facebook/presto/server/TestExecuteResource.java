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
package com.facebook.presto.server;

import com.facebook.presto.client.PrestoHeaders;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.testing.Closeables;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.StringResponse;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestExecuteResource
{
    private TestingPrestoServer server;
    private HttpClient client;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        client = new JettyHttpClient();
    }

    @SuppressWarnings("deprecation")
    @AfterMethod
    public void teardown()
    {
        Closeables.closeQuietly(server);
        Closeables.closeQuietly(client);
    }

    @Test
    public void testExecute()
            throws Exception
    {
        String expected = "{\"columns\":[" +
                "{\"name\":\"foo\",\"type\":\"bigint\",\"typeSignature\":{\"rawType\":\"bigint\",\"typeArguments\":[],\"literalArguments\":[]}}," +
                "{\"name\":\"bar\",\"type\":\"varchar\",\"typeSignature\":{\"rawType\":\"varchar\",\"typeArguments\":[],\"literalArguments\":[]}}]," +
                "\"data\":[[123,\"abc\"]]}\n";

        StringResponse response = executeQuery("SELECT 123 foo, 'abc' bar");
        assertEquals(response.getStatusCode(), HttpStatus.OK.code());
        assertEquals(response.getHeader(HttpHeaders.CONTENT_TYPE), "application/json");
        assertEquals(response.getBody(), expected);
    }

    private StringResponse executeQuery(String query)
    {
        Request request = preparePost()
                .setUri(server.resolve("/v1/execute"))
                .setHeader(PrestoHeaders.PRESTO_USER, "test")
                .setHeader(PrestoHeaders.PRESTO_CATALOG, "catalog")
                .setHeader(PrestoHeaders.PRESTO_SCHEMA, "schema")
                .setHeader(PrestoHeaders.PRESTO_TIME_ZONE, "UTC")
                .setBodyGenerator(createStaticBodyGenerator(query, UTF_8))
                .build();
        return client.execute(request, createStringResponseHandler());
    }
}
