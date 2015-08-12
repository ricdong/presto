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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestServerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(ServerConfig.class)
                .setCoordinator(true)
                .setPrestoVersion(null)
                .setDataSources(null)
                .setIncludeExceptionInResponse(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("coordinator", "false")
                .put("presto.version", "test")
                .put("datasources", "jmx")
                .put("http.include-exception-in-response", "false")
                .build();

        ServerConfig expected = new ServerConfig()
                .setCoordinator(false)
                .setPrestoVersion("test")
                .setDataSources("jmx")
                .setIncludeExceptionInResponse(false);

        assertFullMapping(properties, expected);
    }
}
