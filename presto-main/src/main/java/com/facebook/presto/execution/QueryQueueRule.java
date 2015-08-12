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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class QueryQueueRule
{
    @Nullable
    private final Pattern userRegex;
    @Nullable
    private final Pattern sourceRegex;
    private final Map<String, Pattern> sessionPropertyRegexes;
    private final List<QueryQueueDefinition> queues;

    QueryQueueRule(@Nullable Pattern userRegex, @Nullable Pattern sourceRegex, Map<String, Pattern> sessionPropertyRegexes, List<QueryQueueDefinition> queues)
    {
        this.userRegex = userRegex;
        this.sourceRegex = sourceRegex;
        this.sessionPropertyRegexes = ImmutableMap.copyOf(checkNotNull(sessionPropertyRegexes, "sessionPropertyRegexes is null"));
        checkNotNull(queues, "queues is null");
        checkArgument(!queues.isEmpty(), "queues is empty");
        this.queues = ImmutableList.copyOf(queues);
    }

    public static QueryQueueRule createRule(@Nullable Pattern userRegex, @Nullable Pattern sourceRegex, Map<String, Pattern> sessionPropertyRegexes, List<String> queueKeys, Map<String, QueryQueueDefinition> definedQueues)
    {
        ImmutableList.Builder<QueryQueueDefinition> queues = ImmutableList.builder();

        for (String key : queueKeys) {
            if (!definedQueues.containsKey(key)) {
                throw new IllegalArgumentException(format("Undefined queue %s. Defined queues are %s", key, definedQueues.keySet()));
            }
            queues.add(definedQueues.get(key));
        }

        return new QueryQueueRule(userRegex, sourceRegex, sessionPropertyRegexes, queues.build());
    }

    /**
     * Returns list of queues to enter, or null if query does not match rule
     */
    public List<QueryQueueDefinition> match(Session session)
    {
        if (userRegex != null && !userRegex.matcher(session.getUser()).matches()) {
            return null;
        }
        if (sourceRegex != null) {
            String source = session.getSource().orElse("");
            if (!sourceRegex.matcher(source).matches()) {
                return null;
            }
        }

        for (Map.Entry<String, Pattern> entry : sessionPropertyRegexes.entrySet()) {
            String value = session.getSystemProperties().getOrDefault(entry.getKey(), "");
            if (!entry.getValue().matcher(value).matches()) {
                return null;
            }
        }

        return queues;
    }

    List<QueryQueueDefinition> getQueues()
    {
        return queues;
    }
}
