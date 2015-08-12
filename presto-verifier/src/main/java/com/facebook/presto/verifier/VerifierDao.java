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
package com.facebook.presto.verifier;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;

public interface VerifierDao
{
    @SqlQuery("" +
            "SELECT\n" +
            "  suite\n" +
            ", name\n" +
            ", test_catalog\n" +
            ", test_schema\n" +
            ", test_query\n" +
            ", test_username\n" +
            ", test_password\n" +
            ", control_catalog\n" +
            ", control_schema\n" +
            ", control_query\n" +
            ", control_username\n" +
            ", control_password\n" +
            ", session_properties_json\n" +
            "FROM verifier_queries\n" +
            "WHERE suite = :suite\n" +
            "ORDER BY id\n" +
            "LIMIT :limit")
    @Mapper(QueryPairMapper.class)
    List<QueryPair> getQueriesBySuite(@Bind("suite") String suite, @Bind("limit") int limit);
}
