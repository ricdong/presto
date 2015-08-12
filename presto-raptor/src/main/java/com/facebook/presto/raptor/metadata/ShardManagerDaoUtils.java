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
package com.facebook.presto.raptor.metadata;

import com.google.common.base.Throwables;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.util.concurrent.TimeUnit;

public final class ShardManagerDaoUtils
{
    private static final Logger log = Logger.get(ShardManagerDaoUtils.class);

    private ShardManagerDaoUtils() {}

    public static void createShardTablesWithRetry(ShardManagerDao dao)
    {
        Duration delay = new Duration(10, TimeUnit.SECONDS);
        while (true) {
            try {
                createShardTables(dao);
                return;
            }
            catch (UnableToObtainConnectionException e) {
                log.warn("Failed to connect to database. Will retry again in %s. Exception: %s", delay, e.getMessage());
                sleep(delay);
            }
        }
    }

    private static void createShardTables(ShardManagerDao dao)
    {
        dao.createTableNodes();
        dao.createTableShards();
        dao.createTableShardNodes();
        dao.createTableExternalBatches();
    }

    private static void sleep(Duration duration)
    {
        try {
            Thread.sleep(duration.toMillis());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }
}
