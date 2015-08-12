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
package com.facebook.presto.hive.metastore;

import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class CachingHiveMetastoreStats
{
    private final HiveMetastoreApiStats getAllDatabases = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getDatabase = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getAllTables = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getAllViews = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getTable = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getPartitionNames = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getPartitionNamesPs = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getPartitionByName = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getPartitionsByNames = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats createTable = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats dropTable = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats renameTable = new HiveMetastoreApiStats();

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetAllDatabases()
    {
        return getAllDatabases;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetDatabase()
    {
        return getDatabase;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetAllTables()
    {
        return getAllTables;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getAllViews()
    {
        return getAllViews;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetTable()
    {
        return getTable;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetPartitionNames()
    {
        return getPartitionNames;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetPartitionNamesPs()
    {
        return getPartitionNamesPs;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetPartitionByName()
    {
        return getPartitionByName;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetPartitionsByNames()
    {
        return getPartitionsByNames;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getCreateTable()
    {
        return createTable;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getDropTable()
    {
        return dropTable;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getRenameTable()
    {
        return renameTable;
    }
}
