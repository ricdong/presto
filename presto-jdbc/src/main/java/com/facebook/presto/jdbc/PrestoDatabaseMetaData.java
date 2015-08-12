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

import com.google.common.base.Joiner;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class PrestoDatabaseMetaData
        implements DatabaseMetaData
{
    private final PrestoConnection connection;

    PrestoDatabaseMetaData(PrestoConnection connection)
    {
        this.connection = checkNotNull(connection, "connection is null");
    }

    @Override
    public boolean allProceduresAreCallable()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getURL()
            throws SQLException
    {
        return connection.getURI().toString();
    }

    @Override
    public String getUserName()
            throws SQLException
    {
        return connection.getUser();
    }

    @Override
    public boolean isReadOnly()
            throws SQLException
    {
        return connection.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh()
            throws SQLException
    {
        // TODO: determine null sort order
        throw new NotImplementedException("DatabaseMetaData", "nullsAreSortedHigh");
    }

    @Override
    public boolean nullsAreSortedLow()
            throws SQLException
    {
        // TODO: determine null sort order
        throw new NotImplementedException("DatabaseMetaData", "nullsAreSortedLow");
    }

    @Override
    public boolean nullsAreSortedAtStart()
            throws SQLException
    {
        // TODO: determine null sort order
        throw new NotImplementedException("DatabaseMetaData", "nullsAreSortedAtStart");
    }

    @Override
    public boolean nullsAreSortedAtEnd()
            throws SQLException
    {
        // TODO: determine null sort order
        throw new NotImplementedException("DatabaseMetaData", "nullsAreSortedAtEnd");
    }

    @Override
    public String getDatabaseProductName()
            throws SQLException
    {
        return "Presto";
    }

    @Override
    public String getDatabaseProductVersion()
            throws SQLException
    {
        // TODO: get version from server
        return "UNKNOWN";
    }

    @Override
    public String getDriverName()
            throws SQLException
    {
        return PrestoDriver.DRIVER_NAME;
    }

    @Override
    public String getDriverVersion()
            throws SQLException
    {
        return PrestoDriver.DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion()
    {
        return PrestoDriver.VERSION_MAJOR;
    }

    @Override
    public int getDriverMinorVersion()
    {
        return PrestoDriver.VERSION_MINOR;
    }

    @Override
    public boolean usesLocalFiles()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getIdentifierQuoteString()
            throws SQLException
    {
        return "\"";
    }

    @Override
    public String getSQLKeywords()
            throws SQLException
    {
        return "LIMIT";
    }

    @Override
    public String getNumericFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getStringFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getSystemFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getTimeDateFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getSearchStringEscape()
            throws SQLException
    {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters()
            throws SQLException
    {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsConvert()
            throws SQLException
    {
        // TODO: support convert
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType)
            throws SQLException
    {
        // TODO: support convert
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames()
            throws SQLException
    {
        // TODO: verify this
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated()
            throws SQLException
    {
        // TODO: verify this
        return true;
    }

    @Override
    public boolean supportsGroupBy()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL()
            throws SQLException
    {
        // TODO: verify this
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOuterJoins()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins()
            throws SQLException
    {
        // TODO: support full outer joins
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getSchemaTerm()
            throws SQLException
    {
        return "schema";
    }

    @Override
    public String getProcedureTerm()
            throws SQLException
    {
        return "procedure";
    }

    @Override
    public String getCatalogTerm()
            throws SQLException
    {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getCatalogSeparator()
            throws SQLException
    {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures()
            throws SQLException
    {
        // TODO: support stored procedures
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons()
            throws SQLException
    {
        // TODO: support subqueries in comparisons
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists()
            throws SQLException
    {
        // TODO: support EXISTS
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns()
            throws SQLException
    {
        // TODO: support subqueries in IN clauses
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds()
            throws SQLException
    {
        // TODO: support subqueries in ANY/SOME/ALL predicates
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries()
            throws SQLException
    {
        // TODO: support correlated subqueries
        return false;
    }

    @Override
    public boolean supportsUnion()
            throws SQLException
    {
        // TODO: support UNION
        return false;
    }

    @Override
    public boolean supportsUnionAll()
            throws SQLException
    {
        // TODO: support UNION ALL
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback()
            throws SQLException
    {
        return true;
    }

    @Override
    public int getMaxBinaryLiteralLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxConnections()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxIndexLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxRowSize()
            throws SQLException
    {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs()
            throws SQLException
    {
        return true;
    }

    @Override
    public int getMaxStatementLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxStatements()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxTableNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxTablesInSelect()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxUserNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation()
            throws SQLException
    {
        // TODO: support transactions
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions()
            throws SQLException
    {
        // TODO: support transactions
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level)
            throws SQLException
    {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions()
            throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException
    {
        // TODO: support stored procedures
        throw new SQLFeatureNotSupportedException("stored procedures not supported");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
            throws SQLException
    {
        // TODO: support stored procedures
        throw new SQLFeatureNotSupportedException("stored procedures not supported");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException
    {
        StringBuilder query = new StringBuilder(1024);
        query.append("SELECT");
        query.append(" table_catalog AS TABLE_CAT");
        query.append(", table_schema AS TABLE_SCHEM");
        query.append(", table_name AS TABLE_NAME");
        query.append(", table_type AS TABLE_TYPE");
        query.append(", '' AS REMARKS");
        query.append(", '' AS TYPE_CAT");
        query.append(", '' AS TYPE_SCHEM");
        query.append(", '' AS TYPE_NAME");
        query.append(", '' AS SELF_REFERENCING_COL_NAME");
        query.append(", '' AS REF_GENERATION");
        query.append(" FROM information_schema.tables ");

        List<String> filters = new ArrayList<>(4);
        if (catalog != null) {
            if (catalog.isEmpty()) {
                filters.add("table_catalog IS NULL");
            }
            else {
                filters.add(stringColumnEquals("table_catalog", catalog));
            }
        }

        if (schemaPattern != null) {
            if (schemaPattern.isEmpty()) {
                filters.add("table_schema IS NULL");
            }
            else {
                filters.add(stringColumnLike("table_schema", schemaPattern));
            }
        }

        if (tableNamePattern != null) {
            filters.add(stringColumnLike("table_name", tableNamePattern));
        }

        if (types != null && types.length > 0) {
            StringBuilder filter = new StringBuilder();
            filter.append("table_type in (");

            for (int i = 0; i < types.length; i++) {
                String type = types[i];

                if (i > 0) {
                    filter.append(" ,");
                }

                quoteStringLiteral(filter, type);
            }
            filter.append(")");
            filters.add(filter.toString());
        }

        if (!filters.isEmpty()) {
            query.append(" WHERE ");
            Joiner.on(" AND ").appendTo(query, filters);
        }

        query.append(" ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME");

        return select(query.toString());
    }

    @Override
    public ResultSet getSchemas()
            throws SQLException
    {
        return select("" +
                "SELECT schema_name AS TABLE_SCHEM, catalog_name TABLE_CATALOG " +
                "FROM information_schema.schemata " +
                "ORDER BY TABLE_CATALOG, TABLE_SCHEM");
    }

    @Override
    public ResultSet getCatalogs()
            throws SQLException
    {
        return select("" +
                "SELECT DISTINCT catalog_name AS TABLE_CAT " +
                "FROM information_schema.schemata " +
                "ORDER BY TABLE_CAT");
    }

    @Override
    public ResultSet getTableTypes()
            throws SQLException
    {
        return select("" +
                "SELECT DISTINCT table_type AS TABLE_TYPE " +
                "FROM information_schema.tables " +
                "ORDER BY TABLE_TYPE");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException
    {
        StringBuilder query = new StringBuilder("" +
                "SELECT " +
                "  table_catalog TABLE_CAT " +
                ", table_schema TABLE_SCHEM " +
                ", table_name TABLE_NAME " +
                ", column_name COLUMN_NAME " +
                ", CASE data_type " +
                "    WHEN 'bigint' THEN " + Types.BIGINT + " " +
                "    WHEN 'double' THEN " + Types.DOUBLE + " " +
                "    WHEN 'varchar' THEN " + Types.LONGNVARCHAR + " " +
                "    WHEN 'boolean' THEN " + Types.BOOLEAN + " " +
                "    ELSE " + Types.OTHER + " " +
                "  END DATA_TYPE " +
                ", data_type TYPE_NAME " +
                ", 0 COLUMN_SIZE " +
                ", 0 BUFFER_LENGTH " +
                ", CASE data_type " +
                "    WHEN 'bigint' THEN 0 " +
                "  END DECIMAL_DIGITS " +
                ", CASE data_type " +
                "    WHEN 'bigint' THEN 10 " +
                "    WHEN 'double' THEN 10 " +
                "    ELSE 0 " +
                "  END AS NUM_PREC_RADIX " +
                ", CASE is_nullable " +
                "    WHEN 'NO' THEN " + columnNoNulls + " " +
                "    WHEN 'YES' THEN 1" + columnNullable + " " +
                "    ELSE 2" + columnNullableUnknown + " " +
                "  END NULLABLE " +
                ", CAST(NULL AS varchar) REMARKS " +
                ", column_default AS COLUMN_DEF " +
                ", CAST(NULL AS bigint) AS SQL_DATA_TYPE " +
                ", CAST(NULL AS bigint) AS SQL_DATETIME_SUB " +
                ", 0 AS CHAR_OCTET_LENGTH " +
                ", ordinal_position ORDINAL_POSITION " +
                ", is_nullable IS_NULLABLE " +
                ", CAST(NULL AS varchar) SCOPE_CATALOG " +
                ", CAST(NULL AS varchar) SCOPE_SCHEMA " +
                ", CAST(NULL AS varchar) SCOPE_TABLE " +
                ", CAST(NULL AS bigint) SOURCE_DATA_TYPE " +
                ", '' IS_AUTOINCREMENT " +
                ", '' IS_GENERATEDCOLUMN " +
                "FROM information_schema.columns ");

        List<String> filters = new ArrayList<>(4);
        if (catalog != null) {
            if (catalog.isEmpty()) {
                filters.add("table_catalog IS NULL");
            }
            else {
                filters.add(stringColumnEquals("table_catalog", catalog));
            }
        }

        if (schemaPattern != null) {
            if (schemaPattern.isEmpty()) {
                filters.add("table_schema IS NULL");
            }
            else {
                filters.add(stringColumnLike("table_schema", schemaPattern));
            }
        }

        if (tableNamePattern != null) {
            filters.add(stringColumnLike("table_name", tableNamePattern));
        }

        if (columnNamePattern != null) {
            filters.add(stringColumnLike("column_name", columnNamePattern));
        }

        if (!filters.isEmpty()) {
            query.append(" WHERE ");
            Joiner.on(" AND ").appendTo(query, filters);
        }

        query.append(" ORDER BY table_cat, table_schem, table_name, ordinal_position");

        return select(query.toString());
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("privileges not supported");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("privileges not supported");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("row identifiers not supported");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("version columns not supported");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("primary keys not supported");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("imported keys not supported");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("exported keys not supported");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("cross reference not supported");
    }

    @Override
    public ResultSet getTypeInfo()
            throws SQLException
    {
        // TODO: implement this
        throw new NotImplementedException("DatabaseMetaData", "getTypeInfo");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("indexes not supported");
    }

    @Override
    public boolean supportsResultSetType(int type)
            throws SQLException
    {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency)
            throws SQLException
    {
        return (type == ResultSet.TYPE_FORWARD_ONLY) &&
                (concurrency == ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates()
            throws SQLException
    {
        // TODO: support batch updates
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("user-defined types not supported");
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        return connection;
    }

    @Override
    public boolean supportsSavepoints()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsNamedParameters()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys()
            throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("type hierarchies not supported");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("type hierarchies not supported");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("user-defined types not supported");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability)
            throws SQLException
    {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability()
            throws SQLException
    {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion()
            throws SQLException
    {
        // TODO: get version from server
        return PrestoDriver.VERSION_MAJOR;
    }

    @Override
    public int getDatabaseMinorVersion()
            throws SQLException
    {
        return PrestoDriver.VERSION_MINOR;
    }

    @Override
    public int getJDBCMajorVersion()
            throws SQLException
    {
        return PrestoDriver.JDBC_VERSION_MAJOR;
    }

    @Override
    public int getJDBCMinorVersion()
            throws SQLException
    {
        return PrestoDriver.JDBC_VERSION_MINOR;
    }

    @Override
    public int getSQLStateType()
            throws SQLException
    {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsStatementPooling()
            throws SQLException
    {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime()
            throws SQLException
    {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern)
            throws SQLException
    {
        // The schema columns are:
        // TABLE_SCHEM String => schema name
        // TABLE_CATALOG String => catalog name (may be null)
        StringBuilder query = new StringBuilder(512);
        query.append("SELECT DISTINCT schema_name TABLE_SCHEM, catalog_name TABLE_CATALOG ");
        query.append(" FROM information_schema.schemata");

        List<String> filters = new ArrayList<>(4);
        if (catalog != null) {
            if (catalog.isEmpty()) {
                filters.add("catalog_name IS NULL");
            }
            else {
                filters.add(stringColumnEquals("catalog_name", catalog));
            }
        }

        if (schemaPattern != null) {
            filters.add(stringColumnLike("schema_name", schemaPattern));
        }

        if (!filters.isEmpty()) {
            query.append(" WHERE ");
            Joiner.on(" AND ").appendTo(query, filters);
        }

        query.append(" ORDER BY TABLE_CATALOG, TABLE_SCHEM");

        return select(query.toString());
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets()
            throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties()
            throws SQLException
    {
        // TODO: implement this
        throw new NotImplementedException("DatabaseMetaData", "getClientInfoProperties");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException
    {
        // TODO: implement this
        throw new NotImplementedException("DatabaseMetaData", "getFunctions");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
            throws SQLException
    {
        // TODO: implement this
        throw new NotImplementedException("DatabaseMetaData", "getFunctionColumns");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException
    {
        // TODO: implement this
        throw new NotImplementedException("DatabaseMetaData", "getPseudoColumns");
    }

    @Override
    public boolean generatedKeyAlwaysReturned()
            throws SQLException
    {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return iface.isInstance(this);
    }

    private ResultSet select(String sql)
            throws SQLException
    {
        try (Statement statement = getConnection().createStatement()) {
            return statement.executeQuery(sql);
        }
    }

    private static String stringColumnEquals(String columnName, String value)
    {
        StringBuilder filter = new StringBuilder();
        filter.append(columnName).append(" = ");
        quoteStringLiteral(filter, value);
        return filter.toString();
    }

    private static String stringColumnLike(String columnName, String pattern)
    {
        StringBuilder filter = new StringBuilder();
        filter.append(columnName).append(" LIKE ");
        quoteStringLiteral(filter, pattern);
        return filter.toString();
    }

    private static void quoteStringLiteral(StringBuilder out, String value)
    {
        out.append('\'');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            out.append(c);
            if (c == '\'') {
                out.append('\'');
            }
        }
        out.append('\'');
    }
}
