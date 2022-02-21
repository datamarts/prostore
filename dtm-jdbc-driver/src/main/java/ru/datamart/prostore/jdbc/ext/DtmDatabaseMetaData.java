/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.datamart.prostore.jdbc.ext;

import lombok.extern.slf4j.Slf4j;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.jdbc.core.BaseConnection;
import ru.datamart.prostore.jdbc.core.Field;
import ru.datamart.prostore.jdbc.core.FieldMetadata;
import ru.datamart.prostore.jdbc.core.Tuple;
import ru.datamart.prostore.jdbc.model.ColumnInfo;
import ru.datamart.prostore.jdbc.model.SchemaInfo;
import ru.datamart.prostore.jdbc.model.TableInfo;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.http.util.TextUtils.isEmpty;
import static ru.datamart.prostore.jdbc.util.DriverConstants.*;
import static ru.datamart.prostore.jdbc.util.DriverInfo.*;

@Slf4j
public class DtmDatabaseMetaData implements DatabaseMetaData {

    private final BaseConnection connection;
    private ResultSet catalogs;

    public DtmDatabaseMetaData(BaseConnection dtmConnection) {
        this.connection = dtmConnection;
    }

    private List<String> getCatalogNames(ResultSet catalogs) throws SQLException {
        List<String> names = new ArrayList<>();
        while (catalogs.next()) {
            names.add(catalogs.getString(CATALOG_NAME_COLUMN));
        }
        return names;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        Field[] fields = new Field[]{
                new Field(SCHEMA_NAME_COLUMN, ColumnType.VARCHAR),
                new Field(CATALOG_NAME_COLUMN, ColumnType.VARCHAR)
        };
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            List<Tuple> tuples = Collections.singletonList(new Tuple(new Object[]{"", ""}));
            return dtmStatement.createDriverResultSet(fields, tuples);
        }
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        if (catalogs == null) {
            final List<SchemaInfo> schemas = connection.getQueryExecutor().getSchemas();
            final List<Tuple> tuples = new ArrayList<>();
            final Field[] fields = new Field[]{new Field(CATALOG_NAME_COLUMN, ColumnType.VARCHAR)};
            schemas.forEach(schemaInfo -> {
                String schemaName = schemaInfo.getMnemonic();
                tuples.add(new Tuple(new Object[]{schemaName}));
            });
            try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
                this.catalogs = dtmStatement.createDriverResultSet(fields, tuples);
            }
        }
        return catalogs;
    }

    @Override
    public ResultSet getTables(String catalog,
                               String schemaPattern,
                               String tableNamePattern,
                               String[] types) throws SQLException {
        List<String> catalogNames;
        if (isEmpty(catalog)) {
            catalogNames = getCatalogNames(getCatalogs());
        } else {
            catalogNames = Collections.singletonList(catalog);
        }
        final List<TableInfo> databaseTables = this.connection.getQueryExecutor().getTables(catalog);
        final Field[] fields = createTablesFields(catalog);
        List<Tuple> tuples = catalogNames.stream()
                .flatMap(schemasName -> databaseTables.stream())
                .map(tableInfo -> new Tuple(new Object[]{
                        tableInfo.getDatamartMnemonic(),
                        "",
                        tableInfo.getMnemonic(),
                        TABLE_TYPE,
                        "",
                        null,
                        null
                })).collect(Collectors.toList());
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, tuples);
        }
    }

    private Field[] createTablesFields(String schemaName) {
        return new Field[]{
                new Field(CATALOG_NAME_COLUMN, ColumnType.VARCHAR, new FieldMetadata(CATALOG_NAME_COLUMN, schemaName)),
                new Field(SCHEMA_NAME_COLUMN, ColumnType.VARCHAR, new FieldMetadata(SCHEMA_NAME_COLUMN, schemaName)),
                new Field(TABLE_NAME_COLUMN, ColumnType.VARCHAR, new FieldMetadata(TABLE_NAME_COLUMN, schemaName)),
                new Field(TABLE_TYPE_COLUMN, ColumnType.VARCHAR, new FieldMetadata(TABLE_TYPE_COLUMN, schemaName)),
                //3 field
                new Field(REMARKS_COLUMN, ColumnType.VARCHAR, new FieldMetadata(REMARKS_COLUMN, schemaName)),
                new Field(SELF_REFERENCING_COL_NAME_COLUMN, ColumnType.VARCHAR, new FieldMetadata(SELF_REFERENCING_COL_NAME_COLUMN, schemaName)),
                new Field(REF_GENERATION_COLUMN, ColumnType.VARCHAR, new FieldMetadata(REF_GENERATION_COLUMN, schemaName)),
                //new Field(TABLE_OWNER_COLUMN, ColumnType.VARCHAR, new FieldMetadata(TABLE_OWNER_COLUMN, schemaName))
        };
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        Field[] fields = new Field[]{
                new Field(TABLE_NAME_COLUMN, ColumnType.VARCHAR)
        };
        List<Tuple> tuples = new ArrayList<>();
        tuples.add(new Tuple(new Object[]{TABLE_TYPE}));
        tuples.add(new Tuple(new Object[]{SYSTEM_VIEW_TYPE}));
        tuples.add(new Tuple(new Object[]{VIEW_TYPE}));
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, tuples);
        }
    }

    @Override
    public ResultSet getColumns(String catalog,
                                String schemaPattern,
                                String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();
        tableNamePattern = tableNamePattern.replace("\\", "");
        log.info("Table name pattern: {}", tableNamePattern);
        if (tableNamePattern.indexOf('%') != -1) {
            ResultSet tables = this.getTables(catalog, schemaPattern, null, null);
            while (tables.next()) {
                final List<ColumnInfo> databaseColumns = this.connection.getQueryExecutor().getTableColumns(
                        tables.getString(CATALOG_NAME_COLUMN),
                        tables.getString(TABLE_NAME_COLUMN));
                log.info("Table name: {}", tables.getString(TABLE_NAME_COLUMN));
                columns.addAll(databaseColumns);
            }
        } else {
            columns = this.connection.getQueryExecutor().getTableColumns(catalog, tableNamePattern);
        }
        List<Tuple> tuples = columns.stream()
                .map(columnInfo -> new Tuple(new Object[]{
                        columnInfo.getDatamartMnemonic(),
                        null,
                        columnInfo.getEntityMnemonic(),
                        columnInfo.getMnemonic(),
                        connection.getTypeInfo().getSqlType(columnInfo.getDataType()),
                        connection.getTypeInfo().getAlias(columnInfo.getDataType()).toUpperCase(),
                        getColumnSize(columnInfo),
                        null,
                        getColumnScale(columnInfo),
                        null,
                        isNullable(columnInfo) ? 1 : 0,
                        null,
                        null,
                        null,
                        null,
                        getColumnSize(columnInfo),
                        columnInfo.getOrdinalPosition() + 1,
                        isNullable(columnInfo) ? "YES" : "NO",
                        null,
                        null,
                        null,
                        connection.getTypeInfo().getSqlType(columnInfo.getDataType()),
                        "NO",
                        "NO"
                })).collect(Collectors.toList());
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(createColumnFields(), tuples);
        }
    }

    private Integer getColumnSize(ColumnInfo columnInfo) {
        switch (columnInfo.getDataType()) {
            case TIME:
            case TIMESTAMP:
                return columnInfo.getAccuracy();
            default:
                return columnInfo.getLength() == null ? -1 : columnInfo.getLength();
        }
    }

    private Integer getColumnScale(ColumnInfo columnInfo) {
        switch (columnInfo.getDataType()) {
            case TIME:
            case TIMESTAMP:
                return 0;
            default:
                return columnInfo.getAccuracy() == null ? 0 : columnInfo.getAccuracy();
        }
    }

    private Field[] createColumnFields() {
        return new Field[]{
                new Field(CATALOG_NAME_COLUMN, ColumnType.VARCHAR),
                new Field(SCHEMA_NAME_COLUMN, ColumnType.VARCHAR),
                new Field(TABLE_NAME_COLUMN, ColumnType.VARCHAR),
                new Field(COLUMN_NAME_COLUMN, ColumnType.VARCHAR),
                new Field(DATA_TYPE_COLUMN, ColumnType.INT),
                new Field(TYPE_NAME_COLUMN, ColumnType.VARCHAR),
                new Field(COLUMN_SIZE_COLUMN, ColumnType.INT),
                new Field(BUFFER_LENGTH_COLUMN, ColumnType.VARCHAR),
                new Field(DECIMAL_DIGITS_COLUMN, ColumnType.INT),
                new Field(NUM_PREC_RADIX_COLUMN, ColumnType.INT),
                new Field(NULLABLE_COLUMN, ColumnType.INT),
                new Field(REMARKS_COLUMN, ColumnType.VARCHAR),
                new Field(COLUMN_DEF_COLUMN, ColumnType.VARCHAR),
                new Field(SQL_DATA_TYPE_COLUMN, ColumnType.INT),
                new Field(SQL_DATETIME_SUB_COLUMN, ColumnType.INT),
                new Field(CHAR_OCTET_LENGTH_COLUMN, ColumnType.INT),
                new Field(ORDINAL_POSITION_COLUMN, ColumnType.INT),
                new Field(IS_NULLABLE_COLUMN, ColumnType.VARCHAR),
                new Field(SCOPE_CATALOG_COLUMN, ColumnType.VARCHAR),
                new Field(SCOPE_SCHEMA_COLUMN, ColumnType.VARCHAR),
                new Field(SCOPE_TABLE_COLUMN, ColumnType.VARCHAR),
                new Field(SOURCE_DATA_TYPE_COLUMN, ColumnType.INT),
                new Field(IS_AUTOINCREMENT_COLUMN, ColumnType.VARCHAR),
                new Field(IS_GENERATEDCOLUMN_COLUMN, ColumnType.VARCHAR)
        };
    }

    private boolean isNullable(ColumnInfo columnInfo) {
        return columnInfo.getNullable() == null || columnInfo.getNullable();
    }

    @Override
    public String getDatabaseProductName() {
        return DATABASE_PRODUCT_NAME;
    }

    @Override
    public String getDatabaseProductVersion() {
        return this.connection.getDBVersionNumber();
    }

    @Override
    public String getURL() {
        return this.connection.getUrl();
    }

    @Override
    public String getUserName() {
        return this.connection.getUserName();
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDriverName() throws SQLException {
        return DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_REPEATABLE_READ;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog,
                                   String schemaPattern,
                                   String procedureNamePattern) throws SQLException {
        Field[] fields = {
                new Field("PROCEDURE_CAT", ColumnType.VARCHAR),
                new Field("PROCEDURE_SCHEM", ColumnType.VARCHAR),
                new Field("PROCEDURE_NAME", ColumnType.VARCHAR),
                new Field("", ColumnType.VARCHAR),
                new Field("", ColumnType.VARCHAR),
                new Field("", ColumnType.VARCHAR),
                new Field(REMARKS_COLUMN, ColumnType.VARCHAR),
                new Field("PROCEDURE_TYPE", ColumnType.INT),
                new Field(SPECIFIC_NAME_COLUMN, ColumnType.VARCHAR)
        };
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    @Override
    public ResultSet getProcedureColumns(String catalog,
                                         String schemaPattern,
                                         String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        Field[] fields = {
                new Field("PROCEDURE_CAT", ColumnType.VARCHAR),
                new Field("PROCEDURE_SCHEM", ColumnType.VARCHAR),
                new Field("PROCEDURE_NAME", ColumnType.VARCHAR),
                new Field(COLUMN_NAME_COLUMN, ColumnType.VARCHAR),
                new Field("COLUMN_TYPE", ColumnType.INT),
                new Field(DATA_TYPE_COLUMN, ColumnType.INT),
                new Field(TYPE_NAME_COLUMN, ColumnType.VARCHAR),
                new Field(PRECISION_COLUMN, ColumnType.INT),
                new Field("LENGTH", ColumnType.INT),
                new Field("SCALE", ColumnType.INT),
                new Field("RADIX", ColumnType.INT),
                new Field(NULLABLE_COLUMN, ColumnType.INT),
                new Field(REMARKS_COLUMN, ColumnType.VARCHAR),
                new Field("COLUMN_DEF", ColumnType.VARCHAR),
                new Field("SQL_DATA_TYPE", ColumnType.INT),
                new Field("SQL_DATETIME_SUB", ColumnType.INT),
                new Field(CHAR_OCTET_LENGTH_COLUMN, ColumnType.INT),
                new Field(ORDINAL_POSITION_COLUMN, ColumnType.INT),
                new Field(IS_NULLABLE_COLUMN, ColumnType.VARCHAR),
                new Field(SPECIFIC_NAME_COLUMN, ColumnType.VARCHAR)
        };
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog,
                                         String schema,
                                         String table,
                                         String columnNamePattern) throws SQLException {
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(getPrivilegesMetadata(), new ArrayList<>());
        }
    }

    @Override
    public ResultSet getTablePrivileges(String catalog,
                                        String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(getPrivilegesMetadata(), new ArrayList<>());
        }
    }

    private Field[] getPrivilegesMetadata() {
        return new Field[]{
                new Field("TABLE_CAT", ColumnType.VARCHAR),
                new Field("TABLE_SCHEM", ColumnType.VARCHAR),
                new Field(TABLE_NAME_COLUMN, ColumnType.VARCHAR),
                new Field(COLUMN_NAME_COLUMN, ColumnType.VARCHAR),
                new Field("GRANTOR", ColumnType.VARCHAR),
                new Field("GRANTEE", ColumnType.VARCHAR),
                new Field("PRIVILEGE", ColumnType.VARCHAR),
                new Field("IS_GRANTABLE", ColumnType.VARCHAR)
        };
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog,
                                          String schema,
                                          String table,
                                          int scope,
                                          boolean nullable) throws SQLException {
        Field[] fields = getVersionColumnsMetadata();
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        Field[] fields = getVersionColumnsMetadata();
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    private Field[] getVersionColumnsMetadata() {
        return new Field[]{
                new Field("SCOPE", ColumnType.INT),
                new Field(COLUMN_NAME_COLUMN, ColumnType.VARCHAR),
                new Field(DATA_TYPE_COLUMN, ColumnType.INT),
                new Field(TABLE_NAME_COLUMN, ColumnType.BOOLEAN),
                new Field(COLUMN_SIZE_COLUMN, ColumnType.INT),
                new Field("BUFFER_LENGTH", ColumnType.INT),
                new Field("DECIMAL_DIGITS", ColumnType.INT),
                new Field("PSEUDO_COLUMN", ColumnType.INT),
        };
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        String sql = "SELECT CONSTRAINT_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, CONSTRAINT_NAME" +
                " FROM information_schema.key_column_usage" +
                " WHERE true";
        if (catalog != null) {
            sql += String.format(" AND CONSTRAINT_CATALOG = '%s'", catalog);
        }
        if (schema != null) {
            sql += String.format(" AND TABLE_SCHEMA = '%s'", schema);
        }
        if (table != null) {
            sql += String.format(" AND TABLE_NAME = '%s'", table);
        }
        return createMetaDataStatement().executeQuery(sql);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        Field[] fields = getImportedKeysMetadata();
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        Field[] fields = getImportedKeysMetadata();
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    private Field[] getImportedKeysMetadata() {
        return new Field[]{
                new Field("PKTABLE_CAT", ColumnType.VARCHAR),
                new Field("PKTABLE_SCHEM", ColumnType.VARCHAR),
                new Field("PKTABLE_NAME", ColumnType.VARCHAR),
                new Field("PKCOLUMN_NAME", ColumnType.VARCHAR),
                new Field("FKTABLE_CAT", ColumnType.VARCHAR),
                new Field("FKTABLE_SCHEM", ColumnType.VARCHAR),
                new Field("FKTABLE_NAME", ColumnType.VARCHAR),
                new Field("FKCOLUMN_NAME", ColumnType.VARCHAR),
                new Field("KEY_SEQ", ColumnType.VARCHAR),
                new Field("UPDATE_RULE", ColumnType.VARCHAR),
                new Field("DELETE_RULE", ColumnType.VARCHAR),
                new Field("DEFERRABILITY", ColumnType.INT)
        };
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog,
                                       String parentSchema,
                                       String parentTable,
                                       String foreignCatalog,
                                       String foreignSchema,
                                       String foreignTable) throws SQLException {
        Field[] fields = getImportedKeysMetadata();
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        Field[] fields = new Field[]{
                new Field(TYPE_NAME_COLUMN, ColumnType.VARCHAR),
                new Field(DATA_TYPE_COLUMN, ColumnType.INT),
                new Field(PRECISION_COLUMN, ColumnType.INT),
                new Field("LITERAL_PREFIX", ColumnType.VARCHAR),
                new Field("LITERAL_SUFFIX", ColumnType.VARCHAR),
                new Field("CREATE_PARAMS", ColumnType.INT),
                new Field(NULLABLE_COLUMN, ColumnType.INT),
                new Field("CASE_SENSITIVE", ColumnType.BOOLEAN),
                new Field("SEARCHABLE", ColumnType.INT),
                new Field("UNSIGNED_ATTRIBUTE", ColumnType.BOOLEAN),
                new Field("FIXED_PREC_SCALE", ColumnType.BOOLEAN),
                new Field("AUTO_INCREMENT", ColumnType.BOOLEAN),
                new Field("LOCAL_TYPE_NAME", ColumnType.VARCHAR),
                new Field("MINIMUM_SCALE", ColumnType.INT),
                new Field("MAXIMUM_SCALE", ColumnType.INT),
                new Field("SQL_DATA_TYPE", ColumnType.INT),
                new Field("SQL_DATETIME_SUB", ColumnType.INT),
                new Field("NUM_PREC_RADIX", ColumnType.INT),
        };
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    @Override
    public ResultSet getIndexInfo(String catalog,
                                  String schema,
                                  String table,
                                  boolean unique,
                                  boolean approximate) throws SQLException {
        Field[] fields = new Field[]{
                new Field("TABLE_CAT", ColumnType.VARCHAR),
                new Field("TABLE_SCHEM", ColumnType.VARCHAR),
                new Field("NON_UNIQUE", ColumnType.VARCHAR),
                new Field("INDEX_QUALIFIER", ColumnType.VARCHAR),
                new Field("INDEX_NAME", ColumnType.VARCHAR),
                new Field("TYPE", ColumnType.INT),
                new Field(ORDINAL_POSITION_COLUMN, ColumnType.INT),
                new Field("CARDINALITY", ColumnType.VARCHAR),
                new Field("PAGES", ColumnType.VARCHAR),
                new Field("FILTER_CONDITION", ColumnType.VARCHAR),
                new Field("CI_OID", ColumnType.VARCHAR),
                new Field("I_INDOPTION", ColumnType.VARCHAR),
                new Field("AM_NAME", ColumnType.VARCHAR),

        };
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return (type == ResultSet.TYPE_SCROLL_INSENSITIVE || type == ResultSet.TYPE_FORWARD_ONLY);
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return supportsResultSetType(type);
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return supportsResultSetType(type);
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return supportsResultSetType(type);
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog,
                             String schemaPattern,
                             String typeNamePattern,
                             int[] types) throws SQLException {
        Field[] fields = {
                new Field("TYPE_CAT", ColumnType.VARCHAR),
                new Field("TYPE_SCHEM", ColumnType.VARCHAR),
                new Field("TYPE_NAME", ColumnType.VARCHAR),
                new Field("CLASS_NAME", ColumnType.VARCHAR),
                new Field(REMARKS_COLUMN, ColumnType.VARCHAR)
        };
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    @Override
    public BaseConnection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getAttributes(String catalog,
                                   String schemaPattern,
                                   String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        String sql = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, IS_NULLABLE, ORDINAL_POSITION, CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION, DATA_TYPE" +
                " FROM information_schema.columns" +
                " WHERE true";
        if (catalog != null && !catalog.isEmpty()) {
            sql += String.format(" AND TABLE_CATALOG = '%s'", catalog);
        }
        if (schemaPattern != null && !schemaPattern.isEmpty()) {
            sql += String.format(" AND TABLE_SCHEMA = '%s'", schemaPattern);
        }
        if (typeNamePattern != null && !typeNamePattern.isEmpty()) {
            sql += String.format(" AND DATA_TYPE = '%s'", typeNamePattern);
        }
        if (attributeNamePattern != null && !attributeNamePattern.isEmpty()) {
            sql += String.format(" AND COLUMN_NAME = '%s'", attributeNamePattern);
        }
        return createMetaDataStatement().executeQuery(sql);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        final String[] dbVersionArr = this.connection.getDBVersionNumber().split("\\.");
        if (dbVersionArr.length > 0) {
            return Integer.parseInt(dbVersionArr[0]);
        } else {
            return 0;
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        final String[] dbVersionArr = this.connection.getDBVersionNumber().split("\\.");
        if (dbVersionArr.length > 1) {
            return Integer.parseInt(dbVersionArr[1]);
        } else {
            return 0;
        }
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return JDBC_MAJOR_VERSION;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return JDBC_MINOR_VERSION;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return 2;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }


    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        Field[] fields = {
                new Field("NAME", ColumnType.VARCHAR),
                new Field("MAX_LEN", ColumnType.INT),
                new Field("DEFAULT_VALUE", ColumnType.VARCHAR),
                new Field("DESCRIPTION", ColumnType.VARCHAR)
        };
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    @Override
    public ResultSet getFunctions(String catalog,
                                  String schemaPattern,
                                  String functionNamePattern) throws SQLException {
        Field[] fields = {
                new Field("FUNCTION_CAT", ColumnType.VARCHAR),
                new Field("FUNCTION_SCHEM", ColumnType.VARCHAR),
                new Field("FUNCTION_NAME", ColumnType.VARCHAR),
                new Field(REMARKS_COLUMN, ColumnType.VARCHAR),
                new Field("FUNCTION_TYPE", ColumnType.VARCHAR),
                new Field(SPECIFIC_NAME_COLUMN, ColumnType.VARCHAR)
        };
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    @Override
    public ResultSet getFunctionColumns(String catalog,
                                        String schemaPattern,
                                        String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        Field[] fields = {
                new Field("FUNCTION_CAT", ColumnType.VARCHAR),
                new Field("FUNCTION_SCHEM", ColumnType.VARCHAR),
                new Field("FUNCTION_NAME", ColumnType.VARCHAR),
                new Field(COLUMN_NAME_COLUMN, ColumnType.VARCHAR),
                new Field("COLUMN_TYPE", ColumnType.INT),
                new Field(DATA_TYPE_COLUMN, ColumnType.INT),
                new Field(TYPE_NAME_COLUMN, ColumnType.VARCHAR),
                new Field(PRECISION_COLUMN, ColumnType.INT),
                new Field("LENGTH", ColumnType.INT),
                new Field("SCALE", ColumnType.INT),
                new Field("RADIX", ColumnType.INT),
                new Field(NULLABLE_COLUMN, ColumnType.INT),
                new Field(REMARKS_COLUMN, ColumnType.VARCHAR),
                new Field(CHAR_OCTET_LENGTH_COLUMN, ColumnType.INT),
                new Field(ORDINAL_POSITION_COLUMN, ColumnType.INT),
                new Field(IS_NULLABLE_COLUMN, ColumnType.VARCHAR),
                new Field(SPECIFIC_NAME_COLUMN, ColumnType.VARCHAR)
        };
        try (DtmStatement dtmStatement = (DtmStatement) this.connection.createStatement()) {
            return dtmStatement.createDriverResultSet(fields, new ArrayList<>());
        }
    }

    @Override
    public ResultSet getPseudoColumns(String catalog,
                                      String schemaPattern,
                                      String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(this.getClass());
    }

    private Statement createMetaDataStatement() throws SQLException {
        return connection.createStatement();
    }
}
