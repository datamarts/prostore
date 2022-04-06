/*
 * Copyright © 2022 DATAMART LLC
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.jdbc.core.*;
import ru.datamart.prostore.jdbc.model.ColumnInfo;
import ru.datamart.prostore.jdbc.model.SchemaInfo;
import ru.datamart.prostore.jdbc.model.TableInfo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static ru.datamart.prostore.jdbc.util.DriverConstants.CATALOG_NAME_COLUMN;
import static ru.datamart.prostore.jdbc.util.DriverConstants.DATA_TYPE_COLUMN;

@ExtendWith(MockitoExtension.class)
class DtmDatabaseMetaDataTest {

    private static final String TABLE_CAT_COLUMN = "TABLE_CAT";
    private static final String TABLE_TYPE_COLUMN = "TABLE_TYPE";
    private static final String TABLE_NAME_COLUMN = "TABLE_NAME";

    private static final String TABLE_CATALOG_COLUMN = "TABLE_CATALOG";
    private static final String CONSTRAINT_CATALOG_COLUMN = "CONSTRAINT_CATALOG";
    private static final String TABLE_SCHEMA_COLUMN = "TABLE_SCHEMA";
    private static final String COLUMN_NAME_COLUMN = "COLUMN_NAME";
    private static final String ORDINAL_POSITION_COLUMN = "ORDINAL_POSITION";
    private static final String CONSTRAINT_NAME_COLUMN = "CONSTRAINT_NAME";
    private static final String IS_NULLABLE_COLUMN = "IS_NULLABLE";
    private static final String CHARACTER_MAXIMUM_LENGTH_COLUMN = "CHARACTER_MAXIMUM_LENGTH";
    private static final String DATETIME_PRECISION_COLUMN = "DATETIME_PRECISION";

    private static final String TABLE_TYPE = "TABLE";
    private static final String SYSTEM_VIEW_TYPE = "SYSTEM VIEW";
    private static final String VIEW_TYPE = "VIEW";

    private static final String CATALOG_VALUE = "catalog";
    private static final String TABLE_SCHEMA_VALUE = "dtm";
    private static final String TABLE_NAME_VALUE = "tbl";
    private static final String COLUMN_NAME_VALUE = "varchar_col";
    private static final int ORDINAL_POSITION_VALUE = 1;
    private static final String CONSTRAINT_NAME_VALUE = "pk_key";
    private static final String DATA_TYPE_VALUE = "varchar";
    private static final boolean IS_NULLABLE_VALUE = false;
    private static final int CHARACTER_MAXIMUM_LENGTH_VALUE = 30;

    private static final Field[] PK_FIELDS = new Field[]{new Field(CONSTRAINT_CATALOG_COLUMN, ColumnType.VARCHAR),
            new Field(TABLE_SCHEMA_COLUMN, ColumnType.VARCHAR),
            new Field(TABLE_NAME_COLUMN, ColumnType.VARCHAR),
            new Field(COLUMN_NAME_COLUMN, ColumnType.VARCHAR),
            new Field(ORDINAL_POSITION_COLUMN, ColumnType.INT),
            new Field(CONSTRAINT_NAME_COLUMN, ColumnType.VARCHAR)};
    private static final List<Tuple> PK_TUPLES = Collections.singletonList(new Tuple(new Object[]{CATALOG_VALUE,
            TABLE_SCHEMA_VALUE,
            TABLE_NAME_VALUE,
            COLUMN_NAME_VALUE,
            ORDINAL_POSITION_VALUE,
            CONSTRAINT_NAME_VALUE}));

    private static final Field[] ATTR_FIELDS = new Field[]{new Field(TABLE_CATALOG_COLUMN, ColumnType.VARCHAR),
            new Field(TABLE_SCHEMA_COLUMN, ColumnType.VARCHAR),
            new Field(TABLE_NAME_COLUMN, ColumnType.VARCHAR),
            new Field(COLUMN_NAME_COLUMN, ColumnType.VARCHAR),
            new Field(IS_NULLABLE_COLUMN, ColumnType.BOOLEAN),
            new Field(ORDINAL_POSITION_COLUMN, ColumnType.INT),
            new Field(CHARACTER_MAXIMUM_LENGTH_COLUMN, ColumnType.INT),
            new Field(DATETIME_PRECISION_COLUMN, ColumnType.INT),
            new Field(DATA_TYPE_COLUMN, ColumnType.VARCHAR)};
    private static final List<Tuple> ATTR_TUPLES = Collections.singletonList(new Tuple(new Object[]{CATALOG_VALUE,
            TABLE_SCHEMA_VALUE,
            TABLE_NAME_VALUE,
            COLUMN_NAME_VALUE,
            IS_NULLABLE_VALUE,
            ORDINAL_POSITION_VALUE,
            CHARACTER_MAXIMUM_LENGTH_VALUE,
            null,
            DATA_TYPE_VALUE}));

    @Mock
    private BaseConnection connection;
    @Mock
    private QueryExecutor queryExecutor;
    @Mock
    private Statement statement;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    @InjectMocks
    private DtmDatabaseMetaData dtmDatabaseMetaData;

    @Test
    void getSchemas() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(new DtmStatement(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));

        //act
        val schemas = dtmDatabaseMetaData.getSchemas();

        //assert
        val metaData = schemas.getMetaData();
        assertEquals(2, metaData.getColumnCount());
        val table_schem = "TABLE_SCHEM";
        assertEquals(table_schem, metaData.getColumnLabel(1));
        assertEquals(TABLE_CAT_COLUMN, metaData.getColumnLabel(2));
        assertTrue(schemas.next());
        assertEquals("", schemas.getString(1));
        assertEquals("", schemas.getString(2));
        assertEquals("", schemas.getString(table_schem));
        assertEquals("", schemas.getString(TABLE_CAT_COLUMN));
        val exception = assertThrows(ClassCastException.class, () -> schemas.getFloat(1));
        assertTrue(exception.getMessage().contains("Can't cast to Float, actual type is: class java.lang.String"));
        assertFalse(schemas.next());
    }

    @Test
    void getCatalogs() throws SQLException, JsonProcessingException {
        //arrange
        when(connection.getQueryExecutor()).thenReturn(queryExecutor);
        val str = "[{\"id\":\"schemaId\"," +
                "\"mnemonic\":\"schemaMnemonic\"}]";
        TypeReference<List<SchemaInfo>> typeReference = new TypeReference<List<SchemaInfo>>() {
        };
        when(queryExecutor.getSchemas()).thenReturn(new ObjectMapper().readValue(str, typeReference));
        when(connection.createStatement()).thenReturn(new DtmStatement(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));

        //act
        val catalogs = dtmDatabaseMetaData.getCatalogs();

        //assert
        val meta = catalogs.getMetaData();
        assertEquals(1, meta.getColumnCount());

        assertEquals(TABLE_CAT_COLUMN, meta.getColumnLabel(1));
        val schemaMnemonic = "schemaMnemonic";
        assertEquals(schemaMnemonic, catalogs.getString(1));
        assertEquals(schemaMnemonic, catalogs.getString(TABLE_CAT_COLUMN));
    }

    @Test
    void getTables() throws SQLException, JsonProcessingException {
        //arrange
        when(connection.getQueryExecutor()).thenReturn(queryExecutor);
        val str = "[{\"mnemonic\":\"tbl\"," +
                "\"datamartMnemonic\":\"dtm\"}]";
        TypeReference<List<TableInfo>> typeReference = new TypeReference<List<TableInfo>>() {
        };
        when(queryExecutor.getTables(anyString())).thenReturn(new ObjectMapper().readValue(str, typeReference));
        when(connection.createStatement()).thenReturn(new DtmStatement(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));

        //act
        val tables = dtmDatabaseMetaData.getTables(TABLE_CAT_COLUMN, null, null, null);

        //assert
        val meta = tables.getMetaData();
        assertEquals(7, meta.getColumnCount());

        assertEquals(TABLE_CAT_COLUMN, meta.getColumnLabel(1));
        assertEquals("TABLE_SCHEM", meta.getColumnLabel(2));
        assertEquals(TABLE_NAME_COLUMN, meta.getColumnLabel(3));
        assertEquals(TABLE_TYPE_COLUMN, meta.getColumnLabel(4));
        assertEquals("REMARKS", meta.getColumnLabel(5));
        assertEquals("SELF_REFERENCING_COL_NAME", meta.getColumnLabel(6));
        assertEquals("REF_GENERATION", meta.getColumnLabel(7));
        val schemaMnemonic = "dtm";
        val tableMneminic = "tbl";
        assertEquals(schemaMnemonic, tables.getString(TABLE_CAT_COLUMN));
        assertEquals(tableMneminic, tables.getString(TABLE_NAME_COLUMN));
        assertEquals("TABLE", tables.getString(TABLE_TYPE_COLUMN));
    }

    @Test
    void getTableTypes() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(new DtmStatement(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));

        //act
        val tableTypes = dtmDatabaseMetaData.getTableTypes();

        //assert
        val meta = tableTypes.getMetaData();
        assertEquals(1, meta.getColumnCount());
        assertEquals(TABLE_NAME_COLUMN, meta.getColumnLabel(1));

        tableTypes.next();
        assertEquals(TABLE_TYPE, tableTypes.getString(TABLE_NAME_COLUMN));
        tableTypes.next();
        assertEquals(SYSTEM_VIEW_TYPE, tableTypes.getString(TABLE_NAME_COLUMN));
        tableTypes.next();
        assertEquals(VIEW_TYPE, tableTypes.getString(TABLE_NAME_COLUMN));
    }

    @Test
    void getColumns() throws SQLException, JsonProcessingException {
        //arrange
        when(connection.getQueryExecutor()).thenReturn(queryExecutor);
        val str = "[{\"mnemonic\":\"varchar_col\"," +
                "\"entityMnemonic\":\"tbl\"," +
                "\"dataType\":\"VARCHAR\"," +
                "\"length\":\"30\"," +
                "\"ordinalPosition\":\"0\"," +
                "\"nullable\":\"false\"," +
                "\"datamartMnemonic\":\"dtm\"}]";
        TypeReference<List<ColumnInfo>> typeReference = new TypeReference<List<ColumnInfo>>() {
        };
        when(queryExecutor.getTableColumns(anyString(), anyString())).thenReturn(new ObjectMapper().readValue(str, typeReference));
        when(connection.createStatement()).thenReturn(new DtmStatement(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        when(connection.getTypeInfo()).thenReturn(new TypeInfoCache());

        //act
        val columns = dtmDatabaseMetaData.getColumns(TABLE_CAT_COLUMN, null, "", null);

        //assert
        val meta = columns.getMetaData();
        assertEquals(24, meta.getColumnCount());

        val schemaMnemonic = "dtm";
        val tableMneminic = "tbl";
        assertEquals(schemaMnemonic, columns.getString(TABLE_CAT_COLUMN));
        assertEquals(tableMneminic, columns.getString(TABLE_NAME_COLUMN));
        assertEquals("varchar_col", columns.getString("COLUMN_NAME"));
        assertEquals(12, columns.getInt("DATA_TYPE"));
        assertEquals("VARCHAR", columns.getString("TYPE_NAME"));
        assertEquals(30, columns.getInt("COLUMN_SIZE"));
        assertEquals(0, columns.getInt("NULLABLE"));
        assertEquals(1, columns.getInt("ORDINAL_POSITION"));
        assertEquals("NO", columns.getString("IS_NULLABLE"));
    }

    @Test
    void getColumnsWithPattern() throws SQLException, JsonProcessingException {
        //arrange
        when(connection.getQueryExecutor()).thenReturn(queryExecutor);
        val strTables = "[{\"mnemonic\":\"tbl\"," +
                "\"datamartMnemonic\":\"dtm\"}]";
        TypeReference<List<TableInfo>> tableTypeReference = new TypeReference<List<TableInfo>>() {
        };
        when(queryExecutor.getTables(anyString())).thenReturn(new ObjectMapper().readValue(strTables, tableTypeReference));
        when(connection.createStatement()).thenReturn(new DtmStatement(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        when(connection.getQueryExecutor()).thenReturn(queryExecutor);
        val str = "[{\"mnemonic\":\"varchar_col\"," +
                "\"entityMnemonic\":\"tbl\"," +
                "\"dataType\":\"VARCHAR\"," +
                "\"length\":\"30\"," +
                "\"ordinalPosition\":\"0\"," +
                "\"nullable\":\"false\"," +
                "\"datamartMnemonic\":\"dtm\"}]";
        TypeReference<List<ColumnInfo>> typeReference = new TypeReference<List<ColumnInfo>>() {
        };
        when(queryExecutor.getTableColumns(anyString(), anyString())).thenReturn(new ObjectMapper().readValue(str, typeReference));
        when(connection.createStatement()).thenReturn(new DtmStatement(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        when(connection.getTypeInfo()).thenReturn(new TypeInfoCache());

        //act
        val columns = dtmDatabaseMetaData.getColumns(TABLE_CAT_COLUMN, null, "%", null);

        //assert
        val meta = columns.getMetaData();
        assertEquals(24, meta.getColumnCount());

        val schemaMnemonic = "dtm";
        val tableMneminic = "tbl";
        assertEquals(schemaMnemonic, columns.getString(TABLE_CAT_COLUMN));
        assertEquals(tableMneminic, columns.getString(TABLE_NAME_COLUMN));
        assertEquals("varchar_col", columns.getString("COLUMN_NAME"));
        assertEquals(12, columns.getInt("DATA_TYPE"));
        assertEquals("VARCHAR", columns.getString("TYPE_NAME"));
        assertEquals(30, columns.getInt("COLUMN_SIZE"));
        assertEquals(0, columns.getInt("NULLABLE"));
        assertEquals(1, columns.getInt("ORDINAL_POSITION"));
        assertEquals("NO", columns.getString("IS_NULLABLE"));
    }

    @Test
    void getPrimaryKeys() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(sqlCaptor.capture())).thenReturn(new DtmResultSet(null, null, PK_FIELDS, PK_TUPLES));

        val primaryKeys = dtmDatabaseMetaData.getPrimaryKeys(null, null, null);

        assertEquals(6, primaryKeys.getMetaData().getColumnCount());
        assertEquals(CATALOG_VALUE, primaryKeys.getString(CONSTRAINT_CATALOG_COLUMN));
        assertEquals(TABLE_SCHEMA_VALUE, primaryKeys.getString(TABLE_SCHEMA_COLUMN));
        assertEquals(TABLE_NAME_VALUE, primaryKeys.getString(TABLE_NAME_COLUMN));
        assertEquals(COLUMN_NAME_VALUE, primaryKeys.getString(COLUMN_NAME_COLUMN));
        assertEquals(ORDINAL_POSITION_VALUE, primaryKeys.getInt(ORDINAL_POSITION_COLUMN));
        assertEquals(CONSTRAINT_NAME_VALUE, primaryKeys.getString(CONSTRAINT_NAME_COLUMN));
    }

    @Test
    void getPrimaryKeysWithCatalog() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(sqlCaptor.capture())).thenReturn(new DtmResultSet(null, null, PK_FIELDS, PK_TUPLES));

        val primaryKeys = dtmDatabaseMetaData.getPrimaryKeys("catalog", null, null);

        assertEquals(6, primaryKeys.getMetaData().getColumnCount());
        assertEquals(CATALOG_VALUE, primaryKeys.getString(CONSTRAINT_CATALOG_COLUMN));
        assertEquals(TABLE_SCHEMA_VALUE, primaryKeys.getString(TABLE_SCHEMA_COLUMN));
        assertEquals(TABLE_NAME_VALUE, primaryKeys.getString(TABLE_NAME_COLUMN));
        assertEquals(COLUMN_NAME_VALUE, primaryKeys.getString(COLUMN_NAME_COLUMN));
        assertEquals(ORDINAL_POSITION_VALUE, primaryKeys.getInt(ORDINAL_POSITION_COLUMN));
        assertEquals(CONSTRAINT_NAME_VALUE, primaryKeys.getString(CONSTRAINT_NAME_COLUMN));
        assertTrue(sqlCaptor.getValue().contains("AND CONSTRAINT_CATALOG = "));
    }

    @Test
    void getPrimaryKeysWithSchema() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(sqlCaptor.capture())).thenReturn(new DtmResultSet(null, null, PK_FIELDS, PK_TUPLES));

        val primaryKeys = dtmDatabaseMetaData.getPrimaryKeys(null, "schema", null);

        assertEquals(6, primaryKeys.getMetaData().getColumnCount());
        assertEquals(CATALOG_VALUE, primaryKeys.getString(CONSTRAINT_CATALOG_COLUMN));
        assertEquals(TABLE_SCHEMA_VALUE, primaryKeys.getString(TABLE_SCHEMA_COLUMN));
        assertEquals(TABLE_NAME_VALUE, primaryKeys.getString(TABLE_NAME_COLUMN));
        assertEquals(COLUMN_NAME_VALUE, primaryKeys.getString(COLUMN_NAME_COLUMN));
        assertEquals(ORDINAL_POSITION_VALUE, primaryKeys.getInt(ORDINAL_POSITION_COLUMN));
        assertEquals(CONSTRAINT_NAME_VALUE, primaryKeys.getString(CONSTRAINT_NAME_COLUMN));
        assertTrue(sqlCaptor.getValue().contains("AND TABLE_SCHEMA = "));
    }

    @Test
    void getPrimaryKeysWithTable() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(sqlCaptor.capture())).thenReturn(new DtmResultSet(null, null, PK_FIELDS, PK_TUPLES));

        val primaryKeys = dtmDatabaseMetaData.getPrimaryKeys(null, null, "tbl");

        assertEquals(6, primaryKeys.getMetaData().getColumnCount());
        assertEquals(CATALOG_VALUE, primaryKeys.getString(CONSTRAINT_CATALOG_COLUMN));
        assertEquals(TABLE_SCHEMA_VALUE, primaryKeys.getString(TABLE_SCHEMA_COLUMN));
        assertEquals(TABLE_NAME_VALUE, primaryKeys.getString(TABLE_NAME_COLUMN));
        assertEquals(COLUMN_NAME_VALUE, primaryKeys.getString(COLUMN_NAME_COLUMN));
        assertEquals(ORDINAL_POSITION_VALUE, primaryKeys.getInt(ORDINAL_POSITION_COLUMN));
        assertEquals(CONSTRAINT_NAME_VALUE, primaryKeys.getString(CONSTRAINT_NAME_COLUMN));
        assertTrue(sqlCaptor.getValue().contains("AND TABLE_NAME = "));
    }

    @Test
    void getPrimaryKeysFailed() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(sqlCaptor.capture())).thenThrow(new SQLException());

        assertThrows(SQLException.class, () -> dtmDatabaseMetaData.getPrimaryKeys(null, null, null));
    }

    @Test
    void getAttributes() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(sqlCaptor.capture())).thenReturn(new DtmResultSet(null, null, ATTR_FIELDS, ATTR_TUPLES));

        val attributes = dtmDatabaseMetaData.getAttributes(null, null, null, null);
        assertEquals(9, attributes.getMetaData().getColumnCount());
        assertEquals(CATALOG_VALUE, attributes.getString(TABLE_CATALOG_COLUMN));
        assertEquals(TABLE_SCHEMA_VALUE, attributes.getString(TABLE_SCHEMA_COLUMN));
        assertEquals(TABLE_NAME_VALUE, attributes.getString(TABLE_NAME_COLUMN));
        assertEquals(COLUMN_NAME_VALUE, attributes.getString(COLUMN_NAME_COLUMN));
        assertEquals(IS_NULLABLE_VALUE, attributes.getBoolean(IS_NULLABLE_COLUMN));
        assertEquals(ORDINAL_POSITION_VALUE, attributes.getInt(ORDINAL_POSITION_COLUMN));
        assertEquals(CHARACTER_MAXIMUM_LENGTH_VALUE, attributes.getInt(CHARACTER_MAXIMUM_LENGTH_COLUMN));
        assertNull(attributes.getObject(DATETIME_PRECISION_COLUMN));
        assertEquals(DATA_TYPE_VALUE, attributes.getString(DATA_TYPE_COLUMN));
    }

    @Test
    void getAttributesWithCatalog() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(sqlCaptor.capture())).thenReturn(new DtmResultSet(null, null, ATTR_FIELDS, ATTR_TUPLES));

        val attributes = dtmDatabaseMetaData.getAttributes("catalog", null, null, null);
        assertEquals(9, attributes.getMetaData().getColumnCount());
        assertEquals(CATALOG_VALUE, attributes.getString(TABLE_CATALOG_COLUMN));
        assertEquals(TABLE_SCHEMA_VALUE, attributes.getString(TABLE_SCHEMA_COLUMN));
        assertEquals(TABLE_NAME_VALUE, attributes.getString(TABLE_NAME_COLUMN));
        assertEquals(COLUMN_NAME_VALUE, attributes.getString(COLUMN_NAME_COLUMN));
        assertEquals(IS_NULLABLE_VALUE, attributes.getBoolean(IS_NULLABLE_COLUMN));
        assertEquals(ORDINAL_POSITION_VALUE, attributes.getInt(ORDINAL_POSITION_COLUMN));
        assertEquals(CHARACTER_MAXIMUM_LENGTH_VALUE, attributes.getInt(CHARACTER_MAXIMUM_LENGTH_COLUMN));
        assertNull(attributes.getObject(DATETIME_PRECISION_COLUMN));
        assertEquals(DATA_TYPE_VALUE, attributes.getString(DATA_TYPE_COLUMN));
        assertTrue(sqlCaptor.getValue().contains("AND TABLE_CATALOG = "));
    }

    @Test
    void getAttributesWithSchema() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(sqlCaptor.capture())).thenReturn(new DtmResultSet(null, null, ATTR_FIELDS, ATTR_TUPLES));

        val attributes = dtmDatabaseMetaData.getAttributes(null, "schema", null, null);
        assertEquals(9, attributes.getMetaData().getColumnCount());
        assertEquals(CATALOG_VALUE, attributes.getString(TABLE_CATALOG_COLUMN));
        assertEquals(TABLE_SCHEMA_VALUE, attributes.getString(TABLE_SCHEMA_COLUMN));
        assertEquals(TABLE_NAME_VALUE, attributes.getString(TABLE_NAME_COLUMN));
        assertEquals(COLUMN_NAME_VALUE, attributes.getString(COLUMN_NAME_COLUMN));
        assertEquals(IS_NULLABLE_VALUE, attributes.getBoolean(IS_NULLABLE_COLUMN));
        assertEquals(ORDINAL_POSITION_VALUE, attributes.getInt(ORDINAL_POSITION_COLUMN));
        assertEquals(CHARACTER_MAXIMUM_LENGTH_VALUE, attributes.getInt(CHARACTER_MAXIMUM_LENGTH_COLUMN));
        assertNull(attributes.getObject(DATETIME_PRECISION_COLUMN));
        assertEquals(DATA_TYPE_VALUE, attributes.getString(DATA_TYPE_COLUMN));
        assertTrue(sqlCaptor.getValue().contains("AND TABLE_SCHEMA = "));
    }

    @Test
    void getAttributesWithTypeName() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(sqlCaptor.capture())).thenReturn(new DtmResultSet(null, null, ATTR_FIELDS, ATTR_TUPLES));

        val attributes = dtmDatabaseMetaData.getAttributes(null, null, "typeName", null);
        assertEquals(9, attributes.getMetaData().getColumnCount());
        assertEquals(CATALOG_VALUE, attributes.getString(TABLE_CATALOG_COLUMN));
        assertEquals(TABLE_SCHEMA_VALUE, attributes.getString(TABLE_SCHEMA_COLUMN));
        assertEquals(TABLE_NAME_VALUE, attributes.getString(TABLE_NAME_COLUMN));
        assertEquals(COLUMN_NAME_VALUE, attributes.getString(COLUMN_NAME_COLUMN));
        assertEquals(IS_NULLABLE_VALUE, attributes.getBoolean(IS_NULLABLE_COLUMN));
        assertEquals(ORDINAL_POSITION_VALUE, attributes.getInt(ORDINAL_POSITION_COLUMN));
        assertEquals(CHARACTER_MAXIMUM_LENGTH_VALUE, attributes.getInt(CHARACTER_MAXIMUM_LENGTH_COLUMN));
        assertNull(attributes.getObject(DATETIME_PRECISION_COLUMN));
        assertEquals(DATA_TYPE_VALUE, attributes.getString(DATA_TYPE_COLUMN));
        assertTrue(sqlCaptor.getValue().contains("AND DATA_TYPE = "));
    }

    @Test
    void getAttributesWithAttrName() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(sqlCaptor.capture())).thenReturn(new DtmResultSet(null, null, ATTR_FIELDS, ATTR_TUPLES));

        val attributes = dtmDatabaseMetaData.getAttributes(null, null, null, "attrName");
        assertEquals(9, attributes.getMetaData().getColumnCount());
        assertEquals(CATALOG_VALUE, attributes.getString(TABLE_CATALOG_COLUMN));
        assertEquals(TABLE_SCHEMA_VALUE, attributes.getString(TABLE_SCHEMA_COLUMN));
        assertEquals(TABLE_NAME_VALUE, attributes.getString(TABLE_NAME_COLUMN));
        assertEquals(COLUMN_NAME_VALUE, attributes.getString(COLUMN_NAME_COLUMN));
        assertEquals(IS_NULLABLE_VALUE, attributes.getBoolean(IS_NULLABLE_COLUMN));
        assertEquals(ORDINAL_POSITION_VALUE, attributes.getInt(ORDINAL_POSITION_COLUMN));
        assertEquals(CHARACTER_MAXIMUM_LENGTH_VALUE, attributes.getInt(CHARACTER_MAXIMUM_LENGTH_COLUMN));
        assertNull(attributes.getObject(DATETIME_PRECISION_COLUMN));
        assertEquals(DATA_TYPE_VALUE, attributes.getString(DATA_TYPE_COLUMN));
        assertTrue(sqlCaptor.getValue().contains("AND COLUMN_NAME = "));
    }

    @Test
    void getAttributesFailed() throws SQLException {
        //arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(sqlCaptor.capture())).thenThrow(new SQLException());

        assertThrows(SQLException.class, () -> dtmDatabaseMetaData.getAttributes(null, null, null, null));
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(dtmDatabaseMetaData, dtmDatabaseMetaData.unwrap(DtmDatabaseMetaData.class));
        assertThrows(SQLException.class, () -> dtmDatabaseMetaData.unwrap(DtmDatabaseMetaDataTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(dtmDatabaseMetaData.isWrapperFor(DtmDatabaseMetaData.class));
        assertFalse(dtmDatabaseMetaData.isWrapperFor(null));
    }
}