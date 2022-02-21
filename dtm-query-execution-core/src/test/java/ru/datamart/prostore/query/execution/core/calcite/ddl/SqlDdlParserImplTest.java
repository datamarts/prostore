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
package ru.datamart.prostore.query.execution.core.calcite.ddl;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.extension.check.*;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlAlterView;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlCreateTable;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlCreateView;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlDropTable;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlUseSchema;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDefinitionService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class SqlDdlParserImplTest {
    private static final String CREATE_TABLE_QUERY = "create table test.table_name\n" +
            "(\n" +
            "    account_id bigint,\n" +
            "    account_type varchar(1), -- D/C (дебет/кредит)\n" +
            "    primary key (account_id)\n" +
            ") distributed by (account_id)";
    private static final String DROP_TABLE_QUERY = "drop table test.table_name";
    private final SqlDialect sqlDialect = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(calciteConfiguration.configEddlParser(
                    calciteCoreConfiguration.eddlParserImplFactory()));

    @Test
    void parseAlter() {
        SqlNode sqlNode = definitionService.processingQuery("ALTER VIEW test.view_a AS SELECT * FROM test.tab_1");
        assertTrue(sqlNode instanceof SqlAlterView);
        assertEquals(Arrays.asList("test", "view_a"),
                ((SqlIdentifier) ((SqlAlterView) sqlNode).getOperandList().get(0)).names);
        assertEquals(3, ((SqlAlterView) sqlNode).getOperandList().size());
    }

    @Test
    void parseUseSchema() {
        SqlNode sqlNode = definitionService.processingQuery("USE shares");
        assertTrue(sqlNode instanceof SqlUseSchema);
        assertEquals("shares", ((SqlIdentifier) ((SqlUseSchema) sqlNode).getOperandList().get(0)).names.get(0));
        assertEquals("USE shares", sqlNode.toSqlString(new SqlDialect(SqlDialect.EMPTY_CONTEXT)).toString());
    }

    @Test
    void parseUseSchemaWithQuotes() {
        SqlNode sqlNode = definitionService.processingQuery("USE \"shares\"");
        assertTrue(sqlNode instanceof SqlUseSchema);
        assertEquals("shares", ((SqlIdentifier) ((SqlUseSchema) sqlNode).getOperandList().get(0)).names.get(0));
        assertEquals("USE shares", sqlNode.toSqlString(new SqlDialect(SqlDialect.EMPTY_CONTEXT)).toString());
    }

    @Test
    void parseIncorrectUseSchema() {
        assertThrows(SqlParseException.class, () ->
                definitionService.processingQuery("USEshares"));
        assertThrows(SqlParseException.class, () ->
                definitionService.processingQuery("USE shares t"));
        assertThrows(SqlParseException.class, () ->
                definitionService.processingQuery("USE 'shares'"));
    }

    @Test
    void parseAlterWithoutFromClause() {
        assertThrows(SqlParseException.class, () ->
                definitionService.processingQuery("ALTER VIEW test.view_a AS SELECT * "));
    }

    @Test
    void parseCreateViewSuccess() {
        SqlNode sqlNode = definitionService.processingQuery("CREATE VIEW test.view_a AS SELECT * FROM test.tab_1");
        assertTrue(sqlNode instanceof SqlCreateView);
    }

    @Test
    void parseCreateViewWithoutFromClause() {
        assertThrows(SqlParseException.class, () ->
                definitionService.processingQuery("CREATE VIEW test.view_a AS SELECT * ft"));
    }

    @Test
    void createTable() {
        createTable(CREATE_TABLE_QUERY);
    }

    @Test
    void createTableWithDestination() {
        Set<SourceType> selectedSourceTypes = new HashSet<>();
        selectedSourceTypes.add(SourceType.ADB);
        selectedSourceTypes.add(SourceType.ADG);
        String query = String.format(CREATE_TABLE_QUERY + " DATASOURCE_TYPE (%s)",
                selectedSourceTypes.stream().map(SourceType::name).collect(Collectors.joining(", ")));
        createTable(query, sqlCreateTable -> assertEquals(selectedSourceTypes, sqlCreateTable.getDestination().getDatasourceTypes()));
    }

    @Test
    void createTableWithInformationSchema() {
        String query = String.format(CREATE_TABLE_QUERY + " DATASOURCE_TYPE (%s)",
                SourceType.INFORMATION_SCHEMA.name());
        assertThrows(SqlParseException.class, () -> createTable(query));
    }

    @Test
    void createTableWithInvalidDestination() {
        String query = String.format(CREATE_TABLE_QUERY + " DATASOURCE_TYPE (%s)", "adcvcb");
        assertThrows(SqlParseException.class, () -> createTable(query));
    }

    @Test
    void createTableWithInvalidDatasourceTypeDefinition() {
        //FIXME
        String query = String.format(CREATE_TABLE_QUERY + " DATASOURCE_TYPE='%s'", "adb");
        assertThrows(SqlParseException.class, () -> createTable(query));
    }

    void createTable(String query) {
        createTable(query, sqlCreateTable -> {
        });
    }

    void createTable(String query, Consumer<SqlCreateTable> consumer) {
        SqlNode sqlNode = definitionService.processingQuery(query);
        assertTrue(sqlNode instanceof SqlCreateTable);
        SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;
        assertEquals(Arrays.asList("test", "table_name"),
                ((SqlIdentifier) sqlCreateTable.getOperandList().get(0)).names);
        consumer.accept(sqlCreateTable);
    }

    @Test
    void dropTable() {
        dropTable(DROP_TABLE_QUERY);
    }


    @Test
    void dropTableWithQuotedDestination() {
        String query = DROP_TABLE_QUERY + " DATASOURCE_TYPE = 'adb'";
        dropTable(query, sqlDropTable -> assertEquals(SourceType.ADB, sqlDropTable.getDestination().getValue()));
    }

    @Test
    void dropTableWithDestination() {
        String query = DROP_TABLE_QUERY + " DATASOURCE_TYPE = adb";
        dropTable(query, sqlDropTable -> assertEquals(SourceType.ADB, sqlDropTable.getDestination().getValue()));
    }

    @Test
    void dropTableWithInformationSchema() {
        String query = String.format(DROP_TABLE_QUERY + " DATASOURCE_TYPE = %s",
                SourceType.INFORMATION_SCHEMA.name());
        assertThrows(SqlParseException.class, () -> dropTable(query));
    }

    @Test
    void dropTableWithInvalidDestination() {
        String query = String.format(DROP_TABLE_QUERY + " DATASOURCE_TYPE = %s", "adcvcb");
        assertThrows(SqlParseException.class, () -> dropTable(query));
    }

    @Test
    void checkDatabase() {
        String schema = "test";
        String correctSchema = "CHECK_DATABASE(test)";
        String withoutSchema = "CHECK_DATABASE()";
        String withoutSchema2 = "CHECK_DATABASE";
        String incorrectSchema = "CHECK_DATABASE('77')";
        String incorrectSchema2 = "CHECK_DATABASE(test.ttt)";

        SqlNode sqlNode1 = definitionService.processingQuery(correctSchema);
        SqlNode sqlNode2 = definitionService.processingQuery(withoutSchema);
        SqlNode sqlNode3 = definitionService.processingQuery(withoutSchema2);
        assertEquals(schema, ((SqlCheckDatabase) sqlNode1).getSchema());
        assertNull(((SqlCheckDatabase) sqlNode2).getSchema());
        assertNull(((SqlCheckDatabase) sqlNode3).getSchema());
        assertThrows(SqlParseException.class, () -> definitionService.processingQuery(incorrectSchema));
        assertThrows(SqlParseException.class, () -> definitionService.processingQuery(incorrectSchema2));
    }

    @Test
    void checkTable() {
        String schema = "test";
        String table = "test_table";
        String withSchema = "CHECK_TABLE(test.test_table)";
        String withoutSchema = "CHECK_TABLE(test_table)";
        String incorrectTable = "CHECK_TABLE()";
        SqlNode sqlNode1 = definitionService.processingQuery(withSchema);
        SqlNode sqlNode2 = definitionService.processingQuery(withoutSchema);

        assertEquals(schema, ((SqlCheckTable) sqlNode1).getSchema());
        assertEquals(table, ((SqlCheckTable) sqlNode1).getTable());
        assertNull(((SqlCheckTable) sqlNode2).getSchema());
        assertThrows(SqlParseException.class, () -> definitionService.processingQuery(incorrectTable));
    }

    @Test
    void checkData() {
        testCheckData(1L,
                "CHECK_DATA(test.testTable, 1, [id, name])",
                "CHECK_DATA(testTable, 1, [id, name])",
                "CHECK_DATA(test.testTable, 1)",
                "CHECK_DATA(test.testTable, 1, [id,, name])",
                "CHECK_DATA(test.testTable, a, [id, name])");

    }

    @Test
    void checkDataWithNormalization() {
        testCheckData(2L,
                "CHECK_DATA(test.testTable, 1, 2, [id, name])",
                "CHECK_DATA(testTable, 1, 2, [id, name])",
                "CHECK_DATA(test.testTable, 1, 2)",
                "CHECK_DATA(test.testTable, 1, 2, [id,, name])",
                "CHECK_DATA(test.testTable, a, 2, [id, name])");

    }

    private void testCheckData(long expectedNormalization,
                               String withSchema,
                               String withoutSchema,
                               String withoutColumns,
                               String withIncorrectColumns,
                               String withIncorrectDelta) {
        String schema = "test";
        String table = "testtable";
        Long expectedDeltaNum = 1L;
        Long normalization = expectedNormalization;
        Set<String> columns = new HashSet<>(Arrays.asList("id", "name"));

        SqlNode sqlNode1 = definitionService.processingQuery(withSchema);
        assertEquals(schema, ((SqlCheckData) sqlNode1).getSchema());
        assertEquals(table, ((SqlCheckData) sqlNode1).getTable());
        assertEquals(columns, ((SqlCheckData) sqlNode1).getColumns());
        assertEquals(expectedDeltaNum, ((SqlCheckData) sqlNode1).getDeltaNum());
        assertEquals(normalization, ((SqlCheckData) sqlNode1).getNormalization());
        assertEquals("CHECK_DATA(test.testtable, 1, [id, name])", sqlNode1.toSqlString(sqlDialect).toString());

        SqlNode sqlNode2 = definitionService.processingQuery(withoutSchema);
        assertNull(((SqlCheckData) sqlNode2).getSchema());
        assertEquals(table, ((SqlCheckData) sqlNode2).getTable());
        assertEquals(columns, ((SqlCheckData) sqlNode2).getColumns());
        assertEquals(expectedDeltaNum, ((SqlCheckData) sqlNode2).getDeltaNum());
        assertEquals(normalization, ((SqlCheckData) sqlNode2).getNormalization());
        assertEquals("CHECK_DATA(testtable, 1, [id, name])", sqlNode2.toSqlString(sqlDialect).toString());

        SqlNode sqlNode3 = definitionService.processingQuery(withoutColumns);
        assertEquals(schema, ((SqlCheckData) sqlNode3).getSchema());
        assertEquals(table, ((SqlCheckData) sqlNode3).getTable());
        assertNull(((SqlCheckData) sqlNode3).getColumns());
        assertEquals(expectedDeltaNum, ((SqlCheckData) sqlNode3).getDeltaNum());
        assertEquals(normalization, ((SqlCheckData) sqlNode3).getNormalization());
        assertEquals("CHECK_DATA(test.testtable, 1)", sqlNode3.toSqlString(sqlDialect).toString());

        assertThrows(SqlParseException.class, () -> definitionService.processingQuery(withIncorrectColumns));
        assertThrows(SqlParseException.class, () -> definitionService.processingQuery(withIncorrectDelta));
    }

    @Test
    void checkVersions() {
        String correct = "CHECK_VERSIONS()";
        String incorrect = "CHECK_VERSIONS(test)";

        SqlNode sqlNode1 = definitionService.processingQuery(correct);
        assertNull(((SqlCheckVersions) sqlNode1).getSchema());
        assertThrows(SqlParseException.class, () -> definitionService.processingQuery(incorrect));
    }

    @Test
    void checkSum() {
        testCheckSum(1L,
                "CHECK_SUM(1, test.test_table, [id, name])",
                "CHECK_SUM(1, test_table, [id, name])",
                "CHECK_SUM(1, test_table)",
                "CHECK_SUM(1)",
                "CHECK_SUM(1, test_table, [id,, name])",
                "CHECK_SUM(a, test_table, [id, name])");

    }

    @Test
    void checkSumWithNormalization() {
        testCheckSum(2L,
                "CHECK_SUM(1, 2, test.test_table, [id, name])",
                "CHECK_SUM(1, 2, test_table, [id, name])",
                "CHECK_SUM(1, 2, test_table)",
                "CHECK_SUM(1, 2)",
                "CHECK_SUM(1, 2, test_table, [id,, name])",
                "CHECK_SUM(a, 2, test_table, [id, name])");

    }

    private void testCheckSum(long expectedNormalization,
                              String withSchema,
                              String withoutSchema,
                              String withoutColumns,
                              String withoutTable,
                              String withIncorrectColumns,
                              String withIncorrectDelta) {
        String schema = "test";
        String table = "test_table";
        Long expectedDeltaNum = 1L;
        Set<String> columns = new HashSet<>(Arrays.asList("id", "name"));

        SqlNode sqlNode1 = definitionService.processingQuery(withSchema);
        assertEquals(expectedDeltaNum, ((SqlCheckSumBasic) sqlNode1).getDeltaNum());
        assertEquals(expectedNormalization, ((SqlCheckSumBasic) sqlNode1).getNormalization());
        assertEquals(schema, ((SqlCheckSumBasic) sqlNode1).getSchema());
        assertEquals(table, ((SqlCheckSumBasic) sqlNode1).getTable());
        assertEquals(columns, ((SqlCheckSumBasic) sqlNode1).getColumns());
        assertEquals("CHECK_SUM(1, test.test_table, [id, name])", sqlNode1.toSqlString(sqlDialect).toString());

        SqlNode sqlNode2 = definitionService.processingQuery(withoutSchema);
        assertEquals(expectedDeltaNum, ((SqlCheckSumBasic) sqlNode2).getDeltaNum());
        assertEquals(expectedNormalization, ((SqlCheckSumBasic) sqlNode2).getNormalization());
        assertNull(((SqlCheckSumBasic) sqlNode2).getSchema());
        assertEquals(table, ((SqlCheckSumBasic) sqlNode2).getTable());
        assertEquals(columns, ((SqlCheckSumBasic) sqlNode2).getColumns());
        assertEquals("CHECK_SUM(1, test_table, [id, name])", sqlNode2.toSqlString(sqlDialect).toString());

        SqlNode sqlNode3 = definitionService.processingQuery(withoutColumns);
        assertEquals(expectedDeltaNum, ((SqlCheckSumBasic) sqlNode3).getDeltaNum());
        assertEquals(expectedNormalization, ((SqlCheckSumBasic) sqlNode3).getNormalization());
        assertNull(((SqlCheckSumBasic) sqlNode3).getSchema());
        assertEquals(table, ((SqlCheckSumBasic) sqlNode3).getTable());
        assertNull(((SqlCheckSumBasic) sqlNode3).getColumns());
        assertEquals("CHECK_SUM(1, test_table)", sqlNode3.toSqlString(sqlDialect).toString());

        SqlNode sqlNode4 = definitionService.processingQuery(withoutTable);
        assertEquals(expectedDeltaNum, ((SqlCheckSumBasic) sqlNode4).getDeltaNum());
        assertEquals(expectedNormalization, ((SqlCheckSumBasic) sqlNode4).getNormalization());
        assertNull(((SqlCheckSumBasic) sqlNode4).getSchema());
        assertNull(((SqlCheckSumBasic) sqlNode4).getTable());
        assertNull(((SqlCheckSumBasic) sqlNode4).getColumns());
        assertEquals("CHECK_SUM(1)", sqlNode4.toSqlString(sqlDialect).toString());

        assertThrows(SqlParseException.class, () -> definitionService.processingQuery(withIncorrectColumns));
        assertThrows(SqlParseException.class, () -> definitionService.processingQuery(withIncorrectDelta));
    }


    void dropTable(String query) {
        dropTable(query, sqlDropTable -> {
        });
    }

    void dropTable(String query, Consumer<SqlDropTable> consumer) {
        SqlNode sqlNode = definitionService.processingQuery(query);
        assertTrue(sqlNode instanceof SqlDropTable);
        SqlDropTable sqlDropTable = (SqlDropTable) sqlNode;
        assertEquals(Arrays.asList("test", "table_name"),
                ((SqlIdentifier) sqlDropTable.getOperandList().get(0)).names);
        consumer.accept(sqlDropTable);
    }
}
