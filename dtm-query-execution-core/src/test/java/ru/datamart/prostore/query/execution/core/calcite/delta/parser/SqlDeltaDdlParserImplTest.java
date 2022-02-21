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
package ru.datamart.prostore.query.execution.core.calcite.delta.parser;

import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.common.plugin.exload.Type;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlCreateTable;
import ru.datamart.prostore.query.calcite.core.extension.delta.SqlBeginDelta;
import ru.datamart.prostore.query.calcite.core.extension.delta.SqlCommitDelta;
import ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaByDateTime;
import ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaByNum;
import ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaHot;
import ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaOk;
import ru.datamart.prostore.query.calcite.core.extension.eddl.FormatOperator;
import ru.datamart.prostore.query.calcite.core.extension.eddl.LocationOperator;
import ru.datamart.prostore.query.calcite.core.extension.eddl.SqlCreateDownloadExternalTable;
import ru.datamart.prostore.query.calcite.core.extension.eddl.SqlNodeUtils;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlDeltaDdlParserImplTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(
            calciteCoreConfiguration.eddlParserImplFactory());
    private SqlDialect sqlDialect;

    @BeforeEach
    void setUp() {
        sqlDialect = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    }

    @Test
    public void testBeginDeltaWithNum() throws SqlParseException {
        SqlNode sqlNode = getSqlNode("BEGIN DELTA SET 1");
        assertTrue(sqlNode instanceof SqlBeginDelta);
    }

    @Test
    public void testBeginDeltaDefault() throws SqlParseException {
        SqlNode sqlNode = getSqlNode("BEGIN DELTA");
        assertTrue(sqlNode instanceof SqlBeginDelta);
    }

    @Test
    public void testCommitDeltaWithDateTime() throws SqlParseException {
        SqlNode sqlNode = getSqlNode("COMMIT DELTA SET '2020-06-11T10:00:00'");
        assertTrue(sqlNode instanceof SqlCommitDelta);
    }

    @Test
    public void testCommitDeltaDefault() throws SqlParseException {
        SqlNode sqlNode = getSqlNode("COMMIT DELTA");
        assertTrue(sqlNode instanceof SqlCommitDelta);
    }

    @Test
    public void testCreateDownloadExtTableInvalidSql() {
        assertThrows(SqlParseException.class,
                () -> getSqlNode("CREATE DOWNLOAD EXTERNAL TABLE s FORMAT 'avro'"));
    }

    @Test
    public void testCreateDownloadExtTableInvalidTypeLocationOperator() {
        assertThrows(SqlParseException.class,
                () -> getSqlNode("CREATE DOWNLOAD EXTERNAL TABLE s LOCATION 'kafkaTopic1=test' FORMAT 'avro'"));
    }

    @Test
    public void testCreateDownloadExtTableInvalidFormat() {
        assertThrows(SqlParseException.class,
                () -> getSqlNode("CREATE DOWNLOAD EXTERNAL TABLE s LOCATION 'kafkaTopic=test' FORMAT 'avro1'"));
    }

    @Test
    public void testCreateDownloadExtTableOmitChunkSize() throws SqlParseException {
        SqlNode sqlNode = getSqlNode("CREATE DOWNLOAD EXTERNAL TABLE s (id integer, name varchar(100)) LOCATION 'kafka://zookeeper_host:port/topic' FORMAT 'avro'");
        assertTrue(sqlNode instanceof SqlCreateDownloadExternalTable);

        SqlCreateDownloadExternalTable sqlCreateDownloadExternalTable = (SqlCreateDownloadExternalTable) sqlNode;
        assertEquals("s",
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, SqlIdentifier.class).getSimple());
        assertEquals(Type.KAFKA_TOPIC,
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, LocationOperator.class).getType());
        assertEquals("kafka://zookeeper_host:port/topic",
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, LocationOperator.class).getLocation());
        assertEquals(ExternalTableFormat.AVRO,
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, FormatOperator.class).getFormat());
    }

    @Test
    void parseDdlWithQuote() throws SqlParseException {
        SqlCreateTable node =
                (SqlCreateTable) getSqlNode("CREATE TABLE a(\"index\" integer)");
        assertNotNull(node);
        assertEquals("a", SqlNodeUtils.getOne(node, SqlIdentifier.class).getSimple());
        assertEquals("index",
                SqlNodeUtils.getOne(
                        (SqlColumnDeclaration) SqlNodeUtils.getOne(node, SqlNodeList.class).getList().get(0),
                        SqlIdentifier.class).getSimple());
    }

    @Test
    void parseGetDeltaOk() throws SqlParseException {
        final String sql = "GET_DELTA_OK()";
        SqlGetDeltaOk node = (SqlGetDeltaOk) getSqlNode(sql);
        assertNotNull(node);
        assertEquals(sql, node.toSqlString(sqlDialect).toString());
    }

    @Test
    void parseGetDeltaHot() throws SqlParseException {
        final String sql = "GET_DELTA_HOT()";
        SqlGetDeltaHot node = (SqlGetDeltaHot) getSqlNode(sql);
        assertNotNull(node);
        assertEquals(sql, node.toSqlString(sqlDialect).toString());
    }

    @Test
    void parseGetDeltaByDateTime() throws SqlParseException {
        final String dateTime = "2020-10-26 15:00:11";
        final String sql = "GET_DELTA_BY_DATETIME('" + dateTime + "')";
        SqlGetDeltaByDateTime node = (SqlGetDeltaByDateTime) getSqlNode(sql);
        assertNotNull(node);
        assertEquals(node.getDeltaDateTime(), dateTime);
        assertEquals(sql, node.toSqlString(sqlDialect).toString());
    }

    @Test
    void parseGetDeltaByNum() throws SqlParseException {
        final String sql = "GET_DELTA_BY_NUM(15)";
        SqlGetDeltaByNum node = (SqlGetDeltaByNum) getSqlNode(sql);
        assertNotNull(node);
        assertEquals(15L, node.getDeltaNum());
        assertEquals(sql, node.toSqlString(sqlDialect).toString());
    }

    @Test
    void parseGetDeltaByNumError() {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        assertAll("Try validate GetDeltaByNum error queries",
                () -> assertThrows(SqlParseException.class, () -> planner.parse("GET_DELTA_BY_NUM()")),
                () -> assertThrows(SqlParseException.class, () -> planner.parse("GET_DELTA_BY_NUM('15')")),
                () -> assertThrows(SqlParseException.class, () -> planner.parse("GET_DELTA_BY_NUM(null)")  )
        );
    }

    private SqlNode getSqlNode(String s) throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        return planner.parse(s);
    }
}
