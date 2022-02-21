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
package ru.datamart.prostore.query.execution.core.dml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.core.calcite.factory.CoreSchemaFactory;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteContextProvider;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.core.dml.service.ColumnMetadataService;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.datamart.prostore.query.execution.core.utils.TestUtils.loadTextFromFile;

@Slf4j
@ExtendWith(VertxExtension.class)
class ColumnMetadataServiceTest {
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlParser.Config configParser = calciteConfiguration.configEddlParser(calciteConfiguration.getSqlParserFactory());
    private final CoreCalciteSchemaFactory calciteSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CoreCalciteContextProvider calciteContextProvider = new CoreCalciteContextProvider(configParser, calciteSchemaFactory);
    private final QueryParserService parserService = new CoreCalciteDMLQueryParserService(calciteContextProvider, Vertx.vertx());
    private final ColumnMetadataService service = new ColumnMetadataService(parserService);

    @Test
    void getColumnMetadata(VertxTestContext testContext) throws JsonProcessingException {
        val sql = "select * from dml.accounts";
        val datamarts = CoreSerialization.mapper()
                .readValue(loadTextFromFile("schema/dml_all_types.json"), new TypeReference<List<Datamart>>() {
                });
        List<ColumnMetadata> expectedColumns = Arrays.asList(
                new ColumnMetadata("id", ColumnType.INT, null, false),
                new ColumnMetadata("double_col", ColumnType.DOUBLE, null, true),
                new ColumnMetadata("float_col", ColumnType.FLOAT, null, true),
                new ColumnMetadata("varchar_col", ColumnType.VARCHAR, 36, true),
                new ColumnMetadata("boolean_col", ColumnType.BOOLEAN, null, true),
                new ColumnMetadata("int_col", ColumnType.INT, null, true),
                new ColumnMetadata("bigint_col", ColumnType.BIGINT, null, true),
                new ColumnMetadata("date_col", ColumnType.DATE, null, true),
                new ColumnMetadata("timestamp_col", ColumnType.TIMESTAMP, 6, true),
                new ColumnMetadata("time_col", ColumnType.TIME, 5, true),
                new ColumnMetadata("uuid_col", ColumnType.UUID, 36, true),
                new ColumnMetadata("char_col", ColumnType.CHAR, 10, true),
                new ColumnMetadata("int32_col", ColumnType.INT32, null, true),
                new ColumnMetadata("link_col", ColumnType.LINK, -1, true));
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        service.getColumnMetadata(new QueryParserRequest(sqlNode, datamarts))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals(expectedColumns, result);
                    testContext.completeNow();
                })));
    }

    @Test
    void shouldPickVarcharWhenCastInLogicalType(VertxTestContext testContext) throws JsonProcessingException {
        val sql = "select cast(link_col as VARCHAR) as link_col from dml.accounts";
        val datamarts = CoreSerialization.mapper()
                .readValue(loadTextFromFile("schema/dml_all_types.json"), new TypeReference<List<Datamart>>() {
                });
        List<ColumnMetadata> expectedColumns = Arrays.asList(
                new ColumnMetadata("link_col", ColumnType.VARCHAR, -1, true));
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        service.getColumnMetadata(new QueryParserRequest(sqlNode, datamarts))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals(expectedColumns, result);
                    testContext.completeNow();
                })));
    }

    @Test
    void shouldCorrectlyReturnLogicalTypesWhenJoin(VertxTestContext testContext) throws JsonProcessingException {
        val sql = "select a.link_col as link_col1, b.link_col as link_col2 from dml.accounts a join dml.accounts b using (id)";
        val datamarts = CoreSerialization.mapper()
                .readValue(loadTextFromFile("schema/dml_all_types.json"), new TypeReference<List<Datamart>>() {
                });
        List<ColumnMetadata> expectedColumns = Arrays.asList(
                new ColumnMetadata("link_col1", ColumnType.LINK, -1, true),
                new ColumnMetadata("link_col2", ColumnType.LINK, -1, true)
        );
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        service.getColumnMetadata(new QueryParserRequest(sqlNode, datamarts))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals(expectedColumns, result);
                    testContext.completeNow();
                })));
    }
}
