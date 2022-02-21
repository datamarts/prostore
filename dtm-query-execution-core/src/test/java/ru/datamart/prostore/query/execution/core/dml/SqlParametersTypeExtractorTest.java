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

import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.core.calcite.factory.CoreSchemaFactory;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteContextProvider;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.core.dml.service.SqlParametersTypeExtractor;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static ru.datamart.prostore.query.execution.core.utils.TestUtils.loadTextFromFile;

@Slf4j
@ExtendWith(VertxExtension.class)
class SqlParametersTypeExtractorTest {

    private static final String SQL_WHERE_BETWEEN = "SELECT * FROM all_types_table\n" +
            "WHERE date_col = ?\n" +
            "AND timestamp_col > ?\n" +
            "AND int_col BETWEEN ? AND ?";
    private static final String SQL_WHERE_IN = "SELECT * FROM all_types_table " +
            "WHERE time_col IN (?, ?, ?)";
    private static final String SQL_CASE = "SELECT date_col,\n" +
            "CASE\n" +
            "when id = ? THEN 'case 1'\n" +
            "ELSE 'case other'\n" +
            "END AS id_case\n" +
            "FROM all_types_table";
    private static final String SQL_JOIN = "SELECT a1.date_col, a2.int_col FROM accounts a1\n" +
            "JOIN all_types_table a2 ON a1.id = a2.id AND a1.date_col = ?\n" +
            "WHERE a2.timestamp_col > ?";

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlParser.Config configParser = calciteConfiguration.configEddlParser(calciteConfiguration.getSqlParserFactory());
    private final CoreCalciteSchemaFactory calciteSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CoreCalciteContextProvider calciteContextProvider = new CoreCalciteContextProvider(configParser, calciteSchemaFactory);
    private final QueryParserService parserService = new CoreCalciteDMLQueryParserService(calciteContextProvider, Vertx.vertx());
    private final SqlParametersTypeExtractor parametersTypeExtractor = new SqlParametersTypeExtractor();

    @Test
    void testExtractWhereBetween(VertxTestContext testContext) {
        List<SqlTypeName> expectedResult = Arrays.asList(SqlTypeName.DATE, SqlTypeName.TIMESTAMP, SqlTypeName.BIGINT, SqlTypeName.BIGINT);
        test(SQL_WHERE_BETWEEN, expectedResult, testContext);
    }

    @Test
    void testExtractWhereIn(VertxTestContext testContext) {
        List<SqlTypeName> expectedResult = Arrays.asList(SqlTypeName.TIME, SqlTypeName.TIME, SqlTypeName.TIME);
        test(SQL_WHERE_IN, expectedResult, testContext);
    }

    @Test
    void testExtractCase(VertxTestContext testContext) {
        List<SqlTypeName> expectedResult = Collections.singletonList(SqlTypeName.BIGINT);
        test(SQL_CASE, expectedResult, testContext);
    }

    @Test
    void testExtractJoin(VertxTestContext testContext) {
        List<SqlTypeName> expectedResult = Arrays.asList(SqlTypeName.DATE, SqlTypeName.TIMESTAMP);
        test(SQL_JOIN, expectedResult, testContext);
    }

    @SneakyThrows
    private void test(String sql, List<SqlTypeName> expectedResult, VertxTestContext testContext) {
        val datamarts = CoreSerialization.mapper()
                .readValue(loadTextFromFile("schema/type_extractor_schema.json"), new TypeReference<List<Datamart>>() {
                });
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val parserRequest = new QueryParserRequest(sqlNode, datamarts);
        parserService.parse(parserRequest)
                .map(response -> parametersTypeExtractor.extract(response.getRelNode().rel))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    log.info("Result columns: {}", ar.result());
                    assertEquals(expectedResult, ar.result());
                }).completeNow());
    }
}
