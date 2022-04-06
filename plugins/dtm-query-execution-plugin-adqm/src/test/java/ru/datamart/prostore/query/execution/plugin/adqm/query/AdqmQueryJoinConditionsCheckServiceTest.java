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
package ru.datamart.prostore.query.execution.plugin.adqm.query;

import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.calcite.adqm.configuration.AdqmCalciteConfiguration;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adqm.base.factory.AdqmHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.factory.AdqmCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.factory.AdqmSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.service.AdqmCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.service.AdqmCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmSchemaExtender;
import ru.datamart.prostore.query.execution.plugin.adqm.query.dto.AdqmCheckJoinRequest;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckService;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.extractor.SqlJoinConditionExtractor;
import ru.datamart.prostore.query.execution.plugin.adqm.utils.TestUtils;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@ExtendWith(VertxExtension.class)
class AdqmQueryJoinConditionsCheckServiceTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final AdqmCalciteConfiguration calciteCoreConfiguration = new AdqmCalciteConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
            calciteCoreConfiguration.eddlParserImplFactory()
    );
    private final AdqmCalciteSchemaFactory calciteSchemaFactory = new AdqmCalciteSchemaFactory(new AdqmSchemaFactory());
    private final AdqmCalciteContextProvider calciteContextProvider = new AdqmCalciteContextProvider(parserConfig, calciteSchemaFactory);
    private final SqlJoinConditionExtractor joinConditionExtractor = new SqlJoinConditionExtractor();
    private AdqmQueryJoinConditionsCheckService conditionsCheckService;
    private QueryParserService parserService;

    @BeforeEach
    void setUp(Vertx vertx) {
        val schemaExtender = new AdqmSchemaExtender(new AdqmHelperTableNamesFactory());
        parserService = new AdqmCalciteDMLQueryParserService(calciteContextProvider, vertx, schemaExtender);
        conditionsCheckService = new AdqmQueryJoinConditionsCheckService(joinConditionExtractor);
    }

    @Test
    void trueWhenSubquerySchemaAndSingleDistrKey(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM (\n" +
                "SELECT * FROM dml.products p\n" +
                "    INNER JOIN dml.categories c on p.category_id = c.id\n" +
                "WHERE p.category_id > 5)", testContext, true);
    }

    @Test
    void trueWhenNoJoins(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products p\n" +
                "WHERE p.category_id > 5", testContext, true);
    }

    @Test
    void trueWhenSingleDistrKey(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products p \n" +
                "    INNER JOIN dml.categories c on p.category_id = c.id\n" +
                "WHERE p.category_id > 5", testContext, true);
    }

    @Test
    void trueWhenSubqueryWithAliasesSchemaAndSingleDistrKey(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products p \n" +
                "    INNER JOIN (SELECT *, c.id as aliasedId FROM dml.categories c) t on p.category_id = t.aliasedId\n" +
                "WHERE p.category_id > 5", testContext, true);
    }

    @Test
    void trueWhenSubqueryWithFunctionedIdSchemaAndSingleDistrKey(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products p \n" +
                "    INNER JOIN (SELECT *, ABS(c.id) as aliasedId FROM dml.categories c) t on p.category_id = t.aliasedId\n" +
                "WHERE p.category_id > 5", testContext, true);
    }

    @Test
    void trueWhenSubqueryWithProjectAndSingleDistrKey(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products p \n" +
                "    INNER JOIN (SELECT c.id FROM dml.categories c) t on p.category_id = t.id\n" +
                "WHERE p.category_id > 5", testContext, true);
    }

    @Test
    void trueWhenSingleDistrKeyAndLeftKeyIsFunction(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products p \n" +
                "    INNER JOIN dml.categories c on ABS(p.category_id) = c.id\n" +
                "WHERE p.category_id > 5", testContext, true);
    }

    @Test
    void trueWhenSingleDistrKeyAndRightKeyIsFunction(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products p \n" +
                "    INNER JOIN dml.categories c on p.category_id = ABS(c.id)\n" +
                "WHERE p.category_id > 5", testContext, true);
    }

    @Test
    void trueWhenSingleDistrKeyAndBothKeysIsFunction(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products p \n" +
                "    INNER JOIN dml.categories c on ABS(p.category_id) = ABS(c.id)\n" +
                "WHERE p.category_id > 5", testContext, true);
    }

    @Test
    void falseWhenMultipleConditionsWithOneAlias(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products\n" +
                "    INNER JOIN dml.categories c on dml.products.id = c.id\n" +
                "    AND dml.products.product_name = c.category_name\n" +
                "WHERE dml.products.category_id > 5\n" +
                "    limit 5", testContext, false);
    }

    @Test
    void falseWhenMultipleConditionsWithAlias(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products p\n" +
                "    INNER JOIN dml.categories c on p.id = c.id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", testContext, false);
    }

    @Test
    void falseWhenMultipleConditionsWithoutAliasWithLimit(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products\n" +
                "    INNER JOIN dml.categories on dml.products.id = dml.categories.id\n" +
                "    AND dml.products.product_name = dml.categories.category_name\n" +
                "WHERE dml.products.category_id > 5\n" +
                "    limit 5", testContext, false);
    }

    @Test
    void falseWhenMultipleConditionsButWithinSharding(VertxTestContext testContext) {
        // this test should return TRUE when we will support multiple conditional logic
        testExpectedResult("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.id = c.id\n" +
                "    AND p.distribution_id = c.distribution_id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", testContext, false);
    }

    @Test
    void falseWhenOneConditionInsteadOfTwoInShardingKey(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.id = c.id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", testContext, false);
    }

    @Test
    void falseWhenAllDistributedKeysButWithOr(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.id = c.id\n" +
                "    OR p.distribution_id = c.distribution_id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", testContext, false);
    }

    @Test
    void falseWhenMultipleDistributedKeysInWrongOrder(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml_2.products p\n" +
                "    INNER JOIN dml_2.categories c on p.id = c.distribution_id\n" +
                "    AND p.distribution_id = c.id\n" +
                "    AND p.product_name = c.category_name\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", testContext, false);
    }

    @Test
    void falseWhenUsingNotEquals(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM (\n" +
                "SELECT * FROM dml.products p\n" +
                "    INNER JOIN dml.categories c on p.category_id <> c.id\n" +
                "WHERE p.category_id > 5)", testContext, false);
    }

    @Test
    void falseWhenNotEquiCondition(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM (\n" +
                "SELECT * FROM dml.products p\n" +
                "    INNER JOIN dml.categories c on p.category_id > c.id\n" +
                "WHERE p.category_id > 5)", testContext, false);
    }

    @Test
    void falseWhenBetweenOperator(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM (\n" +
                "SELECT * FROM dml.products p\n" +
                "    INNER JOIN dml.categories c on p.id between '1' AND '2'\n" +
                "WHERE p.category_id > 5)", testContext, false);
    }

    @Test
    void falseWhenMultipleJoinsAndMultipleConditions(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM dml.products p\n" +
                "    INNER JOIN dml.categories c on p.id = c.id AND p.product_name = c.category_name\n" +
                "    INNER JOIN dml_2.products p2 ON p2.category_id = c.id\n" +
                "    INNER JOIN dml_2.categories c2 on c2.id = p2.id AND p2.distribution_id = c2.distribution_id\n" +
                "WHERE p.category_id > 5\n" +
                "    limit 5", testContext, false);
    }

    @Test
    void falseWhenJoiningSubquery(VertxTestContext testContext) {
        testFailedResult("SELECT * FROM dml.products p\n" +
                "    INNER JOIN (SELECT * FROM dml.categories c WHERE c.id in (1,2,3)) as s ON p.category_id = s.id\n" +
                "WHERE p.category_id > 5", testContext, DtmException.class);
    }

    @Test
    void falseWhenTwoLeftSharedKeys(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM (\n" +
                "SELECT * FROM twoLeftDistributed.products p\n" +
                "    INNER JOIN twoLeftDistributed.categories c on p.category_id = c.id\n" +
                "WHERE p.category_id > 5)", testContext, false);
    }

    @Test
    void falseWhenTwoRightSharedKeys(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM (\n" +
                "SELECT * FROM twoRightDistributed.products p\n" +
                "    INNER JOIN twoRightDistributed.categories c on p.category_id = c.id\n" +
                "WHERE p.category_id > 5)", testContext, false);
    }

    @Test
    void falseWhenTwoConditionOnLeftSharedKeys(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM (\n" +
                "SELECT * FROM twoDistributed.products p\n" +
                "    INNER JOIN twoDistributed.categories c on p.category_id + p.category_code = c.id\n" +
                "WHERE p.category_id > 5)", testContext, false);
    }

    @Test
    void falseWhenTwoConditionOnRightSharedKeys(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM (\n" +
                "SELECT * FROM twoDistributed.products p\n" +
                "    INNER JOIN twoDistributed.categories c on p.category_id = c.id + c.code\n" +
                "WHERE p.category_id > 5)", testContext, false);
    }

    @Test
    void falseWhenLeftConditionIsNotSharedKey(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM (\n" +
                "SELECT * FROM twoDistributed.products p\n" +
                "    INNER JOIN twoDistributed.categories c on p.category_code = c.id\n" +
                "WHERE p.category_id > 5)", testContext, false);
    }

    @Test
    void falseWhenRightConditionIsNotSharedKey(VertxTestContext testContext) {
        testExpectedResult("SELECT * FROM (\n" +
                "SELECT * FROM twoDistributed.products p\n" +
                "    INNER JOIN twoDistributed.categories c on p.category_id = c.code\n" +
                "WHERE p.category_id > 5)", testContext, false);
    }

    @SneakyThrows
    void testExpectedResult(String sql, VertxTestContext testContext, boolean expectedResult) {
        // act
        execute(sql).onComplete(ar -> testContext.verify(() -> {
            // assert
            if (ar.failed()) {
                Assertions.fail(ar.cause());
            }

            assertEquals(expectedResult, ar.result());
        }).completeNow());
    }

    @SneakyThrows
    void testFailedResult(String sql, VertxTestContext testContext, Class<? extends Throwable> expectedException) {
        // act
        execute(sql).onComplete(ar -> testContext.verify(() -> {
            // assert
            if (ar.succeeded()) {
                Assertions.fail("Unexpected success");
            }

            assertSame(expectedException, ar.cause().getClass());
        }).completeNow());
    }

    private Future<Boolean> execute(String sql) throws com.fasterxml.jackson.core.JsonProcessingException {
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val datamarts = CoreSerialization.mapper()
                .readValue(loadTextFromFile(), new TypeReference<List<Datamart>>() {
                });
        val parserRequest = new QueryParserRequest(sqlNode, datamarts, "env");
        return parserService.parse(parserRequest)
                .map(response -> conditionsCheckService.isJoinConditionsCorrect(new AdqmCheckJoinRequest(
                        response.getRelNode().rel, datamarts)));
    }

    @SneakyThrows
    String loadTextFromFile() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("schema/dml.json")) {
            assert inputStream != null;
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }
}