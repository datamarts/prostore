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
package ru.datamart.prostore.query.execution.plugin.adqm.mppr.shuttle;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adqm.base.factory.AdqmHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.factory.AdqmCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.factory.AdqmSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.service.AdqmCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.service.AdqmCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmSchemaExtender;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckService;
import ru.datamart.prostore.query.execution.plugin.adqm.utils.TestUtils;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class SimpleQueryRelCheckerTest {
    private static final boolean EXPECTED_SIMPLE = true;
    private static final boolean EXPECTED_COMPLEX = false;
    private static final String ENV = "env";

    private QueryParserService queryParserService;
    private List<Datamart> datamarts;
    private SimpleQueryRelChecker simpleQueryRelChecker = new SimpleQueryRelChecker();

    @BeforeEach
    void setUp(Vertx vertx) throws JsonProcessingException {
        val parserConfig = TestUtils.CALCITE_CONFIGURATION.configDdlParser(
                TestUtils.CALCITE_CONFIGURATION.ddlParserImplFactory());
        val contextProvider = new AdqmCalciteContextProvider(
                parserConfig,
                new AdqmCalciteSchemaFactory(new AdqmSchemaFactory()));

        val schemaExtender = new AdqmSchemaExtender(new AdqmHelperTableNamesFactory());
        queryParserService = new AdqmCalciteDMLQueryParserService(contextProvider, vertx, schemaExtender);

        AdqmQueryJoinConditionsCheckService conditionsCheckService = mock(AdqmQueryJoinConditionsCheckService.class);
        when(conditionsCheckService.isJoinConditionsCorrect(any())).thenReturn(EXPECTED_SIMPLE);

        datamarts = CoreSerialization.mapper()
                .readValue(loadTextFromFile("schema/dml.json"), new TypeReference<List<Datamart>>() {
                });
    }

    @SneakyThrows
    private static String loadTextFromFile(String path) {
        try (InputStream inputStream = AdqmQueryEnrichmentService.class.getClassLoader().getResourceAsStream(path)) {
            assert inputStream != null;
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }

    @Test
    void shouldBeSimple_whenSelect(VertxTestContext testContext) {
        // arrange
        String sql = "SELECT a.account_id FROM shares.accounts a";

        // act assert
        enrichAndAssert(testContext, sql, EXPECTED_SIMPLE);
    }

    @Test
    void shouldBeSimple_whenSelectWithWhere(VertxTestContext testContext) {
        // arrange
        String sql = "SELECT a.account_id FROM shares.accounts a WHERE a.account_id > 0";

        // act assert
        enrichAndAssert(testContext, sql, EXPECTED_SIMPLE);
    }

    @Test
    void shouldBeSimple_whenSelectWithSimpleSubquery(VertxTestContext testContext) {
        // arrange
        String sql = "SELECT * FROM (SELECT account_id FROM shares.accounts a)";

        // act assert
        enrichAndAssert(testContext, sql, EXPECTED_SIMPLE);
    }

    @Test
    void shouldBeComplex_whenSelectWithMultipleWhere(VertxTestContext testContext) {
        // arrange
        String sql = "SELECT account_id FROM (SELECT account_id FROM shares.accounts a WHERE a.account_id > 0) b where account_id > 0";

        // act assert
        enrichAndAssert(testContext, sql, EXPECTED_COMPLEX);
    }

    @Test
    void shouldBeComplex_whenSelectWithAggregate(VertxTestContext testContext) {
        // arrange
        String sql = "SELECT count(*) FROM shares.accounts a";

        // act assert
        enrichAndAssert(testContext, sql, EXPECTED_COMPLEX);
    }

    @Test
    void shouldBeComplex_whenSelectWithSubquery(VertxTestContext testContext) {
        // arrange
        String sql = "SELECT * FROM shares.accounts a WHERE account_id in (SELECT account_id FROM shares.accounts)";

        // act assert
        enrichAndAssert(testContext, sql, EXPECTED_COMPLEX);
    }

    @Test
    void shouldBeComplex_whenSelectWithSubquery2(VertxTestContext testContext) {
        // arrange
        String sql = "SELECT *, (SELECT count(account_id) FROM shares.accounts) FROM shares.accounts a";

        // act assert
        enrichAndAssert(testContext, sql, EXPECTED_COMPLEX);
    }

    @Test
    void shouldBeComplex_whenSelectWithGroupBy(VertxTestContext testContext) {
        // arrange
        String sql = "SELECT account_id FROM shares.accounts a GROUP BY account_id";

        // act assert
        enrichAndAssert(testContext, sql, EXPECTED_COMPLEX);
    }

    @Test
    void shouldBeComplex_whenSelectWithGroupByAndAggregate(VertxTestContext testContext) {
        // arrange
        String sql = "SELECT account_id, count(*) FROM shares.accounts a GROUP BY account_id";

        // act assert
        enrichAndAssert(testContext, sql, EXPECTED_COMPLEX);
    }

    @Test
    void shouldBeComplex_whenJoin(VertxTestContext testContext) {
        // arrange
        String sql = "SELECT a.account_id FROM shares.accounts a" +
                " join shares.transactions t on t.account_id = a.account_id" +
                " where a.account_id = 10";

        // act assert
        enrichAndAssert(testContext, sql, EXPECTED_COMPLEX);
    }

    private void enrichAndAssert(VertxTestContext testContext, String sql, boolean expectedResult) {
        queryParserService.parse(new QueryParserRequest(TestUtils.DEFINITION_SERVICE.processingQuery(sql), datamarts, ENV))
                .map(queryParserResponse -> {
                    queryParserResponse.getRelNode().rel.accept(simpleQueryRelChecker);
                    return simpleQueryRelChecker.isSimple();
                })
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        Assertions.fail(ar.cause());
                    }

                    Assertions.assertEquals(expectedResult, ar.result());
                }).completeNow());
    }
}