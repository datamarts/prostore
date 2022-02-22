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
package ru.datamart.prostore.query.execution.plugin.adqm.enrichment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adqm.base.factory.AdqmHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.factory.AdqmCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.factory.AdqmSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.service.AdqmCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.service.AdqmCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmDmlQueryExtendService;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmQueryGenerator;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmSchemaExtender;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckService;
import ru.datamart.prostore.query.execution.plugin.adqm.utils.DeltaTestUtils;
import ru.datamart.prostore.query.execution.plugin.adqm.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.datamart.prostore.query.execution.plugin.adqm.utils.TestUtils.assertNormalizedEquals;

@Slf4j
@ExtendWith(VertxExtension.class)
class AdqmQueryEnrichmentServiceImplTest {
    private static final String ENV_NAME = "local";
    private static Map<String, String> EXPECTED_SQLS;

    private QueryParserService queryParserService;
    private QueryEnrichmentService enrichService;
    private List<Datamart> datamarts;


    @BeforeAll
    static void loadFiles() throws JsonProcessingException {
        EXPECTED_SQLS = Collections.unmodifiableMap(CoreSerialization.mapper()
                .readValue(loadTextFromFile("sql/expectedDmlSqls.json"), new TypeReference<Map<String, String>>() {
                }));
    }

    @BeforeEach
    void setUp(Vertx vertx) throws JsonProcessingException {
        val parserConfig = TestUtils.CALCITE_CONFIGURATION.configDdlParser(
                TestUtils.CALCITE_CONFIGURATION.ddlParserImplFactory());
        val contextProvider = new AdqmCalciteContextProvider(
                parserConfig,
                new AdqmCalciteSchemaFactory(new AdqmSchemaFactory()));

        queryParserService = new AdqmCalciteDMLQueryParserService(contextProvider, vertx);
        val helperTableNamesFactory = new AdqmHelperTableNamesFactory();
        val queryExtendService = new AdqmDmlQueryExtendService(helperTableNamesFactory);
        val sqlDialect = TestUtils.CALCITE_CONFIGURATION.adqmSqlDialect();
        val relToSqlConverter = new DtmRelToSqlConverter(sqlDialect, false);

        AdqmQueryJoinConditionsCheckService conditionsCheckService = mock(AdqmQueryJoinConditionsCheckService.class);
        when(conditionsCheckService.isJoinConditionsCorrect(any())).thenReturn(true);
        enrichService = new AdqmQueryEnrichmentService(
                queryParserService,
                contextProvider,
                new AdqmQueryGenerator(queryExtendService,
                        sqlDialect, relToSqlConverter),
                new AdqmSchemaExtender(helperTableNamesFactory));

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
    void enrichWithDeltaNum(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum"));
    }

    @Test
    void shouldBeShardedWhenLocal(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));
        enrichRequest.setLocal(true);

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("shouldBeShardedWhenLocal"));
    }

    @Test
    void enrichWithDeltaFinishedIn(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaFinishedIn(1, 1),
                        DeltaTestUtils.deltaFinishedIn(2, 2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaFinishedIn"));
    }

    @Test
    void enrichWithDeltaStartedIn(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaStartedIn(1, 1),
                        DeltaTestUtils.deltaStartedIn(2, 2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaStartedIn"));
    }

    @Test
    void enrichWithDeltaOnDate(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaOnDate(1),
                        DeltaTestUtils.deltaOnDate(2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaOnDate"));
    }

    @Test
    void enrichWithWithoutDelta(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaWithout(1),
                        DeltaTestUtils.deltaWithout(2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithWithoutDelta"));
    }

    @Test
    void enrichWithDeltaNum2(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum2"));
    }

    @Test
    void enrichWithDeltaNum3(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum3"));
    }

    @Test
    void enrichWithDeltaNum4(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas("SELECT * FROM shares.transactions as tran",
                Arrays.asList(DeltaTestUtils.deltaNum(1)));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum4"));
    }

    @Test
    void enrichWithDeltaNum5(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a1.account_id\n" +
                        "FROM (SELECT a2.account_id FROM shares.accounts a2 where a2.account_id = 12) a1\n" +
                        "    INNER JOIN shares.transactions t1 ON a1.account_id = t1.account_id",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum5"));
    }

    @Test
    void enrichWithDeltaNum6(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id " +
                        "LIMIT 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum6"));
    }

    @Test
    void enrichCount(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas("SELECT count(*) FROM shares.accounts",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichCount"));
    }

    @Test
    void enrichWithDeltaNum9(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas("SELECT * FROM shares.transactions where account_id = 1",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithDeltaNum9"));
    }

    @Test
    void enrichWithAggregate(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas("SELECT min(int_col) as min_col, min(double_col) as max_col, varchar_col\n" +
                        "FROM dml.AGGREGATION_TABLE\n" +
                        "group by varchar_col\n" +
                        "order by varchar_col\n" +
                        "limit 2",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithAggregate"));
    }

    @Test
    void enrichWithAggregate2(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas("SELECT min(int_col) as min_col, min(double_col) as max_col, varchar_col, NULL as t1\n" +
                        "FROM dml.AGGREGATION_TABLE\n" +
                        "where varchar_col = 'ф'\n" +
                        "group by varchar_col\n" +
                        "limit 2",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithAggregate2"));
    }

    @Test
    void enrichWithSort(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT COUNT(c.category_name),\n" +
                        "       c.category_name,\n" +
                        "       sum(p.units_in_stock),\n" +
                        "       c.id\n" +
                        "FROM dml.products p\n" +
                        "         JOIN dml.categories c on p.category_id = c.id\n" +
                        "GROUP BY c.category_name, c.id\n" +
                        "ORDER BY c.id\n" +
                        "limit 5",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithSort"));
    }

    @Test
    void testEnrichWithUnions(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestWithDeltas("select account_id\n" +
                                "from (select * from (select * from shares.accounts order by account_id limit 1)\n" +
                                "         union all\n" +
                                "         select * from shares.accounts)",
                        Arrays.asList(
                                DeltaTestUtils.deltaNum(1),
                                DeltaTestUtils.deltaNum(1)
                        ));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id FROM (SELECT account_id FROM (SELECT account_id, account_type, sys_op, sys_to, sys_from, sign, sys_close_date FROM (SELECT account_id, account_type, sys_op, sys_to, sys_from, sign, sys_close_date FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND sys_to >= 1 ORDER BY account_id NULLS LAST LIMIT 1) AS t2 UNION ALL SELECT account_id, account_type, sys_op, sys_to, sys_from, sign, sys_close_date FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t6) AS t7 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT account_id FROM (SELECT account_id FROM (SELECT account_id, account_type, sys_op, sys_to, sys_from, sign, sys_close_date FROM (SELECT account_id, account_type, sys_op, sys_to, sys_from, sign, sys_close_date FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1 ORDER BY account_id NULLS LAST LIMIT 1) AS t17 UNION ALL SELECT account_id, account_type, sys_op, sys_to, sys_from, sign, sys_close_date FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t21) AS t22 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void testEnrichWithUnions2(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestWithDeltas("select account_id\n" +
                                "from (select account_id from (select account_id from shares.accounts order by account_id limit 1) where account_id = 0\n" +
                                "         union all\n" +
                                "         select account_id from shares.accounts)",
                        Arrays.asList(
                                DeltaTestUtils.deltaNum(1),
                                DeltaTestUtils.deltaNum(1)
                        ));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id FROM (SELECT account_id FROM (SELECT account_id FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND sys_to >= 1 ORDER BY account_id NULLS LAST LIMIT 1) AS t3 WHERE account_id = 0 UNION ALL SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t9 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT account_id FROM (SELECT account_id FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1 ORDER BY account_id NULLS LAST LIMIT 1) AS t20 WHERE account_id = 0 UNION ALL SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t26 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void testEnrichWithUnions3(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestWithDeltas("select account_id\n" +
                                "from (select account_id from (select account_id from shares.accounts where account_id = 0 order by account_id limit 1)\n" +
                                "         union all\n" +
                                "         select account_id from shares.accounts)",
                        Arrays.asList(
                                DeltaTestUtils.deltaNum(1),
                                DeltaTestUtils.deltaNum(1)
                        ));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id FROM (SELECT account_id FROM (SELECT account_id FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 0) ORDER BY account_id NULLS LAST LIMIT 1) AS t3 UNION ALL SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t8 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT account_id FROM (SELECT account_id FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 0) ORDER BY account_id NULLS LAST LIMIT 1) AS t19 UNION ALL SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t24 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void enrichWithSort3(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT COUNT(dml.categories.category_name),\n" +
                        "       dml.categories.category_name,\n" +
                        "       dml.categories.id,\n" +
                        "       sum(dml.products.units_in_stock)\n" +
                        "FROM dml.products\n" +
                        "         INNER JOIN dml.categories on dml.products.category_id = dml.categories.id\n" +
                        "GROUP BY dml.categories.category_name, dml.categories.id\n" +
                        "ORDER BY dml.categories.id limit 5",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithSort3"));
    }

    @Test
    void enrichWithSort4(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT COUNT(dml.categories.category_name),\n" +
                        "       sum(dml.products.units_in_stock)\n" +
                        "FROM dml.products\n" +
                        "         INNER JOIN dml.categories on dml.products.category_id = dml.categories.id\n" +
                        "GROUP BY dml.categories.category_name, dml.categories.id\n" +
                        "ORDER BY dml.categories.id limit 5",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithSort4"));
    }

    @Test
    void enrichWithSort5(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT c.id \n" +
                        "FROM dml.products p\n" +
                        "         JOIN dml.categories c on p.category_id = c.id\n" +
                        "ORDER BY c.id",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithSort5"));
    }

    @Test
    void enrichWithSort6(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT *\n" +
                        "from dml.categories c\n" +
                        "         JOIN (select * from  dml.products) p on c.id = p.category_id\n" +
                        "ORDER by c.id, p.product_name desc",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichRequest, EXPECTED_SQLS.get("enrichWithSort6"));
    }

    @Test
    void enrichWithManyInKeyword(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestWithDeltas("SELECT account_id FROM shares.accounts WHERE account_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)",
                Arrays.asList(DeltaTestUtils.deltaNum(1)));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, EXPECTED_SQLS.get("enrichWithManyInKeyword"));
    }

    @Test
    void enrichWithCustomSelect(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestWithDeltas("SELECT *, 0 FROM shares.accounts ORDER BY account_id",
                Arrays.asList(DeltaTestUtils.deltaNum(1)));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, EXPECTED_SQLS.get("enrichWithCustomSelect"));
    }

    @Test
    void enrichWithSubquery(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestWithDeltas("SELECT * FROM shares.accounts as b where b.account_id IN (select c.account_id from shares.transactions as c limit 1)",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(2),
                        DeltaTestUtils.deltaNum(1)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, EXPECTED_SQLS.get("enrichWithSubquery"));
    }

    @Test
    void enrichWithSubqueryWithJoin(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestWithDeltas("SELECT * FROM shares.accounts as b JOIN shares.transactions as c ON b.account_id=c.account_id WHERE b.account_id IN (select c.account_id from shares.transactions as c limit 1)",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(3),
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(2)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id, account_type, transaction_id, transaction_date, account_id0, amount FROM (SELECT account_id, account_type, sys_op, sys_to, sys_from, sign, sys_close_date, transaction_id, transaction_date, account_id0, amount, sys_op0, sys_to0, sys_from0, sign0, sys_close_date0 FROM (SELECT accounts_actual.account_id, accounts_actual.account_type, accounts_actual.sys_op, accounts_actual.sys_to, accounts_actual.sys_from, accounts_actual.sign, accounts_actual.sys_close_date, t0.transaction_id, t0.transaction_date, t0.account_id AS account_id0, t0.amount, t0.sys_op AS sys_op0, t0.sys_to AS sys_to0, t0.sys_from AS sys_from0, t0.sign AS sign0, t0.sys_close_date AS sys_close_date0 FROM local__shares.accounts_actual FINAL INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount, sys_op, sys_to, sys_from, sign, sys_close_date FROM local__shares.transactions_actual_shard FINAL WHERE sys_from <= 2 AND sys_to >= 2) AS t0 ON accounts_actual.account_id = t0.account_id WHERE accounts_actual.account_id IN (SELECT account_id FROM local__shares.transactions_actual_shard FINAL WHERE sys_from <= 3 AND sys_to >= 3 LIMIT 1) AND (accounts_actual.sys_from <= 1 AND accounts_actual.sys_to >= 1)) AS t6 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL OR (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT account_id, account_type, sys_op, sys_to, sys_from, sign, sys_close_date, transaction_id, transaction_date, account_id0, amount, sys_op0, sys_to0, sys_from0, sign0, sys_close_date0 FROM (SELECT accounts_actual1.account_id, accounts_actual1.account_type, accounts_actual1.sys_op, accounts_actual1.sys_to, accounts_actual1.sys_from, accounts_actual1.sign, accounts_actual1.sys_close_date, t18.transaction_id, t18.transaction_date, t18.account_id AS account_id0, t18.amount, t18.sys_op AS sys_op0, t18.sys_to AS sys_to0, t18.sys_from AS sys_from0, t18.sign AS sign0, t18.sys_close_date AS sys_close_date0 FROM local__shares.accounts_actual AS accounts_actual1 INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount, sys_op, sys_to, sys_from, sign, sys_close_date FROM local__shares.transactions_actual_shard WHERE sys_from <= 2 AND sys_to >= 2) AS t18 ON accounts_actual1.account_id = t18.account_id WHERE accounts_actual1.account_id IN (SELECT account_id FROM local__shares.transactions_actual_shard WHERE sys_from <= 3 AND sys_to >= 3 LIMIT 1) AND (accounts_actual1.sys_from <= 1 AND accounts_actual1.sys_to >= 1)) AS t24 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL AND (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL) AS t35");
    }

    @Test
    void enrichWithSubqueryInJoin(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestWithDeltas("SELECT * FROM shares.accounts as b JOIN (select c.account_id from shares.transactions as c) t ON b.account_id=t.account_id WHERE b.account_id > 0 LIMIT 1",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(2)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, EXPECTED_SQLS.get("enrichWithSubqueryInJoin"));
    }

    @Test
    void enrichWithAliasesAndFunctions(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestWithDeltas("SELECT *\n" +
                        "FROM (SELECT\n" +
                        "          account_id      as bc1,\n" +
                        "          true            as bc2,\n" +
                        "          abs(account_id) as bc3,\n" +
                        "          'some$' as bc4,\n" +
                        "          '$some'\n" +
                        "      FROM shares.accounts) t1\n" +
                        "               JOIN shares.transactions as t2\n" +
                        "                    ON t1.bc1 = t2.account_id AND abs(t2.account_id) = t1.bc3\n" +
                        "WHERE t1.bc2 = true AND t1.bc3 = 0",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(2),
                        DeltaTestUtils.deltaNum(1)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, EXPECTED_SQLS.get("enrichWithAliasesAndFunctions"));
    }

    @Test
    void shouldFailWhenNotEnoughDeltas(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        queryParserService.parse(new QueryParserRequest(enrichRequest.getQuery(), enrichRequest.getSchema()))
                .compose(parserResponse -> enrichService.enrich(enrichRequest, parserResponse))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        Assertions.fail("Unexpected success");
                    }

                    Assertions.assertSame(DataSourceException.class, ar.cause().getClass());
                }).completeNow());
    }

    @Test
    void shouldSucceedWhenNotNullableColumnFilteredByIsNull(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichRequest = prepareRequestWithDeltas("SELECT count(*) FROM shares.accounts WHERE account_id is null",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequest, "SELECT COUNT(*) AS EXPR__0 FROM (SELECT __f0 FROM (SELECT 0 AS __f0 FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id IS NULL)) AS t1 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT __f0 FROM (SELECT 0 AS __f0 FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id IS NULL)) AS t10 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL) AS t17");
    }

    private void enrichAndAssert(VertxTestContext testContext, EnrichQueryRequest enrichRequest,
                                 String expectedSql) {
        queryParserService.parse(new QueryParserRequest(enrichRequest.getQuery(), enrichRequest.getSchema()))
                .compose(parserResponse -> enrichService.enrich(enrichRequest, parserResponse))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        Assertions.fail(ar.cause());
                    }

                    assertNormalizedEquals(ar.result(), expectedSql);
                }).completeNow());
    }

    private EnrichQueryRequest prepareRequestWithDeltas(String sql, List<DeltaInformation> deltaInformations) {
        return EnrichQueryRequest.builder()
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .deltaInformations(deltaInformations)
                .envName(ENV_NAME)
                .schema(datamarts)
                .build();
    }
}
