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
import io.vertx.core.Future;
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
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.exception.DtmException;
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

        val schemaExtender = new AdqmSchemaExtender(new AdqmHelperTableNamesFactory());
        queryParserService = new AdqmCalciteDMLQueryParserService(contextProvider, vertx, schemaExtender);
        val helperTableNamesFactory = new AdqmHelperTableNamesFactory();
        val queryExtendService = new AdqmDmlQueryExtendService(helperTableNamesFactory);
        val sqlDialect = TestUtils.CALCITE_CONFIGURATION.adqmSqlDialect();
        val relToSqlConverter = new DtmRelToSqlConverter(sqlDialect, false);

        AdqmQueryJoinConditionsCheckService conditionsCheckService = mock(AdqmQueryJoinConditionsCheckService.class);
        when(conditionsCheckService.isJoinConditionsCorrect(any())).thenReturn(true);
        enrichService = new AdqmQueryEnrichmentService(queryExtendService, sqlDialect, relToSqlConverter);

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
        val enrichRequestFuture = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithDeltaNum"));
    }

    @Test
    void shouldBeShardedWhenLocal(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ))
                .map(request -> {
                    request.setLocal(true);
                    return request;
                });

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("shouldBeShardedWhenLocal"));
    }

    @Test
    void enrichWithDeltaFinishedIn(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaFinishedIn(1, 1),
                        DeltaTestUtils.deltaFinishedIn(2, 2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithDeltaFinishedIn"));
    }

    @Test
    void enrichWithDeltaStartedIn(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaStartedIn(1, 1),
                        DeltaTestUtils.deltaStartedIn(2, 2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithDeltaStartedIn"));
    }

    @Test
    void enrichWithDeltaOnDate(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaOnDate(1),
                        DeltaTestUtils.deltaOnDate(2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithDeltaOnDate"));
    }

    @Test
    void enrichWithWithoutDelta(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaWithout(1),
                        DeltaTestUtils.deltaWithout(2)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithWithoutDelta"));
    }

    @Test
    void enrichWithDeltaNum2(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithDeltaNum2"));
    }

    @Test
    void enrichWithDeltaNum3(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
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
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithDeltaNum3"));
    }

    @Test
    void enrichWithDeltaNum4(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas("SELECT * FROM shares.transactions as tran",
                Arrays.asList(DeltaTestUtils.deltaNum(1)));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithDeltaNum4"));
    }

    @Test
    void enrichWithDeltaNum5(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
                "SELECT a1.account_id\n" +
                        "FROM (SELECT a2.account_id FROM shares.accounts a2 where a2.account_id = 12) a1\n" +
                        "    INNER JOIN shares.transactions t1 ON a1.account_id = t1.account_id",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithDeltaNum5"));
    }

    @Test
    void enrichWithDeltaNum6(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id " +
                        "LIMIT 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithDeltaNum6"));
    }

    @Test
    void enrichCount(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas("SELECT count(*) FROM shares.accounts",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichCount"));
    }

    @Test
    void enrichWithDeltaNum9(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas("SELECT * FROM shares.transactions where account_id = 1",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithDeltaNum9"));
    }

    @Test
    void enrichWithAggregate(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas("SELECT min(int_col) as min_col, min(double_col) as max_col, varchar_col\n" +
                        "FROM dml.AGGREGATION_TABLE\n" +
                        "group by varchar_col\n" +
                        "order by varchar_col\n" +
                        "limit 2",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithAggregate"));
    }

    @Test
    void enrichWithAggregate2(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas("SELECT min(int_col) as min_col, min(double_col) as max_col, varchar_col, NULL as t1\n" +
                        "FROM dml.AGGREGATION_TABLE\n" +
                        "where varchar_col = 'ф'\n" +
                        "group by varchar_col\n" +
                        "limit 2",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithAggregate2"));
    }

    @Test
    void enrichWithSort(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
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
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithSort"));
    }

    @Test
    void testEnrichWithUnions(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture =
                prepareRequestWithDeltas("select account_id\n" +
                                "from (select * from (select * from shares.accounts order by account_id limit 1)\n" +
                                "         union all\n" +
                                "         select * from shares.accounts)",
                        Arrays.asList(
                                DeltaTestUtils.deltaNum(1),
                                DeltaTestUtils.deltaNum(1)
                        ));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id FROM (SELECT account_id FROM (SELECT account_id, account_type FROM (SELECT account_id, account_type FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND sys_to >= 1 ORDER BY account_id NULLS LAST LIMIT 1) AS t3 UNION ALL SELECT account_id, account_type FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t8) AS t9 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT account_id FROM (SELECT account_id FROM (SELECT account_id, account_type FROM (SELECT account_id, account_type FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1 ORDER BY account_id NULLS LAST LIMIT 1) AS t20 UNION ALL SELECT account_id, account_type FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t25) AS t26 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void testEnrichWithUnions2(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture =
                prepareRequestWithDeltas("select account_id\n" +
                                "from (select account_id from (select account_id from shares.accounts order by account_id limit 1) where account_id = 0\n" +
                                "         union all\n" +
                                "         select account_id from shares.accounts)",
                        Arrays.asList(
                                DeltaTestUtils.deltaNum(1),
                                DeltaTestUtils.deltaNum(1)
                        ));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id FROM (SELECT account_id FROM (SELECT account_id FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND sys_to >= 1 ORDER BY account_id NULLS LAST LIMIT 1) AS t3 WHERE account_id = 0 UNION ALL SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t9 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT account_id FROM (SELECT account_id FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1 ORDER BY account_id NULLS LAST LIMIT 1) AS t20 WHERE account_id = 0 UNION ALL SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t26 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void testEnrichWithUnions3(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture =
                prepareRequestWithDeltas("select account_id\n" +
                                "from (select account_id from (select account_id from shares.accounts where account_id = 0 order by account_id limit 1)\n" +
                                "         union all\n" +
                                "         select account_id from shares.accounts)",
                        Arrays.asList(
                                DeltaTestUtils.deltaNum(1),
                                DeltaTestUtils.deltaNum(1)
                        ));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id FROM (SELECT account_id FROM (SELECT account_id FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 0) ORDER BY account_id NULLS LAST LIMIT 1) AS t3 UNION ALL SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t8 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT account_id FROM (SELECT account_id FROM (SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id = 0) ORDER BY account_id NULLS LAST LIMIT 1) AS t19 UNION ALL SELECT account_id FROM local__shares.accounts_actual WHERE sys_from <= 1 AND sys_to >= 1) AS t24 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void enrichWithSort3(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
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
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithSort3"));
    }

    @Test
    void enrichWithSort4(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
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
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithSort4"));
    }

    @Test
    void enrichWithSort5(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
                "SELECT c.id \n" +
                        "FROM dml.products p\n" +
                        "         JOIN dml.categories c on p.category_id = c.id\n" +
                        "ORDER BY c.id",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithSort5"));
    }

    @Test
    void enrichWithSort6(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
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
        enrichAndAssert(testContext, enrichRequestFuture, EXPECTED_SQLS.get("enrichWithSort6"));
    }

    @Test
    void enrichWithManyInKeyword(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT account_id FROM shares.accounts WHERE account_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)",
                Arrays.asList(DeltaTestUtils.deltaNum(1)));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, EXPECTED_SQLS.get("enrichWithManyInKeyword"));
    }

    @Test
    void enrichWithCustomSelect(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT *, 0 FROM shares.accounts ORDER BY account_id",
                Arrays.asList(DeltaTestUtils.deltaNum(1)));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, EXPECTED_SQLS.get("enrichWithCustomSelect"));
    }

    @Test
    void enrichWithSubquery(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM shares.accounts as b where b.account_id IN (select c.account_id from shares.transactions as c limit 1)",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(2),
                        DeltaTestUtils.deltaNum(1)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, EXPECTED_SQLS.get("enrichWithSubquery"));
    }

    @Test
    void enrichWithSubqueryWithJoin(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM shares.accounts as b JOIN shares.transactions as c ON b.account_id=c.account_id WHERE b.account_id IN (select c.account_id from shares.transactions as c limit 1)",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(3),
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(2)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id, account_type, transaction_id, transaction_date, account_id0, amount FROM (SELECT account_id, account_type, transaction_id, transaction_date, account_id0, amount FROM (SELECT accounts_actual.account_id, accounts_actual.account_type, t1.transaction_id, t1.transaction_date, t1.account_id AS account_id0, t1.amount FROM local__shares.accounts_actual FINAL INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM local__shares.transactions_actual_shard FINAL WHERE sys_from <= 2 AND sys_to >= 2) AS t1 ON accounts_actual.account_id = t1.account_id WHERE accounts_actual.sys_from <= 1 AND accounts_actual.sys_to >= 1) AS t4 WHERE t4.account_id IN (SELECT account_id FROM local__shares.transactions_actual_shard FINAL WHERE sys_from <= 3 AND sys_to >= 3 LIMIT 1)) AS t10 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL OR (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT account_id, account_type, transaction_id, transaction_date, account_id0, amount FROM (SELECT account_id, account_type, transaction_id, transaction_date, account_id0, amount FROM (SELECT accounts_actual1.account_id, accounts_actual1.account_type, t23.transaction_id, t23.transaction_date, t23.account_id AS account_id0, t23.amount FROM local__shares.accounts_actual AS accounts_actual1 INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM local__shares.transactions_actual_shard WHERE sys_from <= 2 AND sys_to >= 2) AS t23 ON accounts_actual1.account_id = t23.account_id WHERE accounts_actual1.sys_from <= 1 AND accounts_actual1.sys_to >= 1) AS t26 WHERE t26.account_id IN (SELECT account_id FROM local__shares.transactions_actual_shard WHERE sys_from <= 3 AND sys_to >= 3 LIMIT 1)) AS t32 WHERE (((SELECT 1 AS r FROM local__shares.transactions_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL AND (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void enrichWithSubqueryInJoin(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM shares.accounts as b JOIN (select c.account_id from shares.transactions as c) t ON b.account_id=t.account_id WHERE b.account_id > 0 LIMIT 1",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1),
                        DeltaTestUtils.deltaNum(2)
                )
        );

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, EXPECTED_SQLS.get("enrichWithSubqueryInJoin"));
    }

    @Test
    void enrichWithAliasesAndFunctions(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT *\n" +
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
        enrichAndAssert(testContext, enrichQueryRequestFuture, EXPECTED_SQLS.get("enrichWithAliasesAndFunctions"));
    }

    @Test
    void shouldFailWhenNotEnoughDeltas(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas(
                "SELECT a.account_id FROM shares.accounts a" +
                        " join shares.transactions t on t.account_id = a.account_id" +
                        " where a.account_id = 10",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichRequestFuture.compose(request -> enrichService.enrich(request))
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
        val enrichRequestFuture = prepareRequestWithDeltas("SELECT count(*) FROM shares.accounts WHERE account_id is null",
                Arrays.asList(
                        DeltaTestUtils.deltaNum(1)
                ));

        // act assert
        enrichAndAssert(testContext, enrichRequestFuture, "SELECT COUNT(*) AS EXPR__0 FROM (SELECT __f0 FROM (SELECT 0 AS __f0 FROM local__shares.accounts_actual FINAL WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id IS NULL)) AS t1 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT __f0 FROM (SELECT 0 AS __f0 FROM local__shares.accounts_actual WHERE sys_from <= 1 AND (sys_to >= 1 AND account_id IS NULL)) AS t10 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL) AS t17");
    }

    @Test
    void testFailEnrichWithWritableStandaloneTable(VertxTestContext testContext) {
        // arrange
        val enrichRequestFuture = prepareRequestWithDeltas("SELECT * FROM standalone_writable a WHERE a.id > 0",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build()));

        // act assert
        enrichRequestFuture.compose(enrichService::enrich)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        Assertions.fail("Unexpected success");
                    }

                    Assertions.assertSame(DtmException.class, ar.cause().getClass());
                }).completeNow());
    }

    @Test
    void testEnrichWithStandaloneTable(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM standalone a WHERE a.id > 0",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT id, name FROM schema.tbl WHERE id > 0");
    }

    @Test
    void testEnrichJoinStandaloneTables(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM standalone a JOIN standalone b ON a.id = b.id WHERE a.id > 0",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT tbl.id, tbl.name, tbl_shard.id AS id0, tbl_shard.name AS name0 FROM schema.tbl INNER JOIN schema.tbl_shard ON tbl.id = tbl_shard.id WHERE tbl.id > 0");
    }

    @Test
    void testEnrichJoinStandaloneAndLogical(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM shares.standalone a JOIN shares.accounts b ON a.id = b.account_id WHERE a.id > 0",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT id, name, account_id, account_type FROM (SELECT id, name, account_id, account_type FROM (SELECT tbl.id, tbl.name, accounts_actual_shard.account_id, accounts_actual_shard.account_type FROM schema.tbl INNER JOIN local__shares.accounts_actual_shard FINAL ON tbl.id = accounts_actual_shard.account_id WHERE accounts_actual_shard.sys_from <= 1 AND accounts_actual_shard.sys_to >= 1) AS t1 WHERE t1.id > 0) AS t3 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT id, name, account_id, account_type FROM (SELECT id, name, account_id, account_type FROM (SELECT tbl0.id, tbl0.name, accounts_actual_shard1.account_id, accounts_actual_shard1.account_type FROM schema.tbl AS tbl0 INNER JOIN local__shares.accounts_actual_shard AS accounts_actual_shard1 ON tbl0.id = accounts_actual_shard1.account_id WHERE accounts_actual_shard1.sys_from <= 1 AND accounts_actual_shard1.sys_to >= 1) AS t12 WHERE t12.id > 0) AS t14 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void testEnrichJoinLogicalAndStandalone(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM shares.accounts b JOIN shares.standalone a ON a.id = b.account_id WHERE a.id > 0",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id, account_type, id, name FROM (SELECT account_id, account_type, id, name FROM (SELECT accounts_actual.account_id, accounts_actual.account_type, tbl_shard.id, tbl_shard.name FROM local__shares.accounts_actual FINAL INNER JOIN schema.tbl_shard ON accounts_actual.account_id = tbl_shard.id WHERE accounts_actual.sys_from <= 1 AND accounts_actual.sys_to >= 1) AS t1 WHERE t1.id > 0) AS t3 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT account_id, account_type, id, name FROM (SELECT account_id, account_type, id, name FROM (SELECT accounts_actual1.account_id, accounts_actual1.account_type, tbl_shard0.id, tbl_shard0.name FROM local__shares.accounts_actual AS accounts_actual1 INNER JOIN schema.tbl_shard AS tbl_shard0 ON accounts_actual1.account_id = tbl_shard0.id WHERE accounts_actual1.sys_from <= 1 AND accounts_actual1.sys_to >= 1) AS t12 WHERE t12.id > 0) AS t14 WHERE (((SELECT 1 AS r FROM local__shares.accounts_actual WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void testEnrichWithStandaloneTableWithSysColumns(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM library.authors_standalone_sys a WHERE a.id > 0 and a.sys_from = 'test'",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT id, name, sys_from FROM library.tbl_authors_sa_sys WHERE id > 0 AND sys_from = 'test'");
    }

    @Test
    void testEnrichJoinLogicalAndStandaloneWithSysColumns(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM library.books b JOIN library.authors_standalone_sys a ON a.id = b.author_id WHERE a.sys_from = 'test'",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT id, author_id, description, id0, name, sys_from0 FROM (SELECT id, author_id, description, id0, name, sys_from0 FROM (SELECT books_actual.id, books_actual.author_id, books_actual.description, tbl_authors_sa_sys_shard.id AS id0, tbl_authors_sa_sys_shard.name, tbl_authors_sa_sys_shard.sys_from AS sys_from0 FROM local__library.books_actual FINAL INNER JOIN library.tbl_authors_sa_sys_shard ON books_actual.author_id = tbl_authors_sa_sys_shard.id WHERE books_actual.sys_from <= 1 AND books_actual.sys_to >= 1) AS t1 WHERE t1.sys_from0 = 'test') AS t3 WHERE (((SELECT 1 AS r FROM local__library.books_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT id, author_id, description, id0, name, sys_from0 FROM (SELECT id, author_id, description, id0, name, sys_from0 FROM (SELECT books_actual1.id, books_actual1.author_id, books_actual1.description, tbl_authors_sa_sys_shard0.id AS id0, tbl_authors_sa_sys_shard0.name, tbl_authors_sa_sys_shard0.sys_from AS sys_from0 FROM local__library.books_actual AS books_actual1 INNER JOIN library.tbl_authors_sa_sys_shard AS tbl_authors_sa_sys_shard0 ON books_actual1.author_id = tbl_authors_sa_sys_shard0.id WHERE books_actual1.sys_from <= 1 AND books_actual1.sys_to >= 1) AS t12 WHERE t12.sys_from0 = 'test') AS t14 WHERE (((SELECT 1 AS r FROM local__library.books_actual WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void testEnrichJoinStandaloneWithSysColumnsAndLogical(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM library.authors_standalone_sys a JOIN library.books b ON a.id = b.author_id WHERE a.sys_from = 'test'",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT id, name, sys_from, id0, author_id, description FROM (SELECT id, name, sys_from, id0, author_id, description FROM (SELECT tbl_authors_sa_sys.id, tbl_authors_sa_sys.name, tbl_authors_sa_sys.sys_from, books_actual_shard.id AS id0, books_actual_shard.author_id, books_actual_shard.description FROM library.tbl_authors_sa_sys INNER JOIN local__library.books_actual_shard FINAL ON tbl_authors_sa_sys.id = books_actual_shard.author_id WHERE tbl_authors_sa_sys.sys_from <= 1 AND books_actual_shard.sys_to >= 1) AS t1 WHERE t1.sys_from = 'test') AS t3 WHERE (((SELECT 1 AS r FROM local__library.books_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT id, name, sys_from, id0, author_id, description FROM (SELECT id, name, sys_from, id0, author_id, description FROM (SELECT tbl_authors_sa_sys0.id, tbl_authors_sa_sys0.name, tbl_authors_sa_sys0.sys_from, books_actual_shard1.id AS id0, books_actual_shard1.author_id, books_actual_shard1.description FROM library.tbl_authors_sa_sys AS tbl_authors_sa_sys0 INNER JOIN local__library.books_actual_shard AS books_actual_shard1 ON tbl_authors_sa_sys0.id = books_actual_shard1.author_id WHERE tbl_authors_sa_sys0.sys_from <= 1 AND books_actual_shard1.sys_to >= 1) AS t12 WHERE t12.sys_from = 'test') AS t14 WHERE (((SELECT 1 AS r FROM local__library.books_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void testEnrichJoinStandaloneWithSysColumnsAndStandalone(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM library.authors_standalone_sys a JOIN library.books_standalone b ON a.id = b.author_id WHERE a.sys_from = 'test'",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT tbl_authors_sa_sys.id, tbl_authors_sa_sys.name, tbl_authors_sa_sys.sys_from, tbl_books_sa_shard.id AS id0, tbl_books_sa_shard.author_id, tbl_books_sa_shard.description FROM library.tbl_authors_sa_sys INNER JOIN library.tbl_books_sa_shard ON tbl_authors_sa_sys.id = tbl_books_sa_shard.author_id WHERE tbl_authors_sa_sys.sys_from = 'test'");
    }

    @Test
    void testEnrichJoinStandaloneTablesWithSysColumns(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT * FROM library.authors_standalone_sys a JOIN library.books_standalone_sys b ON a.id = b.author_id WHERE a.sys_from = 'test'",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT tbl_authors_sa_sys.id, tbl_authors_sa_sys.name, tbl_authors_sa_sys.sys_from, tbl_books_sa_sys_shard.id AS id0, tbl_books_sa_sys_shard.author_id, tbl_books_sa_sys_shard.description, tbl_books_sa_sys_shard.sys_to FROM library.tbl_authors_sa_sys INNER JOIN library.tbl_books_sa_sys_shard ON tbl_authors_sa_sys.id = tbl_books_sa_sys_shard.author_id WHERE tbl_authors_sa_sys.sys_from = 'test'");
    }

    @Test
    void testEnrichSelectLogicJoinStandaloneWithSysColumns(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT *\n" +
                        "FROM (SELECT id, description, author_id\n" +
                        "      FROM library.books) t1\n" +
                        "         JOIN library.authors_standalone_sys as t2\n" +
                        "              ON t1.author_id = t2.id\n" +
                        "WHERE t1.id > 0\n" +
                        "  AND t2.sys_from = 'test'",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT id, description, author_id, id0, name, sys_from FROM (SELECT t1.id, t1.description, t1.author_id, tbl_authors_sa_sys_shard.id AS id0, tbl_authors_sa_sys_shard.name, tbl_authors_sa_sys_shard.sys_from FROM (SELECT id, description, author_id FROM local__library.books_actual FINAL WHERE sys_from <= NULL AND sys_to >= NULL) AS t1 INNER JOIN library.tbl_authors_sa_sys_shard ON t1.author_id = tbl_authors_sa_sys_shard.id WHERE t1.id > 0 AND tbl_authors_sa_sys_shard.sys_from = 'test') AS t3 WHERE (((SELECT 1 AS r FROM local__library.books_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT id, description, author_id, id0, name, sys_from FROM (SELECT t12.id, t12.description, t12.author_id, tbl_authors_sa_sys_shard0.id AS id0, tbl_authors_sa_sys_shard0.name, tbl_authors_sa_sys_shard0.sys_from FROM (SELECT id, description, author_id FROM local__library.books_actual WHERE sys_from <= NULL AND sys_to >= NULL) AS t12 INNER JOIN library.tbl_authors_sa_sys_shard AS tbl_authors_sa_sys_shard0 ON t12.author_id = tbl_authors_sa_sys_shard0.id WHERE t12.id > 0 AND tbl_authors_sa_sys_shard0.sys_from = 'test') AS t14 WHERE (((SELECT 1 AS r FROM local__library.books_actual WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void testEnrichStandaloneWithSysColumnsJoinSelectLogic(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT *\n" +
                        "FROM (SELECT id, description, author_id, sys_to\n" +
                        "      FROM library.books_standalone_sys) t1\n" +
                        "         JOIN library.books as t2\n" +
                        "              ON t1.id = t2.author_id\n" +
                        "WHERE t1.sys_to = 'test'",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT id, description, author_id, sys_to, id0, author_id0, description0 FROM (SELECT id, description, author_id, sys_to, id0, author_id0, description0 FROM (SELECT t.id, t.description, t.author_id, t.sys_to, books_actual_shard.id AS id0, books_actual_shard.author_id AS author_id0, books_actual_shard.description AS description0 FROM (SELECT id, description, author_id, sys_to FROM library.tbl_books_sa_sys) AS t INNER JOIN local__library.books_actual_shard FINAL ON t.id = books_actual_shard.author_id WHERE books_actual_shard.sys_from <= 1 AND t.sys_to >= 1) AS t2 WHERE t2.sys_to = 'test') AS t4 WHERE (((SELECT 1 AS r FROM local__library.books_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT id, description, author_id, sys_to, id0, author_id0, description0 FROM (SELECT id, description, author_id, sys_to, id0, author_id0, description0 FROM (SELECT t11.id, t11.description, t11.author_id, t11.sys_to, books_actual_shard1.id AS id0, books_actual_shard1.author_id AS author_id0, books_actual_shard1.description AS description0 FROM (SELECT id, description, author_id, sys_to FROM library.tbl_books_sa_sys) AS t11 INNER JOIN local__library.books_actual_shard AS books_actual_shard1 ON t11.id = books_actual_shard1.author_id WHERE books_actual_shard1.sys_from <= 1 AND t11.sys_to >= 1) AS t14 WHERE t14.sys_to = 'test') AS t16 WHERE (((SELECT 1 AS r FROM local__library.books_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void testEnrichStandaloneWithSysColumnsJoins(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("SELECT *\n" +
                        "FROM (SELECT id, description, author_id, sys_to\n" +
                        "      FROM library.books_standalone_sys) t1\n" +
                        "         JOIN library.authors_standalone_sys as t2\n" +
                        "              ON t1.author_id = t2.id\n" +
                        "WHERE t1.sys_to = 'test'",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT t.id, t.description, t.author_id, t.sys_to, tbl_authors_sa_sys_shard.id AS id0, tbl_authors_sa_sys_shard.name, tbl_authors_sa_sys_shard.sys_from FROM (SELECT id, description, author_id, sys_to FROM library.tbl_books_sa_sys) AS t INNER JOIN library.tbl_authors_sa_sys_shard ON t.author_id = tbl_authors_sa_sys_shard.id WHERE t.sys_to = 'test'");
    }

    @Test
    void testEnrichJoinLogicalWithStandaloneTables(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("select *\n" +
                        "from library.books t1\n" +
                        "         join (select *\n" +
                        "               from library.books_standalone t2\n" +
                        "                        join library.books_standalone_sys t3 on t2.id = t3.id) as t4 on t1.id = t4.id",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT id, author_id, description, id0, author_id0, description0, id00, author_id00, description00, sys_to0 FROM (SELECT books_actual.id, books_actual.author_id, books_actual.description, tbl_books_sa_shard.id AS id0, tbl_books_sa_shard.author_id AS author_id0, tbl_books_sa_shard.description AS description0, tbl_books_sa_sys_shard.id AS id00, tbl_books_sa_sys_shard.author_id AS author_id00, tbl_books_sa_sys_shard.description AS description00, tbl_books_sa_sys_shard.sys_to AS sys_to0 FROM local__library.books_actual FINAL INNER JOIN (library.tbl_books_sa_shard INNER JOIN library.tbl_books_sa_sys_shard ON tbl_books_sa_shard.id = tbl_books_sa_sys_shard.id) ON books_actual.id = tbl_books_sa_shard.id WHERE books_actual.sys_from <= NULL AND books_actual.sys_to >= NULL) AS t1 WHERE (((SELECT 1 AS r FROM local__library.books_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT id, author_id, description, id0, author_id0, description0, id00, author_id00, description00, sys_to0 FROM (SELECT books_actual1.id, books_actual1.author_id, books_actual1.description, tbl_books_sa_shard0.id AS id0, tbl_books_sa_shard0.author_id AS author_id0, tbl_books_sa_shard0.description AS description0, tbl_books_sa_sys_shard0.id AS id00, tbl_books_sa_sys_shard0.author_id AS author_id00, tbl_books_sa_sys_shard0.description AS description00, tbl_books_sa_sys_shard0.sys_to AS sys_to0 FROM local__library.books_actual AS books_actual1 INNER JOIN (library.tbl_books_sa_shard AS tbl_books_sa_shard0 INNER JOIN library.tbl_books_sa_sys_shard AS tbl_books_sa_sys_shard0 ON tbl_books_sa_shard0.id = tbl_books_sa_sys_shard0.id) ON books_actual1.id = tbl_books_sa_shard0.id WHERE books_actual1.sys_from <= NULL AND books_actual1.sys_to >= NULL) AS t10 WHERE (((SELECT 1 AS r FROM local__library.books_actual WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    @Test
    void testEnrichManyStandaloneJoins(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestWithDeltas("select *\n" +
                        "from library.authors_standalone_sys w\n" +
                        "         join\n" +
                        "(select *\n" +
                        "from library.books e\n" +
                        "         join\n" +
                        "     (select *\n" +
                        "      from library.books_standalone a\n" +
                        "               join (select *\n" +
                        "                     from library.books_standalone_sys b\n" +
                        "                              join library.authors_standalone_sys c on b.author_id = c.id) as d\n" +
                        "                    on a.id = d.id) as f on e.id = f.id) as q on w.id = q.author_id",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT tbl_authors_sa_sys.id, tbl_authors_sa_sys.name, tbl_authors_sa_sys.sys_from, t1.id AS id0, t1.author_id, t1.description, t1.id0 AS id00, t1.author_id0, t1.description0, t1.id00 AS id000, t1.author_id00, t1.description00, t1.sys_to0, t1.id000 AS id0000, t1.name AS name0, t1.sys_from0 FROM library.tbl_authors_sa_sys INNER JOIN (SELECT books_actual_shard.id, books_actual_shard.author_id, books_actual_shard.description, tbl_books_sa_shard.id AS id0, tbl_books_sa_shard.author_id AS author_id0, tbl_books_sa_shard.description AS description0, tbl_books_sa_sys_shard.id AS id00, tbl_books_sa_sys_shard.author_id AS author_id00, tbl_books_sa_sys_shard.description AS description00, tbl_books_sa_sys_shard.sys_to AS sys_to0, tbl_authors_sa_sys_shard.id AS id000, tbl_authors_sa_sys_shard.name, tbl_authors_sa_sys_shard.sys_from AS sys_from0 FROM local__library.books_actual_shard FINAL INNER JOIN (library.tbl_books_sa_shard INNER JOIN (library.tbl_books_sa_sys_shard INNER JOIN library.tbl_authors_sa_sys_shard ON tbl_books_sa_sys_shard.author_id = tbl_authors_sa_sys_shard.id) ON tbl_books_sa_shard.id = tbl_books_sa_sys_shard.id) ON books_actual_shard.id = tbl_books_sa_shard.id WHERE books_actual_shard.sys_from <= 1 AND books_actual_shard.sys_to >= 1) AS t1 ON tbl_authors_sa_sys.id = t1.author_id WHERE (((SELECT 1 AS r FROM local__library.books_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT tbl_authors_sa_sys0.id, tbl_authors_sa_sys0.name, tbl_authors_sa_sys0.sys_from, t10.id AS id0, t10.author_id, t10.description, t10.id0 AS id00, t10.author_id0, t10.description0, t10.id00 AS id000, t10.author_id00, t10.description00, t10.sys_to0, t10.id000 AS id0000, t10.name AS name0, t10.sys_from0 FROM library.tbl_authors_sa_sys AS tbl_authors_sa_sys0 INNER JOIN (SELECT books_actual_shard1.id, books_actual_shard1.author_id, books_actual_shard1.description, tbl_books_sa_shard0.id AS id0, tbl_books_sa_shard0.author_id AS author_id0, tbl_books_sa_shard0.description AS description0, tbl_books_sa_sys_shard0.id AS id00, tbl_books_sa_sys_shard0.author_id AS author_id00, tbl_books_sa_sys_shard0.description AS description00, tbl_books_sa_sys_shard0.sys_to AS sys_to0, tbl_authors_sa_sys_shard0.id AS id000, tbl_authors_sa_sys_shard0.name, tbl_authors_sa_sys_shard0.sys_from AS sys_from0 FROM local__library.books_actual_shard AS books_actual_shard1 INNER JOIN (library.tbl_books_sa_shard AS tbl_books_sa_shard0 INNER JOIN (library.tbl_books_sa_sys_shard AS tbl_books_sa_sys_shard0 INNER JOIN library.tbl_authors_sa_sys_shard AS tbl_authors_sa_sys_shard0 ON tbl_books_sa_sys_shard0.author_id = tbl_authors_sa_sys_shard0.id) ON tbl_books_sa_shard0.id = tbl_books_sa_sys_shard0.id) ON books_actual_shard1.id = tbl_books_sa_shard0.id WHERE books_actual_shard1.sys_from <= 1 AND books_actual_shard1.sys_to >= 1) AS t10 ON tbl_authors_sa_sys0.id = t10.author_id WHERE (((SELECT 1 AS r FROM local__library.books_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL");
    }

    private void enrichAndAssert(VertxTestContext testContext, Future<EnrichQueryRequest> enrichRequestFuture,
                                 String expectedSql) {
        enrichRequestFuture.compose(request -> enrichService.enrich(request))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        Assertions.fail(ar.cause());
                    }

                    assertNormalizedEquals(ar.result(), expectedSql);
                }).completeNow());
    }

    private Future<EnrichQueryRequest> prepareRequestWithDeltas(String sql, List<DeltaInformation> deltaInformations) {
        val sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        return queryParserService.parse(new QueryParserRequest(sqlNode, datamarts, ENV_NAME))
                .map(queryParserResponse -> EnrichQueryRequest.builder()
                        .envName(ENV_NAME)
                        .deltaInformations(deltaInformations)
                        .calciteContext(queryParserResponse.getCalciteContext())
                        .relNode(queryParserResponse.getRelNode())
                        .build());
    }
}
