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
package ru.datamart.prostore.query.execution.plugin.adb.dml;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.delta.SelectOnInterval;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbDmlQueryExtendWithoutHistoryService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbQueryGenerator;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbSchemaExtender;
import ru.datamart.prostore.query.execution.plugin.adb.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryExtendService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.query.execution.plugin.adb.utils.TestUtils.assertNormalizedEquals;

@Slf4j
@ExtendWith(VertxExtension.class)
class AdbQueryEnrichmentServiceWithoutHistoryTest {

    public static final String SHARES_SCHEMA_NAME = "shares";
    public static final String SHARES_2_SCHEMA_NAME = "shares_2";
    public static final String TEST_DATAMART_SCHEMA_NAME = "test_datamart";
    public static final String ENV_NAME = "test";
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final QueryExtendService queryExtender = new AdbDmlQueryExtendWithoutHistoryService();
    private final AdbCalciteContextProvider contextProvider = new AdbCalciteContextProvider(
            calciteConfiguration.configDdlParser(calciteConfiguration.ddlParserImplFactory()),
            new AdbCalciteSchemaFactory(new AdbSchemaFactory()));
    private final SqlDialect sqlDialect = calciteConfiguration.adbSqlDialect();
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
    private final AdbQueryGenerator adbQueryGeneratorimpl = new AdbQueryGenerator(queryExtender, calciteConfiguration.adbSqlDialect(), relToSqlConverter);
    private final QueryParserService queryParserService = new AdbCalciteDMLQueryParserService(contextProvider, Vertx.vertx());
    private final QueryEnrichmentService adbQueryEnrichmentService = new AdbQueryEnrichmentService(adbQueryGeneratorimpl, contextProvider, new AdbSchemaExtender());

    @Test
    void testEnrichWithDeltaNum(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("select * from shares.accounts");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1");
    }

    @Test
    void testEnrichWithCountAndLimit(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("SELECT COUNT(*) AS C FROM shares.accounts LIMIT 100");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT COUNT(*) AS c FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1 LIMIT 100");
    }

    @Test
    void testEnrichWithSubquery(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("SELECT * FROM shares.accounts as b where b.account_id IN (select c.account_id from shares.transactions as c limit 1)");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT * FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 WHERE account_id IN (SELECT account_id FROM shares.transactions_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1 LIMIT 1)");
    }

    @Test
    void testEnrichWithCountAndGroupByLimit(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("SELECT account_id, COUNT(*) AS C FROM shares.accounts GROUP BY account_id LIMIT 100");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id, COUNT(*) AS c FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1 GROUP BY account_id LIMIT 100");
    }

    @Test
    void testEnrichWithCountAndGroupByAndOrderByLimit(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("SELECT account_id, COUNT(*) AS C FROM shares.accounts GROUP BY account_id ORDER BY account_id LIMIT 100");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id, COUNT(*) AS c FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1 GROUP BY account_id ORDER BY account_id LIMIT 100");
    }

    @Test
    void testEnrichWithAggregate(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("SELECT COUNT(*) AS C FROM shares.accounts");


        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT COUNT(*) AS c FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1");
    }

    @Test
    void testEnrichWithFunctionInJoin(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("SELECT * FROM shares.accounts a JOIN shares.transactions t ON ABS(a.account_id) = ABS(t.account_id) WHERE a.account_id > 0");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT * FROM (SELECT t1.account_id, t1.account_type, t4.transaction_id, t4.transaction_date, t4.account_id AS account_id0, t4.amount FROM (SELECT account_id, account_type, ABS(account_id) AS __f2 FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t1 INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount, ABS(account_id) AS __f4 FROM shares.transactions_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t4 ON t1.__f2 = t4.__f4) AS t5 WHERE t5.account_id > 0");
    }

    @Test
    void enrichWithDeltaNum(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "select *, (CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END) as c\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT t0.account_id, COALESCE(SUM(t2.amount), 0) AS amount, t0.account_type, CASE WHEN t0.account_type = 'D' AND COALESCE(SUM(t2.amount), 0) >= 0 OR t0.account_type = 'C' AND COALESCE(SUM(t2.amount), 0) <= 0 THEN 'OK' ELSE 'NOT OK' END AS c FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 LEFT JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM shares.transactions_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t2 ON t0.account_id = t2.account_id GROUP BY t0.account_id, t0.account_type");
    }

    @Test
    void enrichWithFinishedIn(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaFinishedIn(
                "SELECT account_id FROM shares.accounts");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id FROM shares.accounts_actual WHERE COALESCE(sys_to, 9223372036854775807) >= 0 AND (COALESCE(sys_to, 9223372036854775807) <= 0 AND sys_op = 1)");
    }

    @Test
    void enrichWithStaticCaseExpressions(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "select a.account_id, (case when a.account_type = 'D' then 'ok' else 'not ok' end) as ss " +
                        "from shares.accounts a ");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id, CASE WHEN account_type = 'D' THEN 'ok' ELSE 'not ok' END AS ss FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1");
    }

    @Test
    void enrichWithDeltaInterval(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
                "select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT t0.account_id, COALESCE(SUM(t2.amount), 0) AS amount, t0.account_type, CASE WHEN t0.account_type = 'D' AND COALESCE(SUM(t2.amount), 0) >= 0 OR t0.account_type = 'C' AND COALESCE(SUM(t2.amount), 0) <= 0 THEN 'OK' ELSE 'NOT OK' END AS EXPR__3 FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5) AS t0 LEFT JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM shares.transactions_actual WHERE COALESCE(sys_to, 9223372036854775807) >= 2 AND (COALESCE(sys_to, 9223372036854775807) <= 3 AND sys_op = 1)) AS t2 ON t0.account_id = t2.account_id GROUP BY t0.account_id, t0.account_type");
    }

    @Test
    void enrichWithNull(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
                "select account_id, null, null from shares.accounts");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id, NULL AS EXPR__1, NULL AS EXPR__2 FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5");
    }

    @Test
    void enrichWithLimit(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
                "select account_id from shares.accounts limit 50");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5 LIMIT 50");
    }

    @Test
    void enrichWithLimitAndOrder(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
                "select account_id from shares.accounts order by account_id limit 50");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5 ORDER BY account_id LIMIT 50");
    }

    @Test
    void enrichWithManyInKeyword(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "SELECT account_id FROM shares.accounts WHERE account_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 WHERE account_id = 1 OR account_id = 2 OR (account_id = 3 OR (account_id = 4 OR account_id = 5)) OR (account_id = 6 OR (account_id = 7 OR account_id = 8) OR (account_id = 9 OR (account_id = 10 OR account_id = 11))) OR (account_id = 12 OR (account_id = 13 OR account_id = 14) OR (account_id = 15 OR (account_id = 16 OR account_id = 17)) OR (account_id = 18 OR (account_id = 19 OR account_id = 20) OR (account_id = 21 OR (account_id = 22 OR account_id = 23))))");
    }

    @Test
    void enrichWithLimitOffset(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
                "select account_id from shares.accounts limit 30 offset 50");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5 LIMIT 30 OFFSET 50");
    }

    @Test
    void enrichWithOffsetFetchNext(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval(
                "select account_id from shares.accounts fetch next 30 rows only offset 50");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5 LIMIT 30 OFFSET 50");
    }

    @Test
    void enrichWithMultipleLogicalSchema(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchemas(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT t0.account_id, t0.account_type, t2.account_id AS account_id0, t2.account_type AS account_type0, t4.transaction_id, t4.transaction_date, t4.account_id AS account_id1, t4.amount FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 INNER JOIN (SELECT account_id, account_type FROM shares_2.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t2 ON t0.account_id = t2.account_id INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t4 ON t0.account_id = t4.account_id");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimit(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchemas(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT t0.account_id, t0.account_type, t2.account_id AS account_id0, t2.account_type AS account_type0, t4.transaction_id, t4.transaction_date, t4.account_id AS account_id1, t4.amount FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 INNER JOIN (SELECT account_id, account_type FROM shares_2.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t2 ON t0.account_id = t2.account_id INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t4 ON t0.account_id = t4.account_id LIMIT 10");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimitOrderBy(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchemas(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT t0.account_id, t0.account_type, t2.account_id AS account_id0, t2.account_type AS account_type0, t4.transaction_id, t4.transaction_date, t4.account_id AS account_id1, t4.amount FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 INNER JOIN (SELECT account_id, account_type FROM shares_2.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t2 ON t0.account_id = t2.account_id INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t4 ON t0.account_id = t4.account_id ORDER BY t2.account_id LIMIT 10");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimitOrderByGroupBy(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchemas(
                "select aa.account_id from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id GROUP BY aa.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT t2.account_id FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 INNER JOIN (SELECT account_id, account_type FROM shares_2.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t2 ON t0.account_id = t2.account_id INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t4 ON t0.account_id = t4.account_id GROUP BY t2.account_id ORDER BY t2.account_id LIMIT 10");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimitOrderByGroupByAndAggregate(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchemas(
                "select aa.account_id, count(*), 1 as K from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id GROUP BY aa.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT t2.account_id, COUNT(*) AS EXPR__1, 1 AS k FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 INNER JOIN (SELECT account_id, account_type FROM shares_2.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t2 ON t0.account_id = t2.account_id INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t4 ON t0.account_id = t4.account_id GROUP BY t2.account_id ORDER BY t2.account_id LIMIT 10");
    }

    @Test
    void enrichWithCustomSelect(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "SELECT *, 0 FROM shares.accounts ORDER BY account_id");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id, account_type, 0 AS EXPR__2 FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1 ORDER BY account_id");
    }

    @Test
    void enrichWithAliasesAndFunctions(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum("SELECT *\n" +
                "FROM (SELECT\n" +
                "          account_id      as bc1,\n" +
                "          true            as bc2,\n" +
                "          abs(account_id) as bc3,\n" +
                "          'some$' as bc4,\n" +
                "          '$some'\n" +
                "      FROM shares.accounts) t1\n" +
                "               JOIN shares.transactions as t2\n" +
                "                    ON t1.bc1 = t2.account_id AND abs(t2.account_id) = t1.bc3\n" +
                "WHERE t1.bc2 = true AND t1.bc3 = 0"
        );

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT * FROM (SELECT t1.bc1, t1.bc2, t1.bc3, t1.bc4, t1.EXPR__4, t4.transaction_id, t4.transaction_date, t4.account_id, t4.amount FROM (SELECT account_id AS bc1, TRUE AS bc2, ABS(account_id) AS bc3, 'some$' AS bc4, '$some' AS EXPR__4 FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t1 INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount, ABS(account_id) AS __f4 FROM shares.transactions_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t4 ON t1.bc1 = t4.account_id AND t1.bc3 = t4.__f4) AS t5 WHERE t5.bc2 = TRUE AND t5.bc3 = 0");
    }

    @Test
    void shouldFailWhenNotEnoughDeltas(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "SELECT t1.account_id FROM shares.accounts t1" +
                        " join shares.accounts t2 on t2.account_id = t1.account_id" +
                        " join shares.accounts t3 on t3.account_id = t2.account_id" +
                        " where t1.account_id = 10"
        );

        // act assert
        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> adbQueryEnrichmentService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        Assertions.fail("Unexpected success");
                    }

                    Assertions.assertSame(DataSourceException.class, ar.cause().getClass());
                }).completeNow());
    }

    @Test
    void testEnrichWithUnions(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("select account_id\n" +
                        "from (select * from (select * from shares.accounts order by account_id limit 1)\n" +
                        "         union all\n" +
                        "         select * from shares.accounts)");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id FROM (SELECT account_id, account_type FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1 ORDER BY account_id LIMIT 1) AS t2 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t6");
    }

    @Test
    void testEnrichWithUnions2(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("select account_id\n" +
                        "from (select account_id from (select account_id from shares.accounts order by account_id limit 1) where account_id = 0\n" +
                        "         union all\n" +
                        "         select account_id from shares.accounts)");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT * FROM (SELECT account_id FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1 ORDER BY account_id LIMIT 1) AS t3 WHERE account_id = 0 UNION ALL SELECT account_id FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1");
    }

    @Test
    void testEnrichWithUnions3(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("select account_id\n" +
                        "from (select account_id from (select account_id from shares.accounts where account_id = 0 order by account_id limit 1)\n" +
                        "         union all\n" +
                        "         select account_id from shares.accounts)");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT account_id FROM (SELECT account_id FROM (SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1) AS t0 WHERE account_id = 0 ORDER BY account_id LIMIT 1) AS t4 UNION ALL SELECT account_id FROM shares.accounts_actual WHERE sys_from <= 1 AND COALESCE(sys_to, 9223372036854775807) >= 1");
    }

    private EnrichQueryRequest prepareRequestMultipleSchemas(String sql) {
        List<Datamart> schemas = Arrays.asList(
                getSchema(SHARES_SCHEMA_NAME, true),
                getSchema(SHARES_2_SCHEMA_NAME, false),
                getSchema(TEST_DATAMART_SCHEMA_NAME, false));
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemas.get(0).getMnemonic())
                        .tableName(schemas.get(0).getEntities().get(0).getName())
                        .pos(pos)
                        .selectOnNum(1L)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("aa")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemas.get(1).getMnemonic())
                        .tableName(schemas.get(1).getEntities().get(1).getName())
                        .pos(pos)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemas.get(2).getMnemonic())
                        .tableName(schemas.get(2).getEntities().get(1).getName())
                        .pos(pos)
                        .build()
        );
        return EnrichQueryRequest.builder()
                .deltaInformations(deltaInforamtions)
                .envName("test")
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .schema(schemas)
                .build();
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> schemas = Collections.singletonList(getSchema(SHARES_SCHEMA_NAME, true));
        String schemaName = schemas.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(schemas.get(0).getEntities().get(0).getName())
                        .pos(pos)
                        .selectOnNum(1L)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(schemas.get(0).getEntities().get(1).getName())
                        .pos(pos)
                        .build()
        );

        return EnrichQueryRequest.builder()
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .schema(schemas)
                .build();
    }

    private EnrichQueryRequest prepareRequestDeltaInterval(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema(SHARES_SCHEMA_NAME, true));
        String schemaName = datamarts.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp(null)
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(new SelectOnInterval(1L, 5L))
                        .selectOnInterval(new SelectOnInterval(1L, 5L))
                        .type(DeltaType.STARTED_IN)
                        .schemaName(schemaName)
                        .tableName(datamarts.get(0).getEntities().get(0).getName())
                        .pos(pos)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t")
                        .deltaTimestamp(null)
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(new SelectOnInterval(3L, 4L))
                        .selectOnInterval(new SelectOnInterval(3L, 4L))
                        .type(DeltaType.FINISHED_IN)
                        .schemaName(schemaName)
                        .tableName(datamarts.get(0).getEntities().get(1).getName())
                        .pos(pos)
                        .build()
        );
        return EnrichQueryRequest.builder()
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .schema(datamarts)
                .build();
    }

    private EnrichQueryRequest prepareRequestDeltaFinishedIn(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema(SHARES_SCHEMA_NAME, true));
        String schemaName = datamarts.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInforamtions = Collections.singletonList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp(null)
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(new SelectOnInterval(1L, 1L))
                        .selectOnInterval(new SelectOnInterval(1L, 1L))
                        .type(DeltaType.FINISHED_IN)
                        .schemaName(schemaName)
                        .tableName(datamarts.get(0).getEntities().get(0).getName())
                        .pos(pos)
                        .build()
        );
        return EnrichQueryRequest.builder()
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .schema(datamarts)
                .build();
    }

    private Datamart getSchema(String schemaName, boolean isDefault) {
        Entity accounts = Entity.builder()
                .schema(schemaName)
                .name("accounts")
                .build();
        List<EntityField> accAttrs = Arrays.asList(
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("account_id")
                        .ordinalPosition(1)
                        .shardingOrder(1)
                        .primaryOrder(1)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.VARCHAR)
                        .name("account_type")
                        .ordinalPosition(2)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(false)
                        .accuracy(null)
                        .size(1)
                        .build()
        );
        accounts.setFields(accAttrs);

        Entity transactions = Entity.builder()
                .schema(schemaName)
                .name("transactions")
                .build();

        List<EntityField> trAttr = Arrays.asList(
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("transaction_id")
                        .ordinalPosition(1)
                        .shardingOrder(1)
                        .primaryOrder(1)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.DATE)
                        .name("transaction_date")
                        .ordinalPosition(2)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(true)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("account_id")
                        .ordinalPosition(3)
                        .shardingOrder(1)
                        .primaryOrder(2)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("amount")
                        .ordinalPosition(4)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build()
        );

        transactions.setFields(trAttr);

        return new Datamart(schemaName, isDefault, Arrays.asList(transactions, accounts));
    }

    private void enrichAndAssert(VertxTestContext testContext, EnrichQueryRequest enrichRequest,
                                 String expectedSql) {
        queryParserService.parse(new QueryParserRequest(enrichRequest.getQuery(), enrichRequest.getSchema()))
                .compose(parserResponse -> adbQueryEnrichmentService.enrich(enrichRequest, parserResponse))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        Assertions.fail(ar.cause());
                    }

                    assertNormalizedEquals(ar.result(), expectedSql);
                }).completeNow());
    }
}
