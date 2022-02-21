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
package ru.datamart.prostore.query.execution.plugin.adg.enrichment.service;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
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
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.factory.AdgCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.factory.AdgSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.service.AdgCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.service.AdgCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adg.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.query.execution.plugin.adg.utils.TestUtils.assertNormalizedEquals;

@ExtendWith(VertxExtension.class)
class AdgQueryEnrichmentServiceTest {
    private static final String ENV_NAME = "local";
    private final QueryEnrichmentService enrichService;
    private final QueryParserService queryParserService;
    private final SqlDialect sqlDialect;

    public AdgQueryEnrichmentServiceTest(Vertx vertx) {
        val calciteConfiguration = new AdgCalciteConfiguration();
        calciteConfiguration.init();
        val parserConfig = calciteConfiguration.configDdlParser(
                calciteConfiguration.ddlParserImplFactory());
        val contextProvider = new AdgCalciteContextProvider(
                parserConfig,
                new AdgCalciteSchemaFactory(new AdgSchemaFactory()));

        queryParserService = new AdgCalciteDMLQueryParserService(contextProvider, vertx);
        val helperTableNamesFactory = new AdgHelperTableNamesFactory();
        val queryExtendService = new AdgDmlQueryExtendService(helperTableNamesFactory);

        sqlDialect = calciteConfiguration.adgSqlDialect();
        val relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
        val collateReplacer = new AdgCollateValueReplacer();
        enrichService = new AdgQueryEnrichmentService(
                contextProvider,
                new AdgQueryGenerator(queryExtendService, sqlDialect, relToSqlConverter, collateReplacer),
                new AdgSchemaExtender(helperTableNamesFactory));
    }

    @Test
    void enrichWithDeltaNum(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum("SELECT account_id FROM shares.accounts");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\"");
    }

    @Test
    void enrichWithFinishedIn(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaFinishedIn("SELECT account_id FROM shares.accounts");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM \"local__shares__accounts_history\" WHERE \"sys_to\" >= 0 AND (\"sys_to\" <= 0 AND \"sys_op\" = 1)");
    }

    @Test
    void enrichWithDeltaInterval(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval("select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                "OR (account_type = 'C' AND  amount <= 0) THEN 'OK    ' ELSE 'NOT OK' END\n" +
                "  from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                "    from shares.accounts a\n" +
                "    left join shares.transactions t using(account_id)\n" +
                "   group by a.account_id, account_type\n" +
                ")x");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"t3\".\"account_id\", COALESCE(SUM(\"t5\".\"amount\"), 0) AS \"amount\", \"t3\".\"account_type\", CASE WHEN \"t3\".\"account_type\" = 'D' AND COALESCE(SUM(\"t5\".\"amount\"), 0) >= 0 OR \"t3\".\"account_type\" = 'C' AND COALESCE(SUM(\"t5\".\"amount\"), 0) <= 0 THEN 'OK    ' ELSE 'NOT OK' END AS \"EXPR__3\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" >= 1 AND \"sys_from\" <= 5 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" >= 1 AND \"sys_from\" <= 5) AS \"t3\" LEFT JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_history\" WHERE \"sys_to\" >= 2 AND (\"sys_to\" <= 3 AND \"sys_op\" = 1)) AS \"t5\" ON \"t3\".\"account_id\" = \"t5\".\"account_id\" GROUP BY \"t3\".\"account_id\", \"t3\".\"account_type\"");
    }

    @Test
    void enrichWithQuotes(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum("SELECT \"account_id\" FROM \"shares\".\"accounts\"");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\"");
    }

    @Test
    void enrichWithWhereCollate(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaWithoutSnapshot("SELECT account_id FROM shares.accounts WHERE account_type = 'type' COLLATE 'unicode_ci'");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" WHERE \"account_type\" = 'type' COLLATE \"unicode_ci\"");
    }

    @Test
    void enrichWithManyInKeyword(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "SELECT account_id FROM shares.accounts WHERE account_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" WHERE \"account_id\" = 1 OR \"account_id\" = 2 OR (\"account_id\" = 3 OR (\"account_id\" = 4 OR \"account_id\" = 5)) OR (\"account_id\" = 6 OR (\"account_id\" = 7 OR \"account_id\" = 8) OR (\"account_id\" = 9 OR (\"account_id\" = 10 OR \"account_id\" = 11))) OR (\"account_id\" = 12 OR (\"account_id\" = 13 OR \"account_id\" = 14) OR (\"account_id\" = 15 OR (\"account_id\" = 16 OR \"account_id\" = 17)) OR (\"account_id\" = 18 OR (\"account_id\" = 19 OR \"account_id\" = 20) OR (\"account_id\" = 21 OR (\"account_id\" = 22 OR \"account_id\" = 23))))");
    }

    @Test
    void enrichEnrichWithSubquery(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("SELECT * FROM shares.accounts as b where b.account_id IN (select c.account_id from shares.transactions as c limit 1)");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT * FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" WHERE \"account_id\" IN (SELECT \"account_id\" FROM (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_actual\" WHERE \"sys_from\" <= 1) AS \"t8\" LIMIT 1)");
    }

    @Test
    void enrichWithMultipleSchemas(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchema("SELECT a.account_id FROM accounts a " +
                "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                "JOIN test_datamart.transactions t ON t.account_id = a.account_id");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"t3\".\"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" INNER JOIN (SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2) AS \"t8\" ON \"t3\".\"account_id\" = \"t8\".\"account_id\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2) AS \"t13\" ON \"t3\".\"account_id\" = \"t13\".\"account_id\"");
    }

    @Test
    void enrichWithCustomSelect(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "SELECT *, 0 FROM shares.accounts ORDER BY account_id");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\", \"account_type\", 0 AS \"EXPR__2\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" ORDER BY \"account_id\"");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimit(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchema(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"t3\".\"account_id\", \"t3\".\"account_type\", \"t8\".\"account_id\" AS \"account_id0\", \"t8\".\"account_type\" AS \"account_type0\", \"t13\".\"transaction_id\", \"t13\".\"transaction_date\", \"t13\".\"account_id\" AS \"account_id1\", \"t13\".\"amount\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" INNER JOIN (SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2) AS \"t8\" ON \"t3\".\"account_id\" = \"t8\".\"account_id\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2) AS \"t13\" ON \"t3\".\"account_id\" = \"t13\".\"account_id\" LIMIT 10");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimitOrderBy(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchema(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"t3\".\"account_id\", \"t3\".\"account_type\", \"t8\".\"account_id\" AS \"account_id0\", \"t8\".\"account_type\" AS \"account_type0\", \"t13\".\"transaction_id\", \"t13\".\"transaction_date\", \"t13\".\"account_id\" AS \"account_id1\", \"t13\".\"amount\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" INNER JOIN (SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2) AS \"t8\" ON \"t3\".\"account_id\" = \"t8\".\"account_id\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2) AS \"t13\" ON \"t3\".\"account_id\" = \"t13\".\"account_id\" ORDER BY \"t8\".\"account_id\" LIMIT 10");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimitOrderByGroupBy(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchema(
                "select aa.account_id from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id GROUP BY aa.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"t8\".\"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" INNER JOIN (SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2) AS \"t8\" ON \"t3\".\"account_id\" = \"t8\".\"account_id\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2) AS \"t13\" ON \"t3\".\"account_id\" = \"t13\".\"account_id\" GROUP BY \"t8\".\"account_id\" ORDER BY \"t8\".\"account_id\" LIMIT 10");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimitOrderByGroupByAndAggregate(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchema(
                "select aa.account_id, count(*), 1 as K from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id GROUP BY aa.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"t8\".\"account_id\", COUNT(*) AS \"EXPR__1\", 1 AS \"k\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" INNER JOIN (SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2) AS \"t8\" ON \"t3\".\"account_id\" = \"t8\".\"account_id\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2) AS \"t13\" ON \"t3\".\"account_id\" = \"t13\".\"account_id\" GROUP BY \"t8\".\"account_id\" ORDER BY \"t8\".\"account_id\" LIMIT 10");
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
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT * FROM (SELECT \"t4\".\"bc1\", \"t4\".\"bc2\", \"t4\".\"bc3\", \"t4\".\"bc4\", \"t4\".\"EXPR__4\", \"t10\".\"transaction_id\", \"t10\".\"transaction_date\", \"t10\".\"account_id\", \"t10\".\"amount\" FROM (SELECT \"account_id\" AS \"bc1\", TRUE AS \"bc2\", ABS(\"account_id\") AS \"bc3\", 'some$' AS \"bc4\", '$some' AS \"EXPR__4\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\") AS \"t4\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\", ABS(\"account_id\") AS \"__f4\" FROM (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_actual\" WHERE \"sys_from\" <= 1) AS \"t9\") AS \"t10\" ON \"t4\".\"bc1\" = \"t10\".\"account_id\" AND \"t4\".\"bc3\" = \"t10\".\"__f4\") AS \"t11\" WHERE \"t11\".\"bc2\" = TRUE AND \"t11\".\"bc3\" = 0");
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
                .compose(parserResponse -> enrichService.enrich(enrichQueryRequest, parserResponse))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        Assertions.fail("Unexpected success");
                    }

                    Assertions.assertSame(DataSourceException.class, ar.cause().getClass());
                }).completeNow());
    }


    @Test
    void getEnrichedSqlWithDeltaNum(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum("SELECT account_id FROM shares.accounts");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\"");
    }

    @Test
    void getEnrichedSqlWithFinishedIn(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaFinishedIn("SELECT account_id FROM shares.accounts");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM \"local__shares__accounts_history\" WHERE \"sys_to\" >= 0 AND (\"sys_to\" <= 0 AND \"sys_op\" = 1)");
    }

    @Test
    void getEnrichedSqlWithDeltaInterval(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaInterval("select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                "OR (account_type = 'C' AND  amount <= 0) THEN 'OK    ' ELSE 'NOT OK' END\n" +
                "  from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                "    from shares.accounts a\n" +
                "    left join shares.transactions t using(account_id)\n" +
                "   group by a.account_id, account_type\n" +
                ")x");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"t3\".\"account_id\", COALESCE(SUM(\"t5\".\"amount\"), 0) AS \"amount\", \"t3\".\"account_type\", CASE WHEN \"t3\".\"account_type\" = 'D' AND COALESCE(SUM(\"t5\".\"amount\"), 0) >= 0 OR \"t3\".\"account_type\" = 'C' AND COALESCE(SUM(\"t5\".\"amount\"), 0) <= 0 THEN 'OK    ' ELSE 'NOT OK' END AS \"EXPR__3\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" >= 1 AND \"sys_from\" <= 5 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" >= 1 AND \"sys_from\" <= 5) AS \"t3\" LEFT JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_history\" WHERE \"sys_to\" >= 2 AND (\"sys_to\" <= 3 AND \"sys_op\" = 1)) AS \"t5\" ON \"t3\".\"account_id\" = \"t5\".\"account_id\" GROUP BY \"t3\".\"account_id\", \"t3\".\"account_type\"");
    }

    @Test
    void getEnrichedSqlWithQuotes(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum("SELECT \"account_id\" FROM \"shares\".\"accounts\"");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\"");
    }

    @Test
    void getEnrichedSqlWithWhereCollate(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaWithoutSnapshot("SELECT account_id FROM shares.accounts WHERE account_type = 'type' COLLATE 'unicode_ci'");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" WHERE \"account_type\" = 'type' COLLATE \"unicode_ci\"");
    }

    @Test
    void getEnrichedSqlWithManyInKeyword(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "SELECT account_id FROM shares.accounts WHERE account_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" WHERE \"account_id\" = 1 OR \"account_id\" = 2 OR (\"account_id\" = 3 OR (\"account_id\" = 4 OR \"account_id\" = 5)) OR (\"account_id\" = 6 OR (\"account_id\" = 7 OR \"account_id\" = 8) OR (\"account_id\" = 9 OR (\"account_id\" = 10 OR \"account_id\" = 11))) OR (\"account_id\" = 12 OR (\"account_id\" = 13 OR \"account_id\" = 14) OR (\"account_id\" = 15 OR (\"account_id\" = 16 OR \"account_id\" = 17)) OR (\"account_id\" = 18 OR (\"account_id\" = 19 OR \"account_id\" = 20) OR (\"account_id\" = 21 OR (\"account_id\" = 22 OR \"account_id\" = 23))))");
    }

    @Test
    void getEnrichedSqlWithSubquery(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest =
                prepareRequestDeltaNum("SELECT * FROM shares.accounts as b where b.account_id IN (select c.account_id from shares.transactions as c limit 1)");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT * FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" WHERE \"account_id\" IN (SELECT \"account_id\" FROM (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_actual\" WHERE \"sys_from\" <= 1) AS \"t8\" LIMIT 1)");
    }

    @Test
    void getEnrichedSqlWithMultipleSchemas(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchema("SELECT a.account_id FROM accounts a " +
                "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                "JOIN test_datamart.transactions t ON t.account_id = a.account_id");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"t3\".\"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" INNER JOIN (SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2) AS \"t8\" ON \"t3\".\"account_id\" = \"t8\".\"account_id\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2) AS \"t13\" ON \"t3\".\"account_id\" = \"t13\".\"account_id\"");
    }

    @Test
    void getEnrichedSqlWithCustomSelect(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "SELECT *, 0 FROM shares.accounts ORDER BY account_id");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\", \"account_type\", 0 AS \"EXPR__2\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" ORDER BY \"account_id\"");
    }

    @Test
    void getEnrichedSqlWithMultipleLogicalSchemaLimit(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchema(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id LIMIT 10");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"t3\".\"account_id\", \"t3\".\"account_type\", \"t8\".\"account_id\" AS \"account_id0\", \"t8\".\"account_type\" AS \"account_type0\", \"t13\".\"transaction_id\", \"t13\".\"transaction_date\", \"t13\".\"account_id\" AS \"account_id1\", \"t13\".\"amount\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" INNER JOIN (SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2) AS \"t8\" ON \"t3\".\"account_id\" = \"t8\".\"account_id\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2) AS \"t13\" ON \"t3\".\"account_id\" = \"t13\".\"account_id\" LIMIT 10");
    }

    @Test
    void getEnrichedSqlWithMultipleLogicalSchemaLimitOrderBy(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchema(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"t3\".\"account_id\", \"t3\".\"account_type\", \"t8\".\"account_id\" AS \"account_id0\", \"t8\".\"account_type\" AS \"account_type0\", \"t13\".\"transaction_id\", \"t13\".\"transaction_date\", \"t13\".\"account_id\" AS \"account_id1\", \"t13\".\"amount\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" INNER JOIN (SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2) AS \"t8\" ON \"t3\".\"account_id\" = \"t8\".\"account_id\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2) AS \"t13\" ON \"t3\".\"account_id\" = \"t13\".\"account_id\" ORDER BY \"t8\".\"account_id\" LIMIT 10");
    }

    @Test
    void getEnrichedSqlWithMultipleLogicalSchemaLimitOrderByGroupBy(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchema(
                "select aa.account_id from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id GROUP BY aa.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"t8\".\"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" INNER JOIN (SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2) AS \"t8\" ON \"t3\".\"account_id\" = \"t8\".\"account_id\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2) AS \"t13\" ON \"t3\".\"account_id\" = \"t13\".\"account_id\" GROUP BY \"t8\".\"account_id\" ORDER BY \"t8\".\"account_id\" LIMIT 10");
    }

    @Test
    void getEnrichedSqlWithMultipleLogicalSchemaLimitOrderByGroupByAndAggregate(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestMultipleSchema(
                "select aa.account_id, count(*), 1 as K from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id GROUP BY aa.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT \"t8\".\"account_id\", COUNT(*) AS \"EXPR__1\", 1 AS \"k\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" INNER JOIN (SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares_2__accounts_actual\" WHERE \"sys_from\" <= 2) AS \"t8\" ON \"t3\".\"account_id\" = \"t8\".\"account_id\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_history\" WHERE \"sys_from\" <= 2 AND \"sys_to\" >= 2 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__test_datamart__transactions_actual\" WHERE \"sys_from\" <= 2) AS \"t13\" ON \"t3\".\"account_id\" = \"t13\".\"account_id\" GROUP BY \"t8\".\"account_id\" ORDER BY \"t8\".\"account_id\" LIMIT 10");
    }

    @Test
    void getEnrichedSqlWithAliasesAndFunctions(VertxTestContext testContext) {
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
        getEnrichedSqlAndAssert(testContext, enrichQueryRequest, "SELECT * FROM (SELECT \"t4\".\"bc1\", \"t4\".\"bc2\", \"t4\".\"bc3\", \"t4\".\"bc4\", \"t4\".\"EXPR__4\", \"t10\".\"transaction_id\", \"t10\".\"transaction_date\", \"t10\".\"account_id\", \"t10\".\"amount\" FROM (SELECT \"account_id\" AS \"bc1\", TRUE AS \"bc2\", ABS(\"account_id\") AS \"bc3\", 'some$' AS \"bc4\", '$some' AS \"EXPR__4\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\") AS \"t4\" INNER JOIN (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\", ABS(\"account_id\") AS \"__f4\" FROM (SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"transaction_id\", \"transaction_date\", \"account_id\", \"amount\" FROM \"local__shares__transactions_actual\" WHERE \"sys_from\" <= 1) AS \"t9\") AS \"t10\" ON \"t4\".\"bc1\" = \"t10\".\"account_id\" AND \"t4\".\"bc3\" = \"t10\".\"__f4\") AS \"t11\" WHERE \"t11\".\"bc2\" = TRUE AND \"t11\".\"bc3\" = 0");
    }

    @Test
    void shouldFailgetEnrichedSqlWhenNotEnoughDeltas(VertxTestContext testContext) {
        // arrange
        EnrichQueryRequest enrichQueryRequest = prepareRequestDeltaNum(
                "SELECT t1.account_id FROM shares.accounts t1" +
                        " join shares.accounts t2 on t2.account_id = t1.account_id" +
                        " join shares.accounts t3 on t3.account_id = t2.account_id" +
                        " where t1.account_id = 10"
        );

        // act assert
        queryParserService.parse(new QueryParserRequest(enrichQueryRequest.getQuery(), enrichQueryRequest.getSchema()))
                .compose(parserResponse -> enrichService.getEnrichedSqlNode(enrichQueryRequest, parserResponse))
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
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM (SELECT \"account_id\", \"account_type\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t\" ORDER BY \"account_id\" LIMIT 1) AS \"t5\" UNION ALL SELECT * FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t\") AS \"t12\"");
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
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT * FROM (SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" ORDER BY \"account_id\" LIMIT 1) AS \"t6\" WHERE \"account_id\" = 0 UNION ALL SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t12\"");
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
        enrichAndAssert(testContext, enrichQueryRequest, "SELECT \"account_id\" FROM (SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t3\" WHERE \"account_id\" = 0 ORDER BY \"account_id\" LIMIT 1) AS \"t7\" UNION ALL SELECT \"account_id\" FROM (SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_history\" WHERE \"sys_from\" <= 1 AND \"sys_to\" >= 1 UNION ALL SELECT \"account_id\", \"account_type\" FROM \"local__shares__accounts_actual\" WHERE \"sys_from\" <= 1) AS \"t13\"");
    }

    private EnrichQueryRequest prepareRequestDeltaWithoutSnapshot(String sql) {
        val datamartName = "shares";
        Datamart datamart = new Datamart();
        datamart.setMnemonic(datamartName);
        datamart.setIsDefault(true);
        datamart.setEntities(Collections.singletonList(Entity.builder()
                .name("accounts")
                .schema(datamartName)
                .entityType(EntityType.TABLE)
                .destination(Collections.singleton(SourceType.ADG))
                .fields(Arrays.asList(EntityField.builder()
                                .ordinalPosition(0)
                                .name("account_id")
                                .type(ColumnType.INT)
                                .nullable(false)
                                .primaryOrder(1)
                                .shardingOrder(1)
                                .build(),
                        EntityField.builder()
                                .ordinalPosition(1)
                                .name("account_type")
                                .type(ColumnType.VARCHAR)
                                .size(36)
                                .nullable(true)
                                .build()))
                .build()));
        List<Datamart> datamarts = Collections.singletonList(datamart);
        String schemaName = datamarts.get(0).getMnemonic();

        List<DeltaInformation> deltaInforamtions = Collections.singletonList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(datamarts.get(0).getEntities().get(0).getName())
                        .selectOnNum(1L)
                        .build()
        );

        return EnrichQueryRequest.builder()
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .schema(datamarts)
                .build();
    }

    private EnrichQueryRequest prepareRequestMultipleSchema(String sql) {
        List<Datamart> datamarts = Arrays.asList(
                getSchema("shares", true),
                getSchema("shares_2", false),
                getSchema("test_datamart", false));
        String defaultSchema = datamarts.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(defaultSchema)
                        .tableName(datamarts.get(0).getEntities().get(0).getName())
                        .pos(pos)
                        .selectOnNum(1L)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("aa")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(2L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(datamarts.get(1).getMnemonic())
                        .tableName(datamarts.get(1).getEntities().get(1).getName())
                        .pos(pos)
                        .build(),
                DeltaInformation.builder()
                        .tableAlias("t")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(2L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(datamarts.get(2).getMnemonic())
                        .tableName(datamarts.get(2).getEntities().get(1).getName())
                        .pos(pos)
                        .build()
        );

        return EnrichQueryRequest.builder()
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .schema(datamarts)
                .build();
    }

    private EnrichQueryRequest prepareRequestDeltaNum(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
        String schemaName = datamarts.get(0).getMnemonic();
        List<DeltaInformation> deltaInforamtions = Arrays.asList(
                DeltaInformation.builder()
                        .tableAlias("a")
                        .deltaTimestamp("2019-12-23 15:15:14")
                        .isLatestUncommittedDelta(false)
                        .selectOnNum(1L)
                        .selectOnInterval(null)
                        .type(DeltaType.NUM)
                        .schemaName(schemaName)
                        .tableName(datamarts.get(0).getEntities().get(0).getName())
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
                        .tableName(datamarts.get(0).getEntities().get(1).getName())
                        .build()
        );

        return EnrichQueryRequest.builder()
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .schema(datamarts)
                .build();
    }

    private EnrichQueryRequest prepareRequestDeltaInterval(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
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
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
                .schema(datamarts)
                .build();
    }

    private EnrichQueryRequest prepareRequestDeltaFinishedIn(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema("shares", true));
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
                .query(TestUtils.DEFINITION_SERVICE.processingQuery(sql))
                .deltaInformations(deltaInforamtions)
                .envName(ENV_NAME)
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
                .compose(parserResponse -> enrichService.enrich(enrichRequest, parserResponse))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        Assertions.fail(ar.cause());
                    }

                    assertNormalizedEquals(ar.result(), expectedSql);
                }).completeNow());
    }

    private void getEnrichedSqlAndAssert(VertxTestContext testContext, EnrichQueryRequest enrichRequest,
                                         String expectedSql) {
        queryParserService.parse(new QueryParserRequest(enrichRequest.getQuery(), enrichRequest.getSchema()))
                .compose(parserResponse -> enrichService.getEnrichedSqlNode(enrichRequest, parserResponse))
                .map(sqlNode -> Util.toLinux(sqlNode.toSqlString(sqlDialect).getSql())
                        .replaceAll("\r\n|\r|\n", " "))
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        Assertions.fail(ar.cause());
                    }

                    assertNormalizedEquals(ar.result(), expectedSql);
                }).completeNow());
    }
}
