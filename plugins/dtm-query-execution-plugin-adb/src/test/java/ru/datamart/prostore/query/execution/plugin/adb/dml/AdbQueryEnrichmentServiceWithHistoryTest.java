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

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.delta.SelectOnInterval;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbDmlQueryExtendWithHistoryService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbQueryEnrichmentService;
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
class AdbQueryEnrichmentServiceWithHistoryTest {

    public static final String SHARES_SCHEMA_NAME = "shares";
    public static final String SHARES_2_SCHEMA_NAME = "shares_2";
    public static final String TEST_DATAMART_SCHEMA_NAME = "test_datamart";
    public static final String ENV_NAME = "test";
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final QueryExtendService queryExtender = new AdbDmlQueryExtendWithHistoryService();
    private final AdbCalciteContextProvider contextProvider = new AdbCalciteContextProvider(
            calciteConfiguration.configDdlParser(calciteConfiguration.ddlParserImplFactory()),
            new AdbCalciteSchemaFactory(new AdbSchemaFactory()));
    private final SqlDialect sqlDialect = calciteConfiguration.adbSqlDialect();
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
    private final AdbSchemaExtender schemaExtender = new AdbSchemaExtender();
    private final QueryParserService queryParserService = new AdbCalciteDMLQueryParserService(contextProvider, Vertx.vertx(), schemaExtender);
    private final QueryEnrichmentService adbQueryEnrichmentService = new AdbQueryEnrichmentService(queryExtender, calciteConfiguration.adbSqlDialect(), relToSqlConverter);

    @Test
    void testEnrichWithDeltaNum(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture =
                prepareRequestDeltaNum("select * from shares.accounts");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1");
    }

    @Test
    void testEnrichWithCountAndLimit(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture =
                prepareRequestDeltaNum("SELECT COUNT(*) AS C FROM shares.accounts LIMIT 100");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT COUNT(*) AS c FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 LIMIT 100");
    }

    @Test
    void testEnrichWithSubquery(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture =
                prepareRequestDeltaNum("SELECT * FROM shares.accounts as b where b.account_id IN (select c.account_id from shares.transactions as c limit 1)");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT * FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 WHERE account_id IN (SELECT account_id FROM (SELECT transaction_id, transaction_date, account_id, amount FROM shares.transactions_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT transaction_id, transaction_date, account_id, amount FROM shares.transactions_actual WHERE sys_from <= 1) AS t8 LIMIT 1)");
    }

    @Test
    void testEnrichWithCountAndGroupByLimit(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture =
                prepareRequestDeltaNum("SELECT account_id, COUNT(*) AS C FROM shares.accounts GROUP BY account_id LIMIT 100");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id, COUNT(*) AS c FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 GROUP BY account_id LIMIT 100");
    }

    @Test
    void testEnrichWithCountAndGroupByAndOrderByLimit(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture =
                prepareRequestDeltaNum("SELECT account_id, COUNT(*) AS C FROM shares.accounts GROUP BY account_id ORDER BY account_id LIMIT 100");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id, COUNT(*) AS c FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 GROUP BY account_id ORDER BY account_id LIMIT 100");
    }

    @Test
    void testEnrichWithAggregate(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture =
                prepareRequestDeltaNum("SELECT COUNT(*) AS C FROM shares.accounts");


        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT COUNT(*) AS c FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3");
    }

    @Test
    void testEnrichWithFunctionInJoin(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture =
                prepareRequestDeltaNum("SELECT * FROM shares.accounts a JOIN shares.transactions t ON ABS(a.account_id) = ABS(t.account_id) WHERE a.account_id > 0");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT * FROM (SELECT t4.account_id, t4.account_type, t10.transaction_id, t10.transaction_date, t10.account_id AS account_id0, t10.amount FROM (SELECT account_id, account_type, ABS(account_id) AS __f2 FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3) AS t4 INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount, ABS(account_id) AS __f4 FROM (SELECT transaction_id, transaction_date, account_id, amount FROM shares.transactions_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT transaction_id, transaction_date, account_id, amount FROM shares.transactions_actual WHERE sys_from <= 1) AS t9) AS t10 ON t4.__f2 = t10.__f4) AS t11 WHERE t11.account_id > 0");
    }

    @Test
    void enrichWithDeltaNum(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaNum(
                "select *, (CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END) as c\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT t3.account_id, COALESCE(SUM(t8.amount), 0) AS amount, t3.account_type, CASE WHEN t3.account_type = 'D' AND COALESCE(SUM(t8.amount), 0) >= 0 OR t3.account_type = 'C' AND COALESCE(SUM(t8.amount), 0) <= 0 THEN 'OK' ELSE 'NOT OK' END AS c FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 LEFT JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM shares.transactions_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT transaction_id, transaction_date, account_id, amount FROM shares.transactions_actual WHERE sys_from <= 1) AS t8 ON t3.account_id = t8.account_id GROUP BY t3.account_id, t3.account_type");
    }

    @Test
    void enrichWithFinishedIn(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaFinishedIn(
                "SELECT account_id FROM shares.accounts");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id FROM shares.accounts_history WHERE sys_to >= 0 AND (sys_to <= 0 AND sys_op = 1)");
    }

    @Test
    void enrichWithStaticCaseExpressions(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaNum(
                "select a.account_id, (case when a.account_type = 'D' then 'ok' else 'not ok' end) as ss " +
                        "from shares.accounts a ");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id, CASE WHEN account_type = 'D' THEN 'ok' ELSE 'not ok' END AS ss FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3");
    }

    @Test
    void enrichWithDeltaInterval(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaInterval(
                "select *, CASE WHEN (account_type = 'D' AND  amount >= 0) " +
                        "OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                        "  from (\n" +
                        "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                        "    from shares.accounts a\n" +
                        "    left join shares.transactions t using(account_id)\n" +
                        "   group by a.account_id, account_type\n" +
                        ")x");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT t3.account_id, COALESCE(SUM(t5.amount), 0) AS amount, t3.account_type, CASE WHEN t3.account_type = 'D' AND COALESCE(SUM(t5.amount), 0) >= 0 OR t3.account_type = 'C' AND COALESCE(SUM(t5.amount), 0) <= 0 THEN 'OK' ELSE 'NOT OK' END AS EXPR__3 FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from >= 1 AND sys_from <= 5 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5) AS t3 LEFT JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM shares.transactions_history WHERE sys_to >= 2 AND (sys_to <= 3 AND sys_op = 1)) AS t5 ON t3.account_id = t5.account_id GROUP BY t3.account_id, t3.account_type");
    }

    @Test
    void enrichWithNull(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaInterval(
                "select account_id, null, null from shares.accounts");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id, NULL AS EXPR__1, NULL AS EXPR__2 FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from >= 1 AND sys_from <= 5 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5) AS t3");
    }

    @Test
    void enrichWithLimit(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaInterval(
                "select account_id from shares.accounts limit 50");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from >= 1 AND sys_from <= 5 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5) AS t3 LIMIT 50");
    }

    @Test
    void enrichWithLimitAndOrder(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaInterval(
                "select account_id from shares.accounts order by account_id limit 50");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from >= 1 AND sys_from <= 5 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5) AS t3 ORDER BY account_id LIMIT 50");
    }

    @Test
    void enrichWithManyInKeyword(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaNum(
                "SELECT account_id FROM shares.accounts WHERE account_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 WHERE account_id = 1 OR account_id = 2 OR (account_id = 3 OR (account_id = 4 OR account_id = 5)) OR (account_id = 6 OR (account_id = 7 OR account_id = 8) OR (account_id = 9 OR (account_id = 10 OR account_id = 11))) OR (account_id = 12 OR (account_id = 13 OR account_id = 14) OR (account_id = 15 OR (account_id = 16 OR account_id = 17)) OR (account_id = 18 OR (account_id = 19 OR account_id = 20) OR (account_id = 21 OR (account_id = 22 OR account_id = 23))))");
    }

    @Test
    void enrichWithLimitOffset(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaInterval(
                "select account_id from shares.accounts limit 30 offset 50");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from >= 1 AND sys_from <= 5 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5) AS t3 LIMIT 30 OFFSET 50");
    }

    @Test
    void enrichWithOffsetFetchNext(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaInterval(
                "select account_id from shares.accounts fetch next 30 rows only offset 50");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from >= 1 AND sys_from <= 5 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from >= 1 AND sys_from <= 5) AS t3 LIMIT 30 OFFSET 50");
    }

    @Test
    void enrichWithMultipleLogicalSchema(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestMultipleSchemas(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT t3.account_id, t3.account_type, t8.account_id AS account_id0, t8.account_type AS account_type0, t13.transaction_id, t13.transaction_date, t13.account_id AS account_id1, t13.amount FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 INNER JOIN (SELECT account_id, account_type FROM shares_2.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares_2.accounts_actual WHERE sys_from <= 1) AS t8 ON t3.account_id = t8.account_id INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_actual WHERE sys_from <= 1) AS t13 ON t3.account_id = t13.account_id");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimit(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestMultipleSchemas(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT t3.account_id, t3.account_type, t8.account_id AS account_id0, t8.account_type AS account_type0, t13.transaction_id, t13.transaction_date, t13.account_id AS account_id1, t13.amount FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 INNER JOIN (SELECT account_id, account_type FROM shares_2.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares_2.accounts_actual WHERE sys_from <= 1) AS t8 ON t3.account_id = t8.account_id INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_actual WHERE sys_from <= 1) AS t13 ON t3.account_id = t13.account_id LIMIT 10");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimitOrderBy(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestMultipleSchemas(
                "select * from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT t3.account_id, t3.account_type, t8.account_id AS account_id0, t8.account_type AS account_type0, t13.transaction_id, t13.transaction_date, t13.account_id AS account_id1, t13.amount FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 INNER JOIN (SELECT account_id, account_type FROM shares_2.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares_2.accounts_actual WHERE sys_from <= 1) AS t8 ON t3.account_id = t8.account_id INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_actual WHERE sys_from <= 1) AS t13 ON t3.account_id = t13.account_id ORDER BY t8.account_id LIMIT 10");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimitOrderByGroupBy(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestMultipleSchemas(
                "select aa.account_id from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id GROUP BY aa.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT t8.account_id FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 INNER JOIN (SELECT account_id, account_type FROM shares_2.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares_2.accounts_actual WHERE sys_from <= 1) AS t8 ON t3.account_id = t8.account_id INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_actual WHERE sys_from <= 1) AS t13 ON t3.account_id = t13.account_id GROUP BY t8.account_id ORDER BY t8.account_id LIMIT 10");
    }

    @Test
    void enrichWithMultipleLogicalSchemaLimitOrderByGroupByAndAggregate(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestMultipleSchemas(
                "select aa.account_id, count(*), 1 as K from accounts a " +
                        "JOIN shares_2.accounts aa ON aa.account_id = a.account_id " +
                        "JOIN test_datamart.transactions t ON t.account_id = a.account_id GROUP BY aa.account_id ORDER BY aa.account_id LIMIT 10");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT t8.account_id, COUNT(*) AS EXPR__1, 1 AS k FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 INNER JOIN (SELECT account_id, account_type FROM shares_2.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares_2.accounts_actual WHERE sys_from <= 1) AS t8 ON t3.account_id = t8.account_id INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT transaction_id, transaction_date, account_id, amount FROM test_datamart.transactions_actual WHERE sys_from <= 1) AS t13 ON t3.account_id = t13.account_id GROUP BY t8.account_id ORDER BY t8.account_id LIMIT 10");
    }

    @Test
    void enrichWithCustomSelect(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaNum(
                "SELECT *, 0 FROM shares.accounts ORDER BY account_id");

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT account_id, account_type, 0 AS EXPR__2 FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 ORDER BY account_id");
    }

    @Test
    void enrichWithAliasesAndFunctions(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaNum("SELECT *\n" +
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
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT * FROM (SELECT t4.bc1, t4.bc2, t4.bc3, t4.bc4, t4.EXPR__4, t10.transaction_id, t10.transaction_date, t10.account_id, t10.amount FROM (SELECT account_id AS bc1, TRUE AS bc2, ABS(account_id) AS bc3, 'some$' AS bc4, '$some' AS EXPR__4 FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3) AS t4 INNER JOIN (SELECT transaction_id, transaction_date, account_id, amount, ABS(account_id) AS __f4 FROM (SELECT transaction_id, transaction_date, account_id, amount FROM shares.transactions_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT transaction_id, transaction_date, account_id, amount FROM shares.transactions_actual WHERE sys_from <= 1) AS t9) AS t10 ON t4.bc1 = t10.account_id AND t4.bc3 = t10.__f4) AS t11 WHERE t11.bc2 = TRUE AND t11.bc3 = 0");
    }

    @Test
    void shouldFailWhenNotEnoughDeltas(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequestDeltaNum(
                "SELECT t1.account_id FROM shares.accounts t1" +
                        " join shares.accounts t2 on t2.account_id = t1.account_id" +
                        " join shares.accounts t3 on t3.account_id = t2.account_id" +
                        " where t1.account_id = 10"
        );

        // act assert
        enrichQueryRequestFuture.compose(adbQueryEnrichmentService::enrich)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        Assertions.fail("Unexpected success");
                    }

                    Assertions.assertSame(DataSourceException.class, ar.cause().getClass());
                }).completeNow());
    }

    @Test
    void shouldFailEnrichWithWritableStandaloneTable(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequest("SELECT * FROM standalone_writable a WHERE a.id > 0",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build()));

        // act assert
        enrichQueryRequestFuture.compose(adbQueryEnrichmentService::enrich)
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
        val enrichQueryRequestFuture = prepareRequest("SELECT * FROM standalone a WHERE a.id > 0",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT * FROM schema.tbl WHERE id > 0");
    }

    @Test
    void testEnrichWithStandaloneTableWithSystemColumns(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequest("SELECT * FROM standalone a WHERE a.id > 0 and a.sys_op > 0",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT * FROM schema.tbl WHERE id > 0 AND sys_op > 0");
    }

    @Test
    void testEnrichJoinStandaloneTables(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequest("SELECT * FROM standalone a JOIN standalone b ON a.id = b.id WHERE a.id > 0",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT * FROM schema.tbl INNER JOIN schema.tbl AS tbl0 ON tbl.id = tbl0.id WHERE tbl.id > 0");
    }

    @Test
    void testEnrichJoinStandaloneAndLogical(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequest("SELECT * FROM shares.standalone a JOIN shares.accounts b ON a.id = b.account_id WHERE a.id > 0",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT * FROM schema.tbl INNER JOIN (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 ON tbl.id = t3.account_id WHERE tbl.id > 0");
    }

    @Test
    void testEnrichJoinLogicalAndStandalone(VertxTestContext testContext) {
        // arrange
        val enrichQueryRequestFuture = prepareRequest("SELECT * FROM shares.accounts b JOIN shares.standalone a ON a.id = b.account_id WHERE a.id > 0",
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build(),
                        DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build()));

        // act assert
        enrichAndAssert(testContext, enrichQueryRequestFuture, "SELECT * FROM (SELECT account_id, account_type FROM shares.accounts_history WHERE sys_from <= 1 AND sys_to >= 1 UNION ALL SELECT account_id, account_type FROM shares.accounts_actual WHERE sys_from <= 1) AS t3 INNER JOIN schema.tbl ON t3.account_id = tbl.id WHERE tbl.id > 0");
    }

    private Future<EnrichQueryRequest> prepareRequestMultipleSchemas(String sql) {
        List<Datamart> schemas = Arrays.asList(
                getSchema(SHARES_SCHEMA_NAME, true),
                getSchema(SHARES_2_SCHEMA_NAME, false),
                getSchema(TEST_DATAMART_SCHEMA_NAME, false));
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInformations = Arrays.asList(
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
        val query = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        return queryParserService.parse(new QueryParserRequest(query, schemas, ENV_NAME))
                .map(parserResponse -> EnrichQueryRequest.builder()
                        .envName(ENV_NAME)
                        .deltaInformations(deltaInformations)
                        .calciteContext(parserResponse.getCalciteContext())
                        .relNode(parserResponse.getRelNode())
                        .build());
    }

    private Future<EnrichQueryRequest> prepareRequest(String sql, List<DeltaInformation> deltaInformations) {
        List<Datamart> schemas = Collections.singletonList(getSchema(SHARES_SCHEMA_NAME, true));
        String schemaName = schemas.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        val query = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        return queryParserService.parse(new QueryParserRequest(query, schemas, ENV_NAME))
                .map(parserResponse -> EnrichQueryRequest.builder()
                        .envName(ENV_NAME)
                        .deltaInformations(deltaInformations)
                        .calciteContext(parserResponse.getCalciteContext())
                        .relNode(parserResponse.getRelNode())
                        .build());
    }

    private Future<EnrichQueryRequest> prepareRequestDeltaNum(String sql) {
        List<Datamart> schemas = Collections.singletonList(getSchema(SHARES_SCHEMA_NAME, true));
        String schemaName = schemas.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInformations = Arrays.asList(
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
        val query = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        return queryParserService.parse(new QueryParserRequest(query, schemas, ENV_NAME))
                .map(parserResponse -> EnrichQueryRequest.builder()
                        .envName(ENV_NAME)
                        .deltaInformations(deltaInformations)
                        .calciteContext(parserResponse.getCalciteContext())
                        .relNode(parserResponse.getRelNode())
                        .build());
    }

    private Future<EnrichQueryRequest> prepareRequestDeltaInterval(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema(SHARES_SCHEMA_NAME, true));
        String schemaName = datamarts.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInformations = Arrays.asList(
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
        val query = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        return queryParserService.parse(new QueryParserRequest(query, datamarts, ENV_NAME))
                .map(parserResponse -> EnrichQueryRequest.builder()
                        .envName(ENV_NAME)
                        .deltaInformations(deltaInformations)
                        .calciteContext(parserResponse.getCalciteContext())
                        .relNode(parserResponse.getRelNode())
                        .build());
    }

    private Future<EnrichQueryRequest> prepareRequestDeltaFinishedIn(String sql) {
        List<Datamart> datamarts = Collections.singletonList(getSchema(SHARES_SCHEMA_NAME, true));
        String schemaName = datamarts.get(0).getMnemonic();
        SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInformations = Collections.singletonList(
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
        val query = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        return queryParserService.parse(new QueryParserRequest(query, datamarts, ENV_NAME))
                .map(parserResponse -> EnrichQueryRequest.builder()
                        .envName(ENV_NAME)
                        .deltaInformations(deltaInformations)
                        .calciteContext(parserResponse.getCalciteContext())
                        .relNode(parserResponse.getRelNode())
                        .build());
    }

    private Datamart getSchema(String schemaName, boolean isDefault) {
        Entity accounts = Entity.builder()
                .schema(schemaName)
                .name("accounts")
                .entityType(EntityType.TABLE)
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

        Entity standalone = Entity.builder()
                .schema(schemaName)
                .name("standalone")
                .entityType(EntityType.READABLE_EXTERNAL_TABLE)
                .externalTableLocationPath("schema.tbl")
                .build();

        Entity standaloneWritable = Entity.builder()
                .schema(schemaName)
                .name("standalone_writable")
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .externalTableLocationPath("schema.tbl")
                .build();
        List<EntityField> standaloneFields = Arrays.asList(
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("id")
                        .ordinalPosition(1)
                        .shardingOrder(1)
                        .primaryOrder(1)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.VARCHAR)
                        .name("name")
                        .ordinalPosition(2)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(false)
                        .accuracy(null)
                        .size(1)
                        .build(),
                EntityField.builder()
                        .type(ColumnType.BIGINT)
                        .name("sys_op")
                        .ordinalPosition(2)
                        .shardingOrder(null)
                        .primaryOrder(null)
                        .nullable(false)
                        .accuracy(null)
                        .size(null)
                        .build()
        );
        standalone.setFields(standaloneFields);
        standaloneWritable.setFields(standaloneFields);

        Entity transactions = Entity.builder()
                .schema(schemaName)
                .name("transactions")
                .entityType(EntityType.TABLE)
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

        return new Datamart(schemaName, isDefault, Arrays.asList(transactions, accounts, standalone, standaloneWritable));
    }

    private void enrichAndAssert(VertxTestContext testContext, Future<EnrichQueryRequest> enrichRequestFuture,
                                 String expectedSql) {
        enrichRequestFuture.compose(adbQueryEnrichmentService::enrich)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        Assertions.fail(ar.cause());
                    }

                    assertNormalizedEquals(ar.result(), expectedSql);
                }).completeNow());
    }
}
