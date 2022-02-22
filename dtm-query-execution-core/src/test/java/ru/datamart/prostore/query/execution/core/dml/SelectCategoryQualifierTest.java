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
package ru.datamart.prostore.query.execution.core.dml;

import ru.datamart.prostore.common.dml.SelectCategory;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.core.base.exception.table.ValidationDtmException;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.dml.service.SelectCategoryQualifier;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SelectCategoryQualifierTest {

    private static final String SELECT_LEFT_JOIN = "SELECT *\n" +
            "FROM transactions t \n" +
            "  LEFT JOIN accounts a ON t.account_id = a.account_id\n" +
            "WHERE a.account_type = 'D'";
    private static final String SELECT_JOIN = "SELECT sum(t.amount)\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON t.account_id = a.account_id\n" +
            "GROUP BY a.account_type";
    private static final String SELECT_SUBQUERY = "SELECT \n" +
            "  (SELECT sum(amount) FROM transactions t WHERE t.account_id = a.account_id)\n" +
            "FROM accounts a \n" +
            "WHERE a.account_type <> 'D' AND a.account_id = 5";
    private static final String SELECT_LIMIT_SUBQUERY = "SELECT \n" +
            "  (SELECT sum(amount) FROM transactions t WHERE t.account_id = a.account_id LIMIT 1)\n" +
            "FROM accounts a \n" +
            "WHERE a.account_type <> 'D' AND a.account_id = 5";
    private static final String SELECT_SUBQUERY_WHERE = "SELECT count(*)\n" +
            "FROM accounts a \n" +
            "WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id)";
    private static final String SELECT_SUBQUERY_WHERE_AND = "SELECT count(*)\n" +
            "FROM accounts a \n" +
            "WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) AND account_id > 0";
    private static final String SELECT_GROUP_BY = "SELECT sum(t.amount)\n" +
            "FROM transactions t\n" +
            "GROUP BY t.transaction_date";
    private static final String SELECT_AGGREGATION = "SELECT count(*)\n" +
            "FROM transactions t";
    private static final String SELECT_GROUP_BY_HAVING = "SELECT account_id\n" +
            "FROM transactions t\n" +
            "GROUP BY account_id\n" +
            "HAVING count(*) > 1";
    private static final String SELECT_PRIMARY_KEY_EQUALS = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.transaction_id = 1 AND amount > 0";
    private static final String SELECT_PRIMARY_KEY_IN = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.transaction_id IN (1,2,3) AND amount < 0";
    private static final String SELECT_PRIMARY_KEY_BETWEEN = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE (t.transaction_id > 1 AND t.transaction_id < 10) OR t.transaction_id BETWEEN 1001 AND 2000";
    private static final String SELECT_UNDEFINED = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE amount < 0";
    private static final String SELECT_OR_UNDEFINED = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE int_col = 1 OR amount < 0";
    private static final String SELECT_LEFT_JOIN_LIMIT = "SELECT *\n" +
            "FROM transactions t \n" +
            "  LEFT JOIN accounts a ON t.account_id = a.account_id\n" +
            "WHERE a.account_type = 'D'" +
            "LIMIT 1";
    private static final String SELECT_JOIN_LIMIT = "SELECT sum(t.amount)\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON t.account_id = a.account_id\n" +
            "GROUP BY a.account_type " +
            "LIMIT 1";
    private static final String SELECT_SUBQUERY_LIMIT = "SELECT \n" +
            "  (SELECT sum(amount) FROM transactions t WHERE t.account_id = a.account_id)\n" +
            "FROM accounts a \n" +
            "WHERE a.account_type <> 'D' AND a.account_id = 5" +
            "LIMIT 1";
    private static final String SELECT_SUBQUERY_WHERE_LIMIT = "SELECT count(*)\n" +
            "FROM accounts a \n" +
            "WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id)" +
            "LIMIT 1";
    private static final String SELECT_SUBQUERY_WHERE_AND_LIMIT = "SELECT count(*)\n" +
            "FROM accounts a \n" +
            "WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) AND account_id > 0" +
            "LIMIT 1";
    private static final String SELECT_GROUP_BY_LIMIT = "SELECT sum(t.amount)\n" +
            "FROM transactions t\n" +
            "GROUP BY t.transaction_date " +
            "LIMIT 1";
    private static final String SELECT_AGGREGATION_LIMIT = "SELECT count(*)\n" +
            "FROM transactions t " +
            "LIMIT 1";
    private static final String SELECT_GROUP_BY_HAVING_LIMIT = "SELECT account_id\n" +
            "FROM transactions t\n" +
            "GROUP BY account_id\n" +
            "HAVING count(*) > 1" +
            "LIMIT 1";
    private static final String SELECT_PRIMARY_KEY_EQUALS_LIMIT = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.transaction_id = 1 AND amount > 0" +
            "LIMIT 1";
    private static final String SELECT_PRIMARY_KEY_IN_LIMIT = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.transaction_id IN (1,2,3) AND amount < 0" +
            "LIMIT 1";
    private static final String SELECT_PRIMARY_KEY_BETWEEN_LIMIT = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE (t.transaction_id > 1 AND t.transaction_id < 10) OR t.transaction_id BETWEEN 1001 AND 2000" +
            "LIMIT 1";
    private static final String SELECT_UNDEFINED_LIMIT = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE amount < 0" +
            "LIMIT 1";
    private static final String SELECT_OR_UNDEFINED_LIMIT = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE int_col = 1 OR amount < 0\n" +
            "LIMIT 1";
    private static final String SELECT_LEFT_JOIN_ORDER_BY = "SELECT *\n" +
            "FROM transactions t \n" +
            "  LEFT JOIN accounts a ON t.account_id = a.account_id\n" +
            "WHERE a.account_type = 'D' " +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_JOIN_ORDER_BY = "SELECT sum(t.amount)\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON t.account_id = a.account_id\n" +
            "GROUP BY a.account_type " +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_SUBQUERY_ORDER_BY = "SELECT \n" +
            "  (SELECT sum(amount) FROM transactions t WHERE t.account_id = a.account_id)\n" +
            "FROM accounts a \n" +
            "WHERE a.account_type <> 'D' AND a.account_id = 5" +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_SUBQUERY_WHERE_ORDER_BY = "SELECT count(*)\n" +
            "FROM accounts a \n" +
            "WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) " +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_SUBQUERY_WHERE_AND_ORDER_BY = "SELECT count(*)\n" +
            "FROM accounts a \n" +
            "WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) AND account_id > 0 " +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_GROUP_BY_ORDER_BY = "SELECT sum(t.amount)\n" +
            "FROM transactions t\n" +
            "GROUP BY t.transaction_date " +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_AGGREGATION_ORDER_BY = "SELECT count(*)\n" +
            "FROM transactions t " +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_AGGREGATION_AS = "SELECT count(*) as K\n" +
            "FROM transactions t";
    private static final String SELECT_GROUP_BY_HAVING_ORDER_BY = "SELECT account_id\n" +
            "FROM transactions t\n" +
            "GROUP BY account_id\n" +
            "HAVING count(*) > 1 " +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_PRIMARY_KEY_EQUALS_ORDER_BY = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.transaction_id = 1 AND amount > 0 " +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_PRIMARY_KEY_IN_ORDER_BY = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.transaction_id IN (1,2,3) AND amount < 0" +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_PRIMARY_KEY_BETWEEN_ORDER_BY = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE (t.transaction_id > 1 AND t.transaction_id < 10) OR t.transaction_id BETWEEN 1001 AND 2000 " +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_UNDEFINED_ORDER_BY = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE amount < 0 " +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_OR_UNDEFINED_ORDER_BY = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE int_col = 1 OR amount < 0 " +
            "ORDER BY transaction_id ASC";
    private static final String SELECT_UNDEFINED_WITHOUT_WHERE = "SELECT *\n" +
            "FROM transactions t";
    private static final String SELECT_UNION = "SELECT * FROM transactions UNION ALL SELECT * FROM transactions";
    private static final String SELECT_UNION_SUBQUERY = "SELECT * FROM (SELECT * FROM transactions UNION ALL SELECT * FROM transactions)";
    private static final String DATAMART = "datamart";

    private final SelectCategoryQualifier selectCategoryQualifier = new SelectCategoryQualifier();
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private Planner planner;
    private List<Datamart> schema;

    @BeforeEach
    public void setup() {
        EntityField transactionId = EntityField.builder()
                .name("transaction_id")
                .primaryOrder(1)
                .build();
        Entity transactionsEntity = Entity.builder()
                .name("transactions")
                .fields(Collections.singletonList(transactionId))
                .build();

        schema = Collections.singletonList(Datamart.builder()
                .mnemonic(DATAMART)
                .entities(Collections.singletonList(transactionsEntity))
                .build());

        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    }

    @Test
    void testSelectUndefined() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_UNDEFINED);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.UNDEFINED, category);
    }

    @Test
    void testSelectOrUndefined() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_OR_UNDEFINED);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.UNDEFINED, category);
    }

    @Test
    void testSelectLeftJoin() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_LEFT_JOIN);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectJoin() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_JOIN);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectSubquery() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY);
        assertThrows(ValidationDtmException.class, () -> selectCategoryQualifier.qualify(schema, sqlNode));
    }

    @Test
    void testSelectLimitSubquery() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_LIMIT_SUBQUERY);
        assertThrows(ValidationDtmException.class, () -> selectCategoryQualifier.qualify(schema, sqlNode));
    }

    @Test
    void testSelectSubqueryWhere() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY_WHERE);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectSubqueryWhereAnd() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY_WHERE_AND);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectGroupBy() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_GROUP_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectAggregation() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_AGGREGATION);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectGroupByHaving() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_GROUP_BY_HAVING);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectPrimaryKeyEquals() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_EQUALS);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.DICTIONARY, category);
    }

    @Test
    void testSelectPrimaryKeyIn() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_IN);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.DICTIONARY, category);
    }

    @Test
    void testSelectPrimaryKeyBetween() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_BETWEEN);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.DICTIONARY, category);
    }

    @Test
    void testSelectUndefinedLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_UNDEFINED_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.UNDEFINED, category);
    }

    @Test
    void testSelectOrUndefinedLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_OR_UNDEFINED_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.UNDEFINED, category);
    }

    @Test
    void testSelectLeftJoinLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_LEFT_JOIN_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectJoinLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_JOIN_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectSubqueryLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY_LIMIT);
        assertThrows(ValidationDtmException.class, () -> selectCategoryQualifier.qualify(schema, sqlNode));
    }

    @Test
    void testSelectSubqueryWhereLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY_WHERE_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectSubqueryWhereAndLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY_WHERE_AND_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectGroupByLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_GROUP_BY_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectAggregationLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_AGGREGATION_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectGroupByHavingLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_GROUP_BY_HAVING_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectPrimaryKeyEqualsLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_EQUALS_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.DICTIONARY, category);
    }

    @Test
    void testSelectPrimaryKeyInLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_IN_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.DICTIONARY, category);
    }

    @Test
    void testSelectPrimaryKeyBetweenLimit() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_BETWEEN_LIMIT);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.DICTIONARY, category);
    }
    //--------

    @Test
    void testSelectUndefinedOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_UNDEFINED_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.UNDEFINED, category);
    }

    @Test
    void testSelectOrUndefinedOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_OR_UNDEFINED_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.UNDEFINED, category);
    }

    @Test
    void testSelectLeftJoinOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_LEFT_JOIN_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectJoinOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_JOIN_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectSubqueryOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY_ORDER_BY);
        assertThrows(ValidationDtmException.class, () -> selectCategoryQualifier.qualify(schema, sqlNode));
    }

    @Test
    void testSelectSubqueryWhereOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY_WHERE_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectSubqueryWhereAndOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY_WHERE_AND_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectGroupByOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_GROUP_BY_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectAggregationOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_AGGREGATION_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectAggregationAs() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_AGGREGATION_AS);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectGroupByHavingOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_GROUP_BY_HAVING_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectPrimaryKeyEqualsOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_EQUALS_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.DICTIONARY, category);
    }

    @Test
    void testSelectPrimaryKeyInOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_IN_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.DICTIONARY, category);
    }

    @Test
    void testSelectPrimaryKeyBetweenOrder() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_BETWEEN_ORDER_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.DICTIONARY, category);
    }

    @Test
    void testSelectWithoutWhere() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_UNDEFINED_WITHOUT_WHERE);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.UNDEFINED, category);
    }

    @Test
    void testSelectUnion() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_UNION);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.UNDEFINED, category);
    }

    @Test
    void testSelectUnionSubquery() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_UNION_SUBQUERY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertSame(SelectCategory.UNDEFINED, category);
    }
}
