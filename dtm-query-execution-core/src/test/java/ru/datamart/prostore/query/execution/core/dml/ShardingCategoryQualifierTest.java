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

import ru.datamart.prostore.common.dml.ShardingCategory;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.dml.service.ShardingCategoryQualifier;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;

class ShardingCategoryQualifierTest {

    private static final String SELECT_WITHOUT_SHARDING_KEY = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE amount > 0";
    private static final String SELECT_WITH_SHARDING_KEY_GREATER_LESS = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.col1 > 1 AND t.col1 < 3";
    private static final String SELECT_WITH_2_SHARDING_KEYS_EQUALS = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.col1 = 1 AND t.col2 = 2 AND amount > 0";
    private static final String SELECT_WITH_2_SHARDING_KEYS_2_CONDITION_GROUPS = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE (t.col1 = 1 AND t.col2 = 2) OR ((t.col1 = 1 AND t.col2 = 1) AND amount > 0)";
    private static final String SELECT_WITH_2_SHARDING_KEYS_IN = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.col1 IN (1, 2, 3) AND t.col2 IN (4, 5) AND amount > 0";
    private static final String SELECT_WITH_2_SHARDING_KEYS_SINGLE_IN = "SELECT * FROM transactions t " +
            "WHERE t.col1 IN (1) AND t.col2 IN (2) AND amount > 0";
    private static final String SELECT_WITH_2_SHARDING_KEYS_2_CONDITION_GROUPS_OR = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE (t.col1 = 1 AND t.col2 = 2) OR amount > 0 ";
    private static final String SELECT_WITH_1_SHARDING_KEY = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.col1 = 1 ";
    private static final String SELECT_WITH_2_TABLES_ALL_SHARDING_KEYS_EQUAL = "SELECT *\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON a.id = t.account_id\n" +
            "WHERE t.col1 = 1 AND t.col2 = 2 AND a.colA = 1 AND t.colB = 2";
    private static final String SELECT_WITH_2_TABLES_ALL_SHARDING_KEYS_EQUAL_OR = "SELECT *\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON a.id = t.account_id\n" +
            "WHERE (t.col1 = 1 AND t.col2 = 2) OR (a.colA = 1 AND t.colB = 2)";
    private static final String SELECT_WITH_2_TABLES_ALL_SHARDING_KEYS_EQUAL_TYPE_MISMATCH = "SELECT *\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON a.id = t.account_id\n" +
            "WHERE t.col1 = 1 AND t.col2 = 2 AND a.colA = 1 AND t.colB = 2";
    private static final String SELECT_WITH_2_TABLES_ALL_SHARDING_KEYS_DIFFERENT_SHARDING_KEYS = "SELECT *\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON a.id = t.account_id\n" +
            "WHERE t.col1 = 1 AND t.col2 = 2 AND a.colB = 1";
    private static final String SELECT_WITH_2_TABLES_1_SHARDING_KEY = "SELECT *\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON a.colB = t.col1 \n" +
            "WHERE t.col1 = 1";
    private static final String SELECT_WITH_2_TABLES_JOIN_BY_SHARDING_KEY_DIFFERENT_SHARDING_KEYS = "SELECT *\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON a.colB = t.col1 \n" +
            "WHERE t.col1 = 1 AND t.col2 = 1";
    private static final String SELECT_WITH_2_TABLES_JOIN_BY_SHARDING_KEY = "SELECT *\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON a.colB = t.col1 \n" +
            "WHERE t.col1 = 1";
    private static final String SELECT_WITH_NON_SHARDING_KEY_IN_SUBQUERY_WITH_SHARDING_KEY_DIFFERENT_SHARDING_KEYS = "SELECT *\n" +
            "FROM transactions t \n" +
            "WHERE t.account_id IN (\n" +
            "  SELECT id \n" +
            "  FROM accounts a\n" +
            "  WHERE a.colB = 3)\n" +
            "AND t.col1 = 1 AND t.col2 = 2";
    private static final String SELECT_WITH_NON_SHARDING_KEY_IN_SUBQUERY_WITH_SHARDING_KEY = "SELECT *\n" +
            "FROM transactions t \n" +
            "WHERE t.account_id IN (\n" +
            "  SELECT id \n" +
            "  FROM accounts a\n" +
            "  WHERE a.colA = 1 AND a.colB = 2)\n" +
            "AND t.col1 = 1 AND t.col2 = 2";
    private static final String SELECT_WITH_2_TABLES_ALL_SHARDING_KEYS_OR = "SELECT *\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON a.id = t.account_id\n" +
            "WHERE t.col1 = 1 OR a.colB = 1";
    private static final String SELECT_WITH_SHARDING_KEY_IN_SUBQUERY_WITH_SHARDING_KEY = "select * from transactions\n" +
            "where col1 IN (\n" +
            "  select id\n" +
            "  from accounts\n" +
            "  where colA = 1\n" +
            ")";
    private static final String SELECT_WITH_SHARDING_KEY_IN_SUBQUERY = "select * from transactions\n" +
            "where col1 IN (\n" +
            "  select id\n" +
            "  from accounts\n" +
            ")";
    private static final String SELECT_WITH_PAIR_SHARDING_KEY_IN_SUBQUERY = "SELECT * \n" +
            "FROM transactions t\n" +
            "WHERE (col1, col2) IN (\n" +
            "  select id, colA\n" +
            "  from accounts\n" +
            "  where colA = 1\n" +
            ") AND t.amount > 0";

    private static final String SELECT_SHARDING_KEY_IS_TRUE = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE booleanColumn IS TRUE";

    private static final String SELECT_SHARDING_KEY_IS_NOT_TRUE = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE booleanColumn IS NOT TRUE";

    private static final String SELECT_SHARDING_KEY_IS_NULL = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE booleanColumn IS NULL";

    private static final String DATAMART = "datamart";

    private final ShardingCategoryQualifier shardingCategoryQualifier = new ShardingCategoryQualifier();
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private Planner planner;
    private EntityField col1;
    private EntityField col2;
    private EntityField cola;
    private EntityField colb;
    private EntityField booleanColumn;

    @BeforeEach
    public void setup() {
        col1 = EntityField.builder()
                .name("col1")
                .shardingOrder(1)
                .type(ColumnType.INT)
                .build();
        col2 = EntityField.builder()
                .name("col2")
                .shardingOrder(2)
                .type(ColumnType.INT)
                .build();
        cola = EntityField.builder()
                .name("colA")
                .shardingOrder(1)
                .type(ColumnType.INT)
                .build();
        colb = EntityField.builder()
                .name("colB")
                .shardingOrder(2)
                .type(ColumnType.INT)
                .build();
        booleanColumn = EntityField.builder()
                .name("booleanColumn")
                .shardingOrder(2)
                .type(ColumnType.BOOLEAN)
                .build();

        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    }

    @Test
    void testSelectWithoutShardingKey() throws SqlParseException {
        val schema = createSchemaWithTransactions(Collections.singletonList(col1));
        testSame(schema, SELECT_WITHOUT_SHARDING_KEY, ShardingCategory.SHARD_ALL);
    }

    @Test
    void testSelectWithShardingKeyGreaterLess() throws SqlParseException {
        val schema = createSchemaWithTransactions(Collections.singletonList(col1));
        testSame(schema, SELECT_WITH_SHARDING_KEY_GREATER_LESS, ShardingCategory.SHARD_ALL);
    }

    @Test
    void testSelectWith2ShardingKeysEquals() throws SqlParseException {
        val schema = createSchemaWithTransactions(Arrays.asList(col1, col2));
        testSame(schema, SELECT_WITH_2_SHARDING_KEYS_EQUALS, ShardingCategory.SHARD_ONE);
    }

    @Test
    void testSelectWith2ShardingKeys2ConditionGroups() throws SqlParseException {
        val schema = createSchemaWithTransactions(Arrays.asList(col1, col2));
        testSame(schema, SELECT_WITH_2_SHARDING_KEYS_2_CONDITION_GROUPS, ShardingCategory.SHARD_SET);
    }

    @Test
    void testSelectWith2ShardingKeysIn() throws SqlParseException {
        val schema = createSchemaWithTransactions(Arrays.asList(col1, col2));
        testSame(schema, SELECT_WITH_2_SHARDING_KEYS_IN, ShardingCategory.SHARD_SET);
    }

    @Test
    void testSelectWith2ShardingKeysSingleIn() throws SqlParseException {
        val schema = createSchemaWithTransactions(Arrays.asList(col1, col2));
        testSame(schema, SELECT_WITH_2_SHARDING_KEYS_SINGLE_IN, ShardingCategory.SHARD_ONE);
    }

    @Test
    void testSelectWith2ShardingKeys2ConditionGroupsOr() throws SqlParseException {
        val schema = createSchemaWithTransactions(Arrays.asList(col1, col2));
        testSame(schema, SELECT_WITH_2_SHARDING_KEYS_2_CONDITION_GROUPS_OR, ShardingCategory.SHARD_ALL);
    }

    @Test
    void testSelectWith1ShardingKeys() throws SqlParseException {
        val schema = createSchemaWithTransactions(Arrays.asList(col1, col2));
        testSame(schema, SELECT_WITH_1_SHARDING_KEY, ShardingCategory.SHARD_ALL);
    }

    @Test
    void testSelectWith2TablesAllShardingKeysEqual() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1, col2), Arrays.asList(cola, colb));
        testSame(schema, SELECT_WITH_2_TABLES_ALL_SHARDING_KEYS_EQUAL, ShardingCategory.SHARD_ONE);
    }

    @Test
    void testSelectWith2TablesAllShardingKeysEqualOr() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1, col2), Arrays.asList(cola, colb));
        testSame(schema, SELECT_WITH_2_TABLES_ALL_SHARDING_KEYS_EQUAL_OR, ShardingCategory.SHARD_ALL);
    }

    @Test
    void testSelectWith2TablesAllShardingKeysEqualTypeMismatch() throws SqlParseException {
        EntityField colaWithType = cola;
        colaWithType.setType(ColumnType.BOOLEAN);
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1, col2), Arrays.asList(colaWithType, colb));
        testSame(schema, SELECT_WITH_2_TABLES_ALL_SHARDING_KEYS_EQUAL_TYPE_MISMATCH, ShardingCategory.SHARD_SET);
    }

    @Test
    void testSelectWith2TablesAllShardingKeysDifferentShardingKeys() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1, col2), Arrays.asList(colb));
        testSame(schema, SELECT_WITH_2_TABLES_ALL_SHARDING_KEYS_DIFFERENT_SHARDING_KEYS, ShardingCategory.SHARD_SET);
    }

    @Test
    void testSelectWith2Tables1ShardingKey() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1, col2), Arrays.asList(colb));
        testSame(schema, SELECT_WITH_2_TABLES_1_SHARDING_KEY, ShardingCategory.SHARD_ALL);
    }

    @Test
    void testSelectWith2TablesJoinShardingKeyDifferentShardingKeys() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1, col2), Arrays.asList(colb));
        testSame(schema, SELECT_WITH_2_TABLES_JOIN_BY_SHARDING_KEY_DIFFERENT_SHARDING_KEYS, ShardingCategory.SHARD_SET);
    }

    @Test
    void testSelectWith2TablesJoinShardingKey() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1), Arrays.asList(colb));
        testSame(schema, SELECT_WITH_2_TABLES_JOIN_BY_SHARDING_KEY, ShardingCategory.SHARD_ONE);
    }

    @Test
    void testSelectWithNonShardingKeyInSubqueryWithShardingKeyDifferentShardingKeys() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1, col2), Arrays.asList(colb));
        testSame(schema, SELECT_WITH_NON_SHARDING_KEY_IN_SUBQUERY_WITH_SHARDING_KEY_DIFFERENT_SHARDING_KEYS, ShardingCategory.SHARD_SET);
    }

    @Test
    void testSelectWithNonShardingKeyInSubqueryWithShardingKey() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1, col2), Arrays.asList(cola, colb));
        testSame(schema, SELECT_WITH_NON_SHARDING_KEY_IN_SUBQUERY_WITH_SHARDING_KEY, ShardingCategory.SHARD_ONE);
    }

    @Test
    void testSelectWith2TablesAllShardingKeysOr() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1), Arrays.asList(colb));
        testSame(schema, SELECT_WITH_2_TABLES_ALL_SHARDING_KEYS_OR, ShardingCategory.SHARD_ALL);
    }

    @Test
    void testSelectWithShardingKeyInSubqueryWithShardingKey() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1), Arrays.asList(cola));
        testSame(schema, SELECT_WITH_SHARDING_KEY_IN_SUBQUERY_WITH_SHARDING_KEY, ShardingCategory.SHARD_SET);
    }

    @Test
    void testSelectWithShardingKeyInSubquery() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1), Arrays.asList(cola));
        testSame(schema, SELECT_WITH_SHARDING_KEY_IN_SUBQUERY, ShardingCategory.SHARD_ALL);
    }

    @Test
    void testSelectWithPairShardingKeyInSubquery() throws SqlParseException {
        val schema = createSchemaWithTransactionsAccounts(Arrays.asList(col1, col2), Arrays.asList(cola));
        testSame(schema, SELECT_WITH_PAIR_SHARDING_KEY_IN_SUBQUERY, ShardingCategory.SHARD_SET);
    }

    @Test
    void testSelectWithShardingKeyIsTrue() throws SqlParseException {
        val schema = createSchemaWithTransactions(Collections.singletonList(booleanColumn));
        testSame(schema, SELECT_SHARDING_KEY_IS_TRUE, ShardingCategory.SHARD_ONE);
    }

    @Test
    void testSelectWithShardingKeyIsNotTrue() throws SqlParseException {
        val schema = createSchemaWithTransactions(Collections.singletonList(booleanColumn));
        testSame(schema, SELECT_SHARDING_KEY_IS_NOT_TRUE, ShardingCategory.SHARD_ALL);
    }

    @Test
    void testSelectWithShardingKeyIsNull() throws SqlParseException {
        val schema = createSchemaWithTransactions(Collections.singletonList(booleanColumn));
        testSame(schema, SELECT_SHARDING_KEY_IS_NULL, ShardingCategory.SHARD_ONE);
    }

    private void testSame(List<Datamart> schema, String selectWithShardingKeyGreaterLess, ShardingCategory shardingCategory) throws SqlParseException {
        SqlNode sqlNode = planner.parse(selectWithShardingKeyGreaterLess);
        val category = shardingCategoryQualifier.qualify(schema, sqlNode);
        assertSame(shardingCategory, category);
    }

    private List<Datamart> createSchemaWithTransactions(List<EntityField> fields) {
        List<EntityField> entityFields = new ArrayList<>();
        entityFields.add(EntityField.builder()
                .name("transaction_id")
                .primaryOrder(1)
                .type(ColumnType.INT)
                .build());
        entityFields.addAll(fields);
        val transactionsEntity = Entity.builder()
                .name("transactions")
                .fields(entityFields)
                .build();

        return Collections.singletonList(Datamart.builder()
                .mnemonic(DATAMART)
                .entities(Collections.singletonList(transactionsEntity))
                .build());
    }

    private List<Datamart> createSchemaWithTransactionsAccounts(List<EntityField> transFields, List<EntityField> accountsFields) {
        List<EntityField> transEntityFields = new ArrayList<>();
        transEntityFields.add(EntityField.builder()
                .name("transaction_id")
                .primaryOrder(1)
                .type(ColumnType.INT)
                .build());
        transEntityFields.addAll(transFields);
        val transactionsEntity = Entity.builder()
                .name("transactions")
                .fields(transEntityFields)
                .build();

        List<EntityField> accEntityFields = new ArrayList<>();
        accEntityFields.add(EntityField.builder()
                .name("accounts_id")
                .primaryOrder(1)
                .type(ColumnType.INT)
                .build());
        accEntityFields.addAll(accountsFields);
        val accountsEntity = Entity.builder()
                .name("acciunts")
                .fields(accEntityFields)
                .build();

        return Collections.singletonList(Datamart.builder()
                .mnemonic(DATAMART)
                .entities(Arrays.asList(accountsEntity, transactionsEntity))
                .build());
    }
}
