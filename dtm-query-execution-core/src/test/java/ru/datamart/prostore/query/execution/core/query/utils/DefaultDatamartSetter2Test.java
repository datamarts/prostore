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
package ru.datamart.prostore.query.execution.core.query.utils;

import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Litmus;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDefinitionService;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultDatamartSetter2Test {

    private static final String DEFAULT_DATAMART = "test";
    private static final String SELECT_SQL = "SELECT * FROM [datamart]all_types_table";
    private static final String SELECT_WITH_HINT_SQL = "SELECT * FROM [datamart]all_types_table DATASOURCE_TYPE = 'ADB'";
    private static final String SELECT_GROUP_BY_SQL = "SELECT * FROM [datamart]all_types_table GROUP BY id";
    private static final String SELECT_ORDER_BY_SQL = "SELECT * FROM [datamart]all_types_table ORDER BY id";
    private static final String SELECT_FOR_SYSTEM_TIME_STARTED_IN_SQL = "SELECT * FROM [datamart]all_types_table FOR SYSTEM_TIME STARTED IN (0,0)";
    private static final String SELECT_WITH_SNAPSHOT_FINISHED_IN_SQL = "SELECT * FROM [datamart]all_types_table FOR SYSTEM_TIME FINISHED IN (0,0)";
    private static final String SELECT_FOR_SYSTEM_TIME_AS_OF_TIMESTAMP_SQL = "SELECT * FROM [datamart]all_types_table FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'";
    private static final String SELECT_FOR_SYSTEM_TIME_AS_OF_LATEST_UNCOMMITTED_DELTA_SQL = "SELECT * FROM [datamart]all_types_table FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA";
    private static final String SELECT_FOR_SYSTEM_TIME_AS_OF_DELTA_NUM_SQL = "SELECT * FROM [datamart]all_types_table FOR SYSTEM_TIME AS OF DELTA_NUM 0";
    private static final String SELECT_WITH_CONDITION_SQL = "SELECT * FROM [datamart]all_types_table WHERE id = 1";
    private static final String SELECT_LIMIT_SQL = "SELECT * FROM [datamart]all_types_table LIMIT 1";
    private static final String SELECT_JOIN_SQL = "SELECT * FROM [datamart]all_types_table1 a1" +
            " JOIN [datamart]all_types_table1 a2 ON a1.id = a2.id";

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));

    @Test
    void testSelect(){
        test(SELECT_SQL);
    }

    @Test
    void testSelectWithHint(){
        test(SELECT_WITH_HINT_SQL);
    }

    @Test
    void testSelectGroupBy(){
        test(SELECT_GROUP_BY_SQL);
    }

    @Test
    void testSelectOrderBy(){
        test(SELECT_ORDER_BY_SQL);
    }

    @Test
    void testSelectForSystemTimeStartedIn(){
        test(SELECT_FOR_SYSTEM_TIME_STARTED_IN_SQL);
    }

    @Test
    void testSelectForSystemTimeFinishedIn(){
        test(SELECT_WITH_SNAPSHOT_FINISHED_IN_SQL);
    }

    @Test
    void testSelectForSystemTimeAsOfTimestamp(){
        test(SELECT_FOR_SYSTEM_TIME_AS_OF_TIMESTAMP_SQL);
    }

    @Test
    void testSelectForSystemTimeAsOfLatestUncommitedDelta(){
        test(SELECT_FOR_SYSTEM_TIME_AS_OF_LATEST_UNCOMMITTED_DELTA_SQL);
    }

    @Test
    void testSelectForSystemTimeAsOfDeltaNum(){
        test(SELECT_FOR_SYSTEM_TIME_AS_OF_DELTA_NUM_SQL);
    }

    @Test
    void testSelectWithCondition(){
        test(SELECT_WITH_CONDITION_SQL);
    }

    @Test
    void testSelectLimit(){
        test(SELECT_LIMIT_SQL);
    }

    @Test
    void testSelectJoin(){
        test(SELECT_JOIN_SQL);
    }

    private void test(String sql) {
        val actualSqlNode = definitionService.processingQuery(sqlWithoutDatamart(sql));
        DefaultDatamartSetter.mutateNode(actualSqlNode, DEFAULT_DATAMART);
        val expectedSqlNode = definitionService.processingQuery(sqlWithDatamart(sql));
        assertTrue(expectedSqlNode.equalsDeep(actualSqlNode, Litmus.IGNORE));
    }

    private String sqlWithoutDatamart(String template) {
        return template.replace("[datamart]", "");
    }

    private String sqlWithDatamart(String template) {
        return template.replace("[datamart]", DEFAULT_DATAMART + ".");
    }
}
