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
package ru.datamart.prostore.query.execution.plugin.adqm.calcite;


import ru.datamart.prostore.calcite.adqm.configuration.AdqmCalciteConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.configuration.CalciteConfiguration;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SqlEddlParserTest {
    private static final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final AdqmCalciteConfiguration calciteCoreConfiguration = new AdqmCalciteConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
            calciteCoreConfiguration.eddlParserImplFactory()
    );

    @Test
    void parseSelectWithAliasFinal() throws SqlParseException {
        String expectedResult = "SELECT *\n" +
                "FROM test.pso AS t FINAL";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlSelect sqlNode = (SqlSelect) planner.parse("select * from test.pso t FINAL");
        assertNotNull(sqlNode);
        assertThat(sqlNode.toSqlString(SQL_DIALECT).toString()).isEqualToNormalizingNewlines(expectedResult);
    }

    @Test
    void parseSelectWithoutAliasFinal() throws SqlParseException {
        String expectedResult = "SELECT *\n" +
                "FROM test.pso FINAL";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlSelect sqlNode = (SqlSelect) planner.parse("select * from test.pso FINAL");
        assertNotNull(sqlNode);
        assertThat(sqlNode.toSqlString(SQL_DIALECT).toString()).isEqualToNormalizingNewlines(expectedResult);
    }

    @Test
    void parseSelectWithASFinal() throws SqlParseException {
        String expectedResult = "SELECT *\n" +
                "FROM test.pso AS t FINAL";
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlSelect sqlNode = (SqlSelect) planner.parse("select * from test.pso as t FINAL");
        assertNotNull(sqlNode);
        assertThat(sqlNode.toSqlString(SQL_DIALECT).toString()).isEqualToNormalizingNewlines(expectedResult);
    }


}
