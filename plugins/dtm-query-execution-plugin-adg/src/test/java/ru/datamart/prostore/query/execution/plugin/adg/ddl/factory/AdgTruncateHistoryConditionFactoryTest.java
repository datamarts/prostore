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
package ru.datamart.prostore.query.execution.plugin.adg.ddl.factory;

import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdgTruncateHistoryConditionFactoryTest {

    private final AdgCalciteConfiguration calciteConfiguration = new AdgCalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration
            .configDdlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    private final AdgTruncateHistoryConditionFactory conditionFactory = new AdgTruncateHistoryConditionFactory(calciteConfiguration.adgSqlDialect());

    @Test
    void createEmptyRequest() {
        TruncateHistoryRequest request = TruncateHistoryRequest.builder()
                .build();
        assertEquals("", conditionFactory.create(request));
    }

    @Test
    void createSysCnRequest() {
        long sysCn = 1L;
        String expectedResult = String.format("\"sys_to\" < %d", sysCn);
        TruncateHistoryRequest request = TruncateHistoryRequest.builder()
                .sysCn(sysCn)
                .build();
        assertEquals(expectedResult, conditionFactory.create(request));
    }

    @Test
    void createConditionCnRequest() {
        String expectedResult = "(\"id\" = 1)";
        TruncateHistoryRequest request = TruncateHistoryRequest.builder()
                .conditions(getConditionNode(expectedResult))
                .build();
        assertEquals(expectedResult, conditionFactory.create(request));
    }

    @Test
    void createSysCnConditionCnRequest() {
        String expectedResult = "(\"id\" = 1) AND \"sys_to\" < 1";
        TruncateHistoryRequest request = TruncateHistoryRequest.builder()
                .sysCn(1L)
                .conditions(getConditionNode("id = 1"))
                .build();
        assertEquals(expectedResult, conditionFactory.create(request));
    }

    private SqlNode getConditionNode(String conditions) {
        try {
            return ((SqlSelect) planner.parse(String.format("SELECT * from t WHERE %s", conditions)))
                    .getWhere();
        } catch (SqlParseException e) {
            throw new DataSourceException("Error", e);
        }
    }
}