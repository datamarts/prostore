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
package ru.datamart.prostore.query.execution.plugin.adqm.utils;

import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.calcite.core.service.impl.CalciteDefinitionService;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.configuration.CalciteConfiguration;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Assertions;

public class TestUtils {
    public static final CalciteConfiguration CALCITE_CONFIGURATION = new CalciteConfiguration();
    public static final DefinitionService<SqlNode> DEFINITION_SERVICE =
            new CalciteDefinitionService(CALCITE_CONFIGURATION.configDdlParser(CALCITE_CONFIGURATION.ddlParserImplFactory())) {
            };

    private TestUtils() {
    }

    public static void assertNormalizedEquals(String actual, String expected) {
        if (actual == null || expected == null) {
            Assertions.assertEquals(expected, actual);
            return;
        }

        String fixedActual = actual.replaceAll("\r\n|\r|\n", " ");
        String fixedExpected = expected.replaceAll("\r\n|\r|\n", " ");
        Assertions.assertEquals(fixedExpected, fixedActual);
    }

}
