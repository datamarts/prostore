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
package ru.datamart.prostore.query.execution.core.calcite.edml;

import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.extension.edml.SqlRollbackCrashedWriteOps;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlEdmlParserImplTest {

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(calciteConfiguration.configEddlParser(
                    calciteCoreConfiguration.eddlParserImplFactory()));

    @Test
    void parseRollbackCrashedWriteOpsSuccess() {
        SqlNode sqlNode = definitionService.processingQuery("ROLLBACK CRASHED_WRITE_OPERATIONS");
        assertTrue(sqlNode instanceof SqlRollbackCrashedWriteOps);
    }

    @Test
    void parseRollbackCrashedWriteOpsError() {
        assertThrows(SqlParseException.class, () -> {
            definitionService.processingQuery("ROLLBACK CRASHED_WRITE_OPERATIONS()");
        });
    }

}
