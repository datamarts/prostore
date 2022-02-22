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
package ru.datamart.prostore.query.execution.plugin.adqm.converter;

import ru.datamart.prostore.query.execution.plugin.adqm.base.service.converter.AdqmPluginSpecificLiteralConverter;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdqmPluginSpecificLiteralConverterTest {
    private static final List<SqlNode> EXPECTED = Lists.newArrayList(
            SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric("-1577869199999999", SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric("54000000000", SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric("-18263", SqlParserPos.ZERO)
    );
    private final AdqmPluginSpecificLiteralConverter converter = new AdqmPluginSpecificLiteralConverter();

    @Test
    void convert() {
        List<SqlNode> actual = converter.convert(
                Lists.newArrayList(
                        SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
                        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                        SqlLiteral.createCharString("1920-01-01 15:00:00.000001", SqlParserPos.ZERO),
                        SqlLiteral.createCharString("15:00:00", SqlParserPos.ZERO),
                        SqlLiteral.createCharString("1920-01-01", SqlParserPos.ZERO)
                ),
                Lists.newArrayList(
                        SqlTypeName.BOOLEAN,
                        SqlTypeName.BOOLEAN,
                        SqlTypeName.TIMESTAMP,
                        SqlTypeName.TIME,
                        SqlTypeName.DATE
                )
        );
        assertEquals(EXPECTED, actual);
    }
}
