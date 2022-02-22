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
package ru.datamart.prostore.query.calcite.core.visitors;

import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.dialect.LimitSqlDialect;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.calcite.core.service.impl.CalciteDefinitionService;
import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class SqlDollarReplacementShuttleTest {
    private static final DefinitionService<SqlNode> DEFINITION_SERVICE =
            new CalciteDefinitionService(
                    SqlParser.configBuilder()
                            .setParserFactory(new CalciteCoreConfiguration().eddlParserImplFactory())
                            .setConformance(SqlConformanceEnum.DEFAULT)
                            .setLex(Lex.MYSQL)
                            .setCaseSensitive(false)
                            .setUnquotedCasing(Casing.TO_LOWER)
                            .setQuotedCasing(Casing.TO_LOWER)
                            .setQuoting(Quoting.DOUBLE_QUOTE)
                            .build()) {
            };

    private final SqlDollarReplacementShuttle sqlDollarReplacementShuttle = new SqlDollarReplacementShuttle();

    @Test
    void shouldReplaceAllDollars() {
        // arrange (absolutely synthetic)
        val sql = "SELECT *, 1 as \"tbl.$f1\", 2 as \"expr.$expr2\", '$string' as \"$f2\"\n" +
                "FROM tbl a\n" +
                "JOIN tbl2 b\n" +
                "ON \"a.$some1\" = \"b$.$som$e2\"\n" +
                "WHERE \"$a.$b.$c$expr\" > ABS(\"a.$f1\") AND (SELECT true as expr$1 FROM tbl3 c WHERE c.expr$1 AND abs(\"c.$f1\") > \"b.$f1\") AND 'notchange$1' = \"a.$f2\"";

        SqlNode sqlNode = DEFINITION_SERVICE.processingQuery(sql);

        // act
        SqlNode result = sqlNode.accept(sqlDollarReplacementShuttle);

        // assert
        String resultSqlString = result.toSqlString(new LimitSqlDialect(SqlDialect.EMPTY_CONTEXT)).toString();
        Assertions.assertThat(resultSqlString).isEqualToIgnoringNewLines("SELECT *, 1 AS tbl.__f1, 2 AS expr.__expr2, '$string' AS __f2\n" +
                "FROM tbl AS a\n" +
                "INNER JOIN tbl2 AS b ON a.__some1 = b__.__som__e2\n" +
                "WHERE __a.__b.__c__expr > ABS(a.__f1) AND (SELECT TRUE AS expr__1\n" +
                "FROM tbl3 AS c\n" +
                "WHERE c.expr__1 AND ABS(c.__f1) > b.__f1) AND 'notchange$1' = a.__f2");
    }
}