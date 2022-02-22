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
package ru.datamart.prostore.query.calcite.core.util;

import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.calcite.core.service.impl.CalciteDefinitionService;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlNodeUtilTest {
    public static final DefinitionService<SqlNode> DEFINITION_SERVICE =
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

    @Test
    void copySimpleSelect() {
        assertCopied("SELECT count(*) from dtm_918.accounts");
    }

    @Test
    void copyUncommitDelta() {
        assertCopied(
                "SELECT count(*) from dtm_918.accounts FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA"
        );
    }

    @Test
    void copySimpleSelectWhere() {
        assertCopied("SELECT count(*) from dtm_918.accounts where id = 1");
    }

    @Test
    void copySimpleSelectJoinWhere() {
        assertCopied("SELECT t1.id, t1.name as name, 10 as code from dtm_918.accounts t1" +
                " join transactions t2 on t2.id = t1.id where id = 1");
    }

    @Test
    void copySimpleSelectJoinWhereLimit() {
        assertCopied("SELECT t1.id, t1.name as name, 10 as code from dtm_918.accounts t1" +
                " join transactions FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA t2 on t2.id = t1.id where id = 1 limit 10");
    }

    @Test
    void copySimpleSelectJoinWhereOrderBy() {
        assertCopied("SELECT t1.id, t1.name as name, 10 as code from dtm_918.accounts t1" +
                " join transactions FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA t2 on t2.id = t1.id where id = 1" +
                " order by t1.id");
    }

    @Test
    void copyWithWhereSubQuery() {
        assertCopied("select * from dtm.table1 a join table3 c on c.id = (select a2.id from dtm.table1 a2 where a2.id = 10 limit 1) and c.id < 20 where a.id in (select b.id from table2 b where b.id > 10)");
    }

    private void assertCopied(String sql) {
        SqlNode expected = DEFINITION_SERVICE.processingQuery(sql);
        SqlNode actual = SqlNodeUtil.copy(expected);
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
}
