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

import ru.datamart.prostore.common.reader.QueryTemplateResult;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.service.impl.CalciteDefinitionService;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreQueryTemplateExtractor;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryTemplateExtractorImplTest {
    private static final String EXPECTED_SQL = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = 1 AND \"x\" > 2 AND \"x\" < 3 AND \"x\" <= 4 AND \"x\" >= 5 AND \"x\" <> 6 AND \"z\" = '8'";

    private static final String EXPECTED_FULL_SQL = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = 1" +
            " AND 2 = 2" +
            " AND 3 < \"x\"" +
            " AND \"z\" = \"x\"";

    private static final String EXPECTED_SQL_WITH_IN = "SELECT *\n" +
            "FROM \"testdelta\".\"accounts\"\n" +
            "WHERE \"account_id\" IN (1, 2, 3)";
    private static final String EXPECTED_SQL_WITH_IN_AND_DYNAMIC_PARAMS = "SELECT *\n" +
            "FROM \"testdelta\".\"accounts\"\n" +
            "WHERE \"account_id\" IN (1, 2, ?)";
    private static final String EXPECTED_SQL_WITH_IN_TEMPLATE = "SELECT *\n" +
            "FROM \"testdelta\".\"accounts\"\n" +
            "WHERE \"account_id\" IN (?, ?, ?)";

    private static final String EXPECTED_SQL_WITH_NOT_IN = "SELECT *\n" +
            "FROM \"testdelta\".\"accounts\"\n" +
            "WHERE \"account_id\" NOT IN (1, 2, 3)";
    private static final String EXPECTED_SQL_WITH_NOT_IN_AND_DYNAMIC_PARAMS = "SELECT *\n" +
            "FROM \"testdelta\".\"accounts\"\n" +
            "WHERE \"account_id\" NOT IN (1, 2, ?)";
    private static final String EXPECTED_SQL_WITH_NOT_IN_TEMPLATE = "SELECT *\n" +
            "FROM \"testdelta\".\"accounts\"\n" +
            "WHERE \"account_id\" NOT IN (?, ?, ?)";

    private static final String EXPECTED_FULL_TEMPLATE = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = ? AND 2 = ? AND 3 < \"x\" AND \"z\" = \"x\"";

    private static final String EXPECTED_SQL_WITH_SYS_COLUMNS = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = 1 AND \"x\" > 2 AND \"x\" < 3 AND \"x\" <= 4 AND \"x\" >= 5 AND \"x\" <> 6 AND \"z\" = '8'" +
            " AND \"sys_from\" = 1";
    private static final String EXPECTED_BETWEEN_SQL = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" BETWEEN ASYMMETRIC 1 AND 5 AND \"z\" = \"x\"";
    private static final String EXPECTED_SUB_SQL = "SELECT *\n" +
            "FROM (SELECT *\n" +
            "FROM \"tbl1\" AS \"t2\"\n" +
            "WHERE \"t2\".\"x\" = 1 AND \"t2\".\"x\" > 2 AND \"t2\".\"x\" < 3 AND \"t2\".\"x\" <= 4 AND \"t2\".\"x\" >= 5 AND \"t2\".\"x\" <> 6 AND \"t2\".\"z\" = '8') AS \"t\"\n" +
            "WHERE \"x\" = 1 AND \"x\" > 2 AND \"x\" < 3 AND \"x\" <= 4 AND \"x\" >= 5 AND \"x\" <> 6 AND \"z\" = '8'";
    private static final String EXPECTED_TEMPLATE = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = ? AND \"x\" > ? AND \"x\" < ? AND \"x\" <= ? AND \"x\" >= ? AND \"x\" <> ? AND \"z\" = ?";
    private static final String EXPECTED_SUB_TEMPLATE = "SELECT *\n" +
            "FROM (SELECT *\n" +
            "FROM \"tbl1\" AS \"t2\"\n" +
            "WHERE \"t2\".\"x\" = ? AND \"t2\".\"x\" > ? AND \"t2\".\"x\" < ? AND \"t2\".\"x\" <= ? AND \"t2\".\"x\" >= ? AND \"t2\".\"x\" <> ? AND \"t2\".\"z\" = ?) AS \"t\"\n" +
            "WHERE \"x\" = ? AND \"x\" > ? AND \"x\" < ? AND \"x\" <= ? AND \"x\" >= ? AND \"x\" <> ? AND \"z\" = ?";
    private static final String EXPECTED_TEMPLATE_WITH_SYS_COLUMNS = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = ? AND \"x\" > ? AND \"x\" < ? AND \"x\" <= ? AND \"x\" >= ? AND \"x\" <> ? AND \"z\" = ?" +
            " AND \"sys_from\" = 1";
    private static final String EXPECTED_SQL_WITH_BETWEEN_TEMPLATE = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" BETWEEN ASYMMETRIC ? AND ? AND \"z\" = \"x\"";
    private static final String EXPECTED_SQL_WITH_JOIN = "SELECT *\n" +
            "FROM \"testdb623\".\"products\"\n" +
            "INNER JOIN \"testdb623\".\"categories\" ON \"testdb623\".\"products\".\"category_id\" = \"testdb623\".\"categories\".\"id\"\n" +
            "LEFT JOIN \"testdb623\".\"categories\" ON \"testdb623\".\"products\".\"category_id\" = \"testdb623\".\"categories\".\"id\"\n" +
            "RIGHT JOIN \"testdb623\".\"categories\" ON \"testdb623\".\"products\".\"category_id\" = \"testdb623\".\"categories\".\"id\"\n" +
            "CROSS JOIN \"testdb623\".\"categories\"\n" +
            "WHERE \"id\" = 1";
    private static final String EXPECTED_SQL_WITH_JOIN_TEMPLATE = "SELECT *\n" +
            "FROM \"testdb623\".\"products\"\n" +
            "INNER JOIN \"testdb623\".\"categories\" ON \"testdb623\".\"products\".\"category_id\" = \"testdb623\".\"categories\".\"id\"\n" +
            "LEFT JOIN \"testdb623\".\"categories\" ON \"testdb623\".\"products\".\"category_id\" = \"testdb623\".\"categories\".\"id\"\n" +
            "RIGHT JOIN \"testdb623\".\"categories\" ON \"testdb623\".\"products\".\"category_id\" = \"testdb623\".\"categories\".\"id\"\n" +
            "CROSS JOIN \"testdb623\".\"categories\"\n" +
            "WHERE \"id\" = ?";

    private static final String EXPECTED_SQL_WITH_WHERE_SUBQUERY = "SELECT *\n" +
            "FROM \"dtm\".\"table1\" AS \"a\"\n" +
            "INNER JOIN \"table3\" AS \"c\" ON \"c\".\"id\" = (SELECT \"a2\".\"id\"\n" +
            "FROM \"dtm\".\"table1\" AS \"a2\"\n" +
            "WHERE \"a2\".\"id\" = 10\n" +
            "LIMIT 1) AND \"c\".\"id\" < 20\n" +
            "WHERE \"a\".\"id\" IN (SELECT \"b\".\"id\"\n" +
            "FROM \"table2\" AS \"b\"\n" +
            "WHERE \"b\".\"id\" > 10)";

    private static final String EXPECTED_SQL_WITH_WHERE_SUBQUERY_TEMPLATE = "SELECT *\n" +
            "FROM \"dtm\".\"table1\" AS \"a\"\n" +
            "INNER JOIN \"table3\" AS \"c\" ON \"c\".\"id\" = (SELECT \"a2\".\"id\"\n" +
            "FROM \"dtm\".\"table1\" AS \"a2\"\n" +
            "WHERE \"a2\".\"id\" = ?\n" +
            "LIMIT 1) AND \"c\".\"id\" < ?\n" +
            "WHERE \"a\".\"id\" IN (SELECT \"b\".\"id\"\n" +
            "FROM \"table2\" AS \"b\"\n" +
            "WHERE \"b\".\"id\" > ?)";

    private static final String EXPECTED_SQL_WITH_COLLATE = "SELECT *\n" +
            "FROM \"dtm\".\"table1\" AS \"a\"\n" +
            "WHERE \"table1\".\"varchar_col\" = 'test' COLLATE 'unicode_ci'";

    private static final String EXPECTED_SQL_WITH_COLLATE_TEMPLATE = "SELECT *\n" +
            "FROM \"dtm\".\"table1\" AS \"a\"\n" +
            "WHERE \"table1\".\"varchar_col\" = ? COLLATE 'unicode_ci'";

    private static final String EXPECTED_SQL_WITH_SELECT = "SELECT *, 1, 'test'\n" +
            "FROM \"dtm\".\"table1\" AS \"a\"\n" +
            "WHERE \"table1\".\"varchar_col\" = 'test'";

    private static final String EXPECTED_SQL_WITH_SELECT_TEMPLATE = "SELECT *, 1, 'test'\n" +
            "FROM \"dtm\".\"table1\" AS \"a\"\n" +
            "WHERE \"table1\".\"varchar_col\" = ?";

    private static final String EXPECTED_SQL_WITH_EXCLUDED_COALESCE = "SELECT \"id\"\n" +
            "FROM \"datamart\".\"tbl_actual\"\n" +
            "WHERE \"sys_from\" <= 0 AND COALESCE(\"sys_to\", 9223372036854775807) >= 0 AND \"id\" = 1";

    private static final String EXPECTED_SQL_WITH_EXCLUDED_COALESCE_TEMPLATE = "SELECT \"id\"\n" +
            "FROM \"datamart\".\"tbl_actual\"\n" +
            "WHERE \"sys_from\" <= 0 AND COALESCE(\"sys_to\", 9223372036854775807) >= 0 AND \"id\" = ?";

    private static final String EXPECTED_SQL_WITH_OFFSET = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "LIMIT 2\n" +
            "OFFSET 1";

    private static final String EXPECTED_SQL_WITH_OFFSET_TEMPLATE = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "LIMIT ?\n" +
            "OFFSET ?";

    private static final String EXPECTED_STRING_LITERALS_ON_LEFT = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE 'x' LIKE 'x' AND " +
            "'x' = 'x' AND " +
            "'x' <> 'x' AND " +
            "'x' < 'x' AND " +
            "'x' <= 'x' AND " +
            "'x' > 'x' AND " +
            "'x' >= 'x' AND " +
            "'x' IN ('x', 'y') AND " +
            "'x' NOT IN ('x', 'y') AND " +
            "'x' BETWEEN ASYMMETRIC 'x' AND 'y'";

    private static final String EXPECTED_STRING_LITERALS_ON_LEFT_TEMPLATE = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE 'x' LIKE ? AND " +
            "'x' = ? AND " +
            "'x' <> ? AND " +
            "'x' < ? AND " +
            "'x' <= ? AND " +
            "'x' > ? AND " +
            "'x' >= ? AND " +
            "'x' IN (?, ?) AND " +
            "'x' NOT IN (?, ?) AND " +
            "'x' BETWEEN ASYMMETRIC ? AND ?";

    private static final String EXPECTED_NUMBER_LITERALS_ON_LEFT = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE 1.1 LIKE 1.1 AND " +
            "1.1 = 1.1 AND " +
            "1.1 <> 1.1 AND " +
            "1.1 < 1.1 AND " +
            "1.1 <= 1.1 AND " +
            "1.1 > 1.1 AND " +
            "1.1 >= 1.1 AND " +
            "1.1 IN (1.1, 2.2) AND " +
            "1.1 NOT IN (1.1, 2.2) AND " +
            "1.1 BETWEEN ASYMMETRIC 1.1 AND 2.2";

    private static final String EXPECTED_NUMBER_LITERALS_ON_LEFT_TEMPLATE = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE 1.1 LIKE ? AND " +
            "1.1 = ? AND " +
            "1.1 <> ? AND " +
            "1.1 < ? AND " +
            "1.1 <= ? AND " +
            "1.1 > ? AND " +
            "1.1 >= ? AND " +
            "1.1 IN (?, ?) AND " +
            "1.1 NOT IN (?, ?) AND " +
            "1.1 BETWEEN ASYMMETRIC ? AND ?";

    private static final String EXPECTED_ON_ORDER_BY = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"id\" IN (SELECT \"id\"\n" +
            "FROM \"tbl1\"\n" +
            "ORDER BY 1\n" +
            "LIMIT 1\n" +
            "OFFSET 1)\n" +
            "ORDER BY 1\n" +
            "LIMIT 1\n" +
            "OFFSET 1";

    private static final String EXPECTED_ON_ORDER_BY_TEMPLATE = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"id\" IN (SELECT \"id\"\n" +
            "FROM \"tbl1\"\n" +
            "ORDER BY 1\n" +
            "LIMIT 1\n" +
            "OFFSET 1)\n" +
            "ORDER BY 1\n" +
            "LIMIT ?\n" +
            "OFFSET ?";

    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private AbstractQueryTemplateExtractor extractor;

    @BeforeEach
    void setUp() {
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setParserFactory(calciteCoreConfiguration.eddlParserImplFactory())
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        CalciteDefinitionService definitionService = new CalciteDefinitionService(parserConfig) {
        };
        extractor = new CoreQueryTemplateExtractor(definitionService, SqlDialect.CALCITE);
    }

    @Test
    void extract() {
        assertExtract(EXPECTED_SQL, EXPECTED_TEMPLATE, 7);
    }

    @Test
    void extractSubSql() {
        assertExtract(EXPECTED_SUB_SQL, EXPECTED_SUB_TEMPLATE, 14);
    }

    @Test
    void extractWithSysColumn() {
        assertExtract(EXPECTED_SQL_WITH_SYS_COLUMNS,
                EXPECTED_TEMPLATE_WITH_SYS_COLUMNS,
                7,
                Collections.singletonList("sys_from"));
    }

    @Test
    void extractWithFull() {
        assertExtract(EXPECTED_FULL_SQL, EXPECTED_FULL_TEMPLATE, 2);
    }

    @Test
    void extractWithStringLiteralsOnLeft() {
        assertExtract(EXPECTED_STRING_LITERALS_ON_LEFT, EXPECTED_STRING_LITERALS_ON_LEFT_TEMPLATE, 13);
    }

    @Test
    void extractWithNumberLiteralsOnLeft() {
        assertExtract(EXPECTED_NUMBER_LITERALS_ON_LEFT, EXPECTED_NUMBER_LITERALS_ON_LEFT_TEMPLATE, 13);
    }

    @Test
    void extractWithOrderBy() {
        assertExtract(EXPECTED_ON_ORDER_BY, EXPECTED_ON_ORDER_BY_TEMPLATE, 2);
    }

    @Test
    void extractWithBetween() {
        assertExtract(EXPECTED_BETWEEN_SQL, EXPECTED_SQL_WITH_BETWEEN_TEMPLATE, 2);
    }

    @Test
    void extractWithIn() {
        assertExtract(EXPECTED_SQL_WITH_IN, EXPECTED_SQL_WITH_IN_TEMPLATE, 3);
    }

    @Test
    void extractWithInAndDynamicParams() {
        assertExtract(EXPECTED_SQL_WITH_IN_AND_DYNAMIC_PARAMS, EXPECTED_SQL_WITH_IN_TEMPLATE, 3);
    }

    @Test
    void extractWithNotIn() {
        assertExtract(EXPECTED_SQL_WITH_NOT_IN, EXPECTED_SQL_WITH_NOT_IN_TEMPLATE, 3);
    }

    @Test
    void extractWithNotInAndDynamicParams() {
        assertExtract(EXPECTED_SQL_WITH_NOT_IN_AND_DYNAMIC_PARAMS, EXPECTED_SQL_WITH_NOT_IN_TEMPLATE, 3);
    }

    @Test
    void extractWithJoin() {
        assertExtract(EXPECTED_SQL_WITH_JOIN, EXPECTED_SQL_WITH_JOIN_TEMPLATE, 1);
    }

    @Test
    void extractWithSubQuery() {
        assertExtract(EXPECTED_SQL_WITH_WHERE_SUBQUERY, EXPECTED_SQL_WITH_WHERE_SUBQUERY_TEMPLATE, 3);
    }

    @Test
    void extractWithCollate() {
        assertExtract(EXPECTED_SQL_WITH_COLLATE, EXPECTED_SQL_WITH_COLLATE_TEMPLATE, 1);
    }

    @Test
    void extractWithSelectLiterals() {
        assertExtract(EXPECTED_SQL_WITH_SELECT, EXPECTED_SQL_WITH_SELECT_TEMPLATE, 1);
    }

    @Test
    void extractWithSelectWithOffset() {
        assertExtract(EXPECTED_SQL_WITH_OFFSET, EXPECTED_SQL_WITH_OFFSET_TEMPLATE, 2);
    }

    @Test
    void extractWithCoalesce() {
        assertExtract(EXPECTED_SQL_WITH_EXCLUDED_COALESCE, EXPECTED_SQL_WITH_EXCLUDED_COALESCE_TEMPLATE, 1, Arrays.asList("sys_to", "sys_from"));
    }

    private void assertExtract(String sql, String template, int paramsSize) {
        assertExtract(sql, template, paramsSize, Collections.emptyList());
    }

    private void assertExtract(String sql, String template, int paramsSize, List<String> excludeColumns) {
        QueryTemplateResult templateResult = excludeColumns.isEmpty() ?
                extractor.extract(sql) : extractor.extract(sql, excludeColumns);
        assertEquals(paramsSize, templateResult.getParams().size());
        assertThat(templateResult.getTemplate()).isEqualToIgnoringNewLines(template);
        SqlNode enrichTemplate = extractor.enrichTemplate(templateResult.getTemplateNode(), templateResult.getParams());
        System.out.println(enrichTemplate.toString());
        assertThat(enrichTemplate.toSqlString(SqlDialect.CALCITE).toString()).isEqualToIgnoringNewLines(sql);
    }
}
