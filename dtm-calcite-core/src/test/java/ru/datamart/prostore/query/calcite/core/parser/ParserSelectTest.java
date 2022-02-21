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
package ru.datamart.prostore.query.calcite.core.parser;

import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.extension.dml.LimitableSqlOrderBy;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlSelectExt;
import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class ParserSelectTest {
    
    @Test
    void shouldParseWhenLimitAndOffset() throws SqlParseException {
        // arrange
        val sql = "select * from dtm.table1 a LIMIT 1 OFFSET 2";
        SqlParser parser = getParser(sql);

        // act
        SqlNode sqlNode = parser.parseQuery();

        // assert
        assertSame(LimitableSqlOrderBy.class, sqlNode.getClass());
        LimitableSqlOrderBy limitableSqlOrderBy = (LimitableSqlOrderBy) sqlNode;
        assertNotNull(limitableSqlOrderBy.fetch);
        assertEquals(BigDecimal.valueOf(1), ((SqlNumericLiteral)limitableSqlOrderBy.fetch).getValue());

        assertNotNull(limitableSqlOrderBy.offset);
        assertEquals(BigDecimal.valueOf(2), ((SqlNumericLiteral)limitableSqlOrderBy.offset).getValue());
    }

    @Test
    void shouldParseWhenFetchAndOffset() throws SqlParseException {
        // arrange
        val sql = "select * from dtm.table1 FETCH NEXT 1 ROWS ONLY OFFSET 2";
        SqlParser parser = getParser(sql);

        // act
        SqlNode sqlNode = parser.parseQuery();

        // assert
        assertSame(LimitableSqlOrderBy.class, sqlNode.getClass());
        LimitableSqlOrderBy limitableSqlOrderBy = (LimitableSqlOrderBy) sqlNode;
        assertNotNull(limitableSqlOrderBy.fetch);
        assertEquals(BigDecimal.valueOf(1), ((SqlNumericLiteral)limitableSqlOrderBy.fetch).getValue());

        assertNotNull(limitableSqlOrderBy.offset);
        assertEquals(BigDecimal.valueOf(2), ((SqlNumericLiteral)limitableSqlOrderBy.offset).getValue());
    }

    @Test
    void shouldParseWhenLimitAndOffsetDynamic() throws SqlParseException {
        // arrange
        val sql = "select * from dtm.table1 a LIMIT ? OFFSET ?";
        SqlParser parser = getParser(sql);

        // act
        SqlNode sqlNode = parser.parseQuery();

        // assert
        assertSame(LimitableSqlOrderBy.class, sqlNode.getClass());
        LimitableSqlOrderBy limitableSqlOrderBy = (LimitableSqlOrderBy) sqlNode;
        assertNotNull(limitableSqlOrderBy.fetch);
        assertSame(SqlKind.DYNAMIC_PARAM, limitableSqlOrderBy.fetch.getKind());

        assertNotNull(limitableSqlOrderBy.offset);
        assertSame(SqlKind.DYNAMIC_PARAM, limitableSqlOrderBy.fetch.getKind());
    }

    @Test
    void shouldParseWhenFetchAndOffsetDynamic() throws SqlParseException {
        // arrange
        val sql = "select * from dtm.table1 FETCH NEXT ? ROWS ONLY OFFSET ?";
        SqlParser parser = getParser(sql);

        // act
        SqlNode sqlNode = parser.parseQuery();

        // assert
        assertSame(LimitableSqlOrderBy.class, sqlNode.getClass());
        LimitableSqlOrderBy limitableSqlOrderBy = (LimitableSqlOrderBy) sqlNode;
        assertNotNull(limitableSqlOrderBy.fetch);
        assertSame(SqlKind.DYNAMIC_PARAM, limitableSqlOrderBy.fetch.getKind());

        assertNotNull(limitableSqlOrderBy.offset);
        assertSame(SqlKind.DYNAMIC_PARAM, limitableSqlOrderBy.fetch.getKind());
    }

    @Test
    void shouldFailWhenOffsetAndFetchLast() {
        parseShouldFail("select * from dtm.table1 OFFSET 2 FETCH NEXT 1 ROWS ONLY");
    }

    @Test
    void shouldFailWhenOffsetAndLimitLast() {
        // arrange
        parseShouldFail("select * from dtm.table1 OFFSET 2 LIMIT 1");
    }

    @Test
    void shouldFailWhenOnlyOffset() {
        // arrange
        parseShouldFail("select * from dtm.table1 OFFSET 2");
    }

    @Test
    void shouldFailWhenOnlyDynamicOffset() {
        // arrange
        parseShouldFail("select * from dtm.table1 OFFSET ?");
    }

    @Test
    void shouldFailWhenDoubleLimit() {
        // arrange
        parseShouldFail("select * from dtm.table1 LIMIT 1,2");
    }

    @Test
    void shouldFailWhenDynamicDoubleLimit() {
        // arrange
        parseShouldFail("select * from dtm.table1 LIMIT ?,?");
    }

    @Test
    void shouldFailWhenDataSourceTypeBeforeOrderBy() {
        // arrange
        parseShouldFail("SELECT * FROM dtm.table1 DATASOURCE_TYPE = 'ADB' ORDER BY id");
    }

    @Test
    void shouldFailWhenDataSourceTypeAfterEstimateOnly() {
        // arrange
        parseShouldFail("SELECT * FROM dtm.table1 ESTIMATE_ONLY DATASOURCE_TYPE = 'ADB'");
    }

    @Test
    void shouldFailWhenFirstEstimateOnly() {
        // arrange
        parseShouldFail("SELECT * FROM dtm.table1 ESTIMATE_ONLY ORDER_BY id DATASOURCE_TYPE = 'ADB'");
    }

    @Test
    void shouldParseWhenWhereEqualCollate() throws SqlParseException {
        // arrange
        val sql = "select * from dtm.table1 a WHERE table1.varchar_col = 'test' COLLATE 'unicode_ci'";
        SqlParser parser = getParser(sql);

        // act
        SqlNode sqlNode = parser.parseQuery();

        // assert
        assertSame(SqlSelectExt.class, sqlNode.getClass());
        val sqlSelect = (SqlSelectExt) sqlNode;
        val condition = (SqlBasicCall) sqlSelect.getWhere();
        assertEquals(SqlKind.EQUALS, condition.getOperator().getKind());
        val collation = (SqlBasicCall) condition.getOperands()[1];
        assertEquals("COLLATE", collation.getOperator().getName());
        assertEquals(SqlKind.OTHER, collation.getOperator().getKind());
    }

    @Test
    void shouldParseWhenWhereCollateEqual() throws SqlParseException {
        // arrange
        val sql = "select * from dtm.table1 a WHERE table1.varchar_col COLLATE 'unicode_ci' = 'test'";
        SqlParser parser = getParser(sql);

        // act
        SqlNode sqlNode = parser.parseQuery();

        // assert
        assertSame(SqlSelectExt.class, sqlNode.getClass());
        val sqlSelect = (SqlSelectExt) sqlNode;
        val condition = (SqlBasicCall) sqlSelect.getWhere();
        assertEquals(SqlKind.EQUALS, condition.getOperator().getKind());
        val collation = (SqlBasicCall) condition.getOperands()[0];
        assertEquals("COLLATE", collation.getOperator().getName());
        assertEquals(SqlKind.OTHER, collation.getOperator().getKind());
    }

    private SqlParser getParser(String sql) {
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(new CalciteCoreConfiguration().eddlParserImplFactory())
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        return SqlParser.create(sql, config);
    }

    private void parseShouldFail(String sql) {
        // arrange
        SqlParser parser = getParser(sql);

        // act assert
        Assertions.assertThrows(SqlParseException.class, parser::parseQuery);
    }
}
