package ru.datamart.prostore.query.calcite.core.node;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
class SqlSelectTreeTest {

    @Test
    void test() throws SqlParseException {
        val sql = "SELECT v.col1 AS c\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' v";
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseQuery();
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
        assertNotNull(selectTree);
        assertEquals(10, selectTree.getNodeMap().size());
        assertEquals(1, selectTree.findAllTableAndSnapshots().size());
    }

    @Test
    void test2() throws SqlParseException {
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz as z WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseQuery();
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
        assertNotNull(selectTree);
        assertEquals(61, selectTree.getNodeMap().size());
        assertEquals(4, selectTree.findAllTableAndSnapshots().size());
    }

    @Test
    void test3() throws SqlParseException {
        val sql = "select *, CASE WHEN (account_type = 'D' AND  amount >= 0) OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                "  from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                "    from shares.accounts a\n" +
                "    left join shares.transactions FOR SYSTEM_TIME AS OF '2020-06-30 16:18:58' t using(account_id)\n" +
                "    left join shares.transactions2 t2 using(account_id)\n" +
                "    left join shares.transactions3 using(account_id)\n" +
                "   group by a.account_id, account_type\n" +
                ")x";
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseQuery();
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
        assertNotNull(selectTree);
        assertEquals(65, selectTree.getNodeMap().size());
        assertEquals(4, selectTree.findAllTableAndSnapshots().size());
    }

    @Test
    void test4() throws SqlParseException {
        val sql = "select * from dtm.table1 a " +
                "join table3 c on c.id = (select a2.id from dtm.table1 a2 where a2.id = 10 limit 1) " +
                "where a.id in (select b.id from table2 b where b.id > 10)";
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseQuery();
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
        assertNotNull(selectTree);
        assertEquals(38, selectTree.getNodeMap().size());
        assertEquals(4, selectTree.findAllTableAndSnapshots().size());
    }
}
