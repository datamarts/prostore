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
package ru.datamart.prostore.jdbc;

import ru.datamart.prostore.jdbc.core.Query;
import ru.datamart.prostore.jdbc.core.SqlParser;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class ParserCases {

    public static void main(String[] args) throws SQLException {
        final List<String> sqlQueries = Arrays.asList(
                "select * from t;",
                "select 1;",
                "update t.t set id = 1;",
                "select id from t.t",
                "select \"id\" from test;",
                "select * from (select id, acc from test.t t join test.tt tt on t.id = tt.id);",
                "select * from (select 1 as id union all select 2 union all select * from (select 3 as id union all select 4));",
                "select case name = ';' then true else false end from test.t",
                "select case id = ';' then true else false end from test.t;",
                "select id from t1;select id from t2; select id from t3;",
                "select id from t1;select id from t4; select id from t4",
                "insert into t select id from t_ext;",
                "select * from t where c1 = '\'';",
                "use t;",
                "begin delta;",
                "create table t (id int, name varchar(100));",
                "use test; begin delta; insert into test select id, acc from test_ext; commit delta;"
        );
        SqlParser parser = new SqlParser();
        for (String sqlQuery : sqlQueries) {
            final List<Query> queries = parser.parseSql(sqlQuery);
            queries.forEach(query -> System.out.println("Parsed native query: " + query.getNativeSql()));
        }

    }
}
