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
package ru.datamart.prostore.query.execution.core.utils;

import ru.datamart.prostore.query.execution.core.ddl.utils.SqlPreparer;
import org.junit.jupiter.api.Test;

import static ru.datamart.prostore.query.execution.core.ddl.utils.SqlPreparer.getTableWithSchema;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlPreparerTest {

    String createIfTable = "create table if not exists obj ()";
    String createTable = "create table obj ()";
    String createTableWithSchema = "create table dtmservice.obj ()";

    @Test
    void getTableWithSchemaMnemonic() {
        assertThat("dtmservice.table", equalTo(getTableWithSchema("dtmservice", "table")));
    }

    @Test
    void getTableWithSchemaMnemonicNull() {
        assertThat("dtmservice.table", equalTo(getTableWithSchema("", "table")));
    }

    @Test
    void getTableWithSchemaMnemonicAndSchema() {
        assertThat("dtmservice.table", equalTo(getTableWithSchema("dtmservice", "schema1.table")));
    }

    @Test
    void getTableWithSchemaMnemonicNullAndSchema() {
        assertThat("schema1.table", equalTo(getTableWithSchema("", "schema1.table")));
    }

    @Test
    void getTableWithSchemaMnemonicNullAndSchemaNull() {
        assertThat("dtmservice.table", equalTo(getTableWithSchema("", "table")));
    }

    @Test
    void replaceTableInSql() {
        String result = SqlPreparer.replaceTableInSql(createTable, "dtmservice.obj");
        assertThat(result, containsString(" dtmservice.obj "));
    }

    @Test
    void replaceTableInSqlWithSchema() {
        String result = SqlPreparer.replaceTableInSql(createTableWithSchema, "dtmservice.obj");
        assertThat(result, containsString(" dtmservice.obj "));
    }

    @Test
    void replaceTableInSqlUpperCase() {
        String result = SqlPreparer.replaceTableInSql(createTable.toUpperCase(), "dtmservice.obj");
        assertThat(result, containsString(" dtmservice.obj "));
    }

    @Test
    void replaceTableInSqlIf() {
        String result = SqlPreparer.replaceTableInSql(createIfTable, "dtmservice.obj");
        assertThat(result, containsString(" dtmservice.obj "));
    }

    @Test
    void replaceTableInSqlIfUpperCase() {
        String result = SqlPreparer.replaceTableInSql(createIfTable.toUpperCase(), "dtmservice.obj");
        assertThat(result, containsString(" dtmservice.obj "));
    }

    @Test
    void replaceTableWithSchemaUnderscore() {
        String result = SqlPreparer.replaceTableInSql("create table if not exists test_datamart.doc (", "dtmservice.obj");
        assertThat(result, containsString(" dtmservice.obj "));
    }

    @Test
    void replaceQuoting() {
        String input = "create table tbl(\"index\" varchar(50))";
        String expectedResult = "create table tbl(`index` varchar(50))";
        assertEquals(expectedResult, SqlPreparer.replaceQuote(input));
    }

}
