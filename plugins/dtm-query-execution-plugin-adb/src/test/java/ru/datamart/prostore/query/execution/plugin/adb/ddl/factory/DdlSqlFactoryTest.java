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
package ru.datamart.prostore.query.execution.plugin.adb.ddl.factory;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DdlSqlFactoryTest {
    private final DdlSqlFactory ddlSqlFactory = new DdlSqlFactory();

    private static final String SCHEMA = "test";
    private static final String TABLE = "tb_name";
    private static final String TABLE_NAME_WITH_SCHEMA = SCHEMA + '.' + TABLE;

    private static final String EXPECTED_DROP_TABLE_SCRIPT = "DROP TABLE IF EXISTS test.tb_name_actual;" +
            " DROP TABLE IF EXISTS test.tb_name_history;" +
            " DROP TABLE IF EXISTS test.tb_name_staging; ";
    private static final String EXPECTED_DROP_SCHEMA_SCRIPT = "DROP SCHEMA IF EXISTS test CASCADE";
    private static final String EXPECTED_CREATE_SCHEMA_SCRIPT = "CREATE SCHEMA IF NOT EXISTS test";
    private static final String EXPECTED_CREATE_INDEX_SCRIPT = "CREATE INDEX tb_name_actual_sys_from_idx ON test.tb_name_actual (sys_from);" +
            " CREATE INDEX tb_name_history_sys_to_idx ON test.tb_name_history (sys_to, sys_op)";

    @Test
    void shouldGenerateDropTableScript() {
        assertEquals(EXPECTED_DROP_TABLE_SCRIPT, ddlSqlFactory.createDropTableScript(TABLE_NAME_WITH_SCHEMA));
    }

    @Test
    void shouldGenerateDropSchemaSqlQuery() {
        assertEquals(EXPECTED_DROP_SCHEMA_SCRIPT, ddlSqlFactory.dropSchemaSqlQuery(SCHEMA));
    }

    @Test
    void shouldGenerateCreateSchemaSqlQuery() {
        assertEquals(EXPECTED_CREATE_SCHEMA_SCRIPT, ddlSqlFactory.createSchemaSqlQuery(SCHEMA));
    }

    @Test
    void shouldGenerateCreateSecondaryIndexSqlQuery() {
        assertEquals(EXPECTED_CREATE_INDEX_SCRIPT, ddlSqlFactory.createSecondaryIndexSqlQuery(SCHEMA, TABLE));
    }
}