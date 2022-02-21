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
package ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.impl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adb.base.dto.metadata.AdbTables;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.metadata.AdbTableEntitiesFactory;
import ru.datamart.prostore.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import utils.CreateEntityUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AdbCreateTableQueriesFactoryTest {
    private static final String EXPECTED_CREATE_ACTUAL_TABLE_QUERY = "CREATE TABLE test_schema.test_table_actual " +
            "(id int8 NOT NULL, sk_key2 int8 NOT NULL, pk2 int8 NOT NULL, sk_key3 int8 NOT NULL, " +
            "VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, INT32_type int4, DOUBLE_type float8, " +
            "FLOAT_type float4, DATE_type date, TIME_type time(6), TIMESTAMP_type timestamp(6), BOOLEAN_type bool, " +
            "UUID_type varchar(36), LINK_type varchar, sys_from int8, sys_to int8, sys_op int4, " +
            "constraint pk_test_schema_test_table_actual primary key (id, pk2, sys_from)) " +
            "DISTRIBUTED BY (id, sk_key2, sk_key3)";

    private static final String EXPECTED_CREATE_HISTORY_TABLE_QUERY = "CREATE TABLE test_schema.test_table_history " +
            "(id int8 NOT NULL, sk_key2 int8 NOT NULL, pk2 int8 NOT NULL, sk_key3 int8 NOT NULL, " +
            "VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, INT32_type int4, DOUBLE_type float8, " +
            "FLOAT_type float4, DATE_type date, TIME_type time(6), TIMESTAMP_type timestamp(6), BOOLEAN_type bool, " +
            "UUID_type varchar(36), LINK_type varchar, sys_from int8, sys_to int8, sys_op int4, " +
            "constraint pk_test_schema_test_table_history primary key (id, pk2, sys_from)) " +
            "DISTRIBUTED BY (id, sk_key2, sk_key3)";

    private static final String EXPECTED_CREATE_STAGING_TABLE_QUERY = "CREATE TABLE test_schema.test_table_staging " +
            "(id int8, sk_key2 int8, pk2 int8, sk_key3 int8, " +
            "VARCHAR_type varchar(20), CHAR_type varchar(20), BIGINT_type int8, INT_type int8, INT32_type int4, DOUBLE_type float8, " +
            "FLOAT_type float4, DATE_type date, TIME_type time(6), TIMESTAMP_type timestamp(6), BOOLEAN_type bool, " +
            "UUID_type varchar(36), LINK_type varchar, sys_from int8, sys_to int8, sys_op int4) " +
            "DISTRIBUTED BY (id, sk_key2, sk_key3)";

    private static AdbTables<String> adbCreateTableQueries;

    @BeforeAll
    static void setUp() {
        Entity entity = CreateEntityUtils.getEntity();
        CreateTableQueriesFactory<AdbTables<String>> adbCreateTableQueriesFactory = new AdbCreateTableQueriesFactory(new AdbTableEntitiesFactory());
        adbCreateTableQueries = adbCreateTableQueriesFactory.create(entity, "");
    }

    @Test
    void createActualTableQueryTest() {
        assertEquals(EXPECTED_CREATE_ACTUAL_TABLE_QUERY, adbCreateTableQueries.getActual());
    }

    @Test
    void createHistoryTableQueryTest() {
        assertEquals(EXPECTED_CREATE_HISTORY_TABLE_QUERY, adbCreateTableQueries.getHistory());
    }

    @Test
    void createStagingTableQueryTest() {
        assertEquals(EXPECTED_CREATE_STAGING_TABLE_QUERY, adbCreateTableQueries.getStaging());
    }
}
