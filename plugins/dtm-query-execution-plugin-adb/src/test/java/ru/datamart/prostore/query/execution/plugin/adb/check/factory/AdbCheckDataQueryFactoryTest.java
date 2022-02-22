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
package ru.datamart.prostore.query.execution.plugin.adb.check.factory;

import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.adb.check.factory.impl.AdbCheckDataQueryFactory;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static utils.CreateEntityUtils.getEntity;

class AdbCheckDataQueryFactoryTest {

    private final AdbCheckDataQueryFactory adbCheckDataQueryFactory = new AdbCheckDataQueryFactory();

    @Test
    void testCreateCheckDataByCountQuery() {
        val expectedSql = "SELECT count(1) as cnt FROM (SELECT 1 FROM test_schema.test_table_actual WHERE (sys_from >= 1 AND sys_from <= 2 )\n" +
                " OR\n" +
                " (COALESCE(sys_to, 9223372036854775807) >= 0 AND COALESCE(sys_to, 9223372036854775807) <= 1 AND sys_op = 1)) AS tmp";
        CheckDataByCountRequest request = new CheckDataByCountRequest(
                UUID.randomUUID(),
                "",
                "",
                getEntity(),
                1L,
                2L);

        val checkQuery = adbCheckDataQueryFactory.createCheckDataByCountQuery(request, "cnt");
        assertEquals(expectedSql, checkQuery);
    }

    @Test
    void testCreateCheckDataByHashInt32QueryEmptyColumns() {
        val expectedSql = "SELECT sum(dtmInt32Hash(MD5(concat())::bytea)/1) as hash_sum FROM\n" +
                " (\n" +
                " SELECT \n" +
                " FROM test_schema.test_table_actual\n" +
                " WHERE (sys_from >= 1 AND sys_from <= 2 )\n" +
                " OR\n" +
                " (COALESCE(sys_to, 9223372036854775807) >= 0 AND COALESCE(sys_to, 9223372036854775807) <= 1 AND sys_op = 1)) AS tmp";
        CheckDataByHashInt32Request request = new CheckDataByHashInt32Request(
                UUID.randomUUID(),
                "",
                "",
                getEntity(),
                1L,
                2L,
                new HashSet<>(),
                1L);

        val checkQuery = adbCheckDataQueryFactory.createCheckDataByHashInt32Query(request, "hash_sum");
        assertEquals(expectedSql, checkQuery);
    }

    @Test
    void testCreateCheckDataByHashInt32QueryAllColumns() {
        val expectedSql = "SELECT sum(dtmInt32Hash(MD5(concat(UUID_type,';',INT32_type,';',LINK_type,';',VARCHAR_type,';',sk_key2,';',sk_key3,';',BIGINT_type,';',INT_type,';',DOUBLE_type,';',CHAR_type,';',(extract(epoch from TIME_type)*1000000)::bigint,';',pk2,';',(extract(epoch from TIMESTAMP_type)*1000000)::bigint,';',id,';',FLOAT_type,';',BOOLEAN_type::int,';',DATE_type - make_date(1970, 01, 01)))::bytea)/1) as hash_sum FROM\n" +
                " (\n" +
                " SELECT UUID_type,';',INT32_type,';',LINK_type,';',VARCHAR_type,';',sk_key2,';',sk_key3,';',BIGINT_type,';',INT_type,';',DOUBLE_type,';',CHAR_type,';',TIME_type,';',pk2,';',TIMESTAMP_type,';',id,';',FLOAT_type,';',BOOLEAN_type,';',DATE_type\n" +
                " FROM test_schema.test_table_actual\n" +
                " WHERE (sys_from >= 1 AND sys_from <= 2 )\n" +
                " OR\n" +
                " (COALESCE(sys_to, 9223372036854775807) >= 0 AND COALESCE(sys_to, 9223372036854775807) <= 1 AND sys_op = 1)) AS tmp";
        val entity = getEntity();
        CheckDataByHashInt32Request request = new CheckDataByHashInt32Request(
                UUID.randomUUID(),
                "",
                "",
                entity,
                1L,
                2L,
                entity.getFields().stream().map(EntityField::getName).collect(Collectors.toSet()),
                1L);
        val checkQuery = adbCheckDataQueryFactory.createCheckDataByHashInt32Query(request, "hash_sum");
        assertEquals(expectedSql, checkQuery);
    }

}
