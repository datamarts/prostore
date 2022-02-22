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
package ru.datamart.prostore.query.execution.plugin.adp.check.factory;

import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.UUID;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.createAllTypesTable;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AdpCheckDataQueryFactoryTest {

    @Test
    void testCreateCheckDataByCountQuery() {
        val expectedSql = "SELECT count(1) as cnt FROM (SELECT 1 FROM datamart.table_actual WHERE (sys_from >= 1 AND sys_from <= 2 )\n" +
                " OR\n" +
                " (COALESCE(sys_to, 9223372036854775807) >= 0 AND COALESCE(sys_to, 9223372036854775807) <= 1 AND sys_op = 1)) AS tmp";
        CheckDataByCountRequest request = new CheckDataByCountRequest(
                UUID.randomUUID(),
                "",
                "",
                createAllTypesTable(),
                1L,
                2L);

        val checkQuery = AdpCheckDataQueryFactory.createCheckDataByCountQuery(request);
        assertEquals(expectedSql, checkQuery);
    }

    @Test
    void testCreateCheckDataByHashInt32QueryEmptyColumns() {
        val expectedSql = "SELECT sum(dtmInt32Hash(MD5(concat())::bytea)/1) as hash_sum FROM\n" +
                " (\n" +
                " SELECT \n" +
                " FROM datamart.table_actual\n" +
                " WHERE (sys_from >= 1 AND sys_from <= 2 )\n" +
                " OR\n" +
                " (COALESCE(sys_to, 9223372036854775807) >= 0 AND COALESCE(sys_to, 9223372036854775807) <= 1 AND sys_op = 1)) AS tmp";
        CheckDataByHashInt32Request request = new CheckDataByHashInt32Request(
                UUID.randomUUID(),
                "",
                "",
                createAllTypesTable(),
                1L,
                2L,
                new HashSet<>(),
                1L);

        val checkQuery = AdpCheckDataQueryFactory.createCheckDataByHashInt32Query(request);
        assertEquals(expectedSql, checkQuery);
    }

    @Test
    void testCreateCheckDataByHashInt32QueryAllColumns() {
        val expectedSql = "SELECT sum(dtmInt32Hash(MD5(concat(uuid_col,';',char_col,';',int_col,';',link_col,';',bigint_col,';',boolean_col::int,';',date_col - make_date(1970, 01, 01),';',float_col,';',varchar_col,';',int32_col,';',(extract(epoch from timestamp_col)*1000000)::bigint,';',id,';',(extract(epoch from time_col)*1000000)::bigint,';',double_col))::bytea)/1) as hash_sum FROM\n" +
                " (\n" +
                " SELECT uuid_col,';',char_col,';',int_col,';',link_col,';',bigint_col,';',boolean_col,';',date_col,';',float_col,';',varchar_col,';',int32_col,';',timestamp_col,';',id,';',time_col,';',double_col\n" +
                " FROM datamart.table_actual\n" +
                " WHERE (sys_from >= 1 AND sys_from <= 2 )\n" +
                " OR\n" +
                " (COALESCE(sys_to, 9223372036854775807) >= 0 AND COALESCE(sys_to, 9223372036854775807) <= 1 AND sys_op = 1)) AS tmp";
        val entity = createAllTypesTable();
        CheckDataByHashInt32Request request = new CheckDataByHashInt32Request(
                UUID.randomUUID(),
                "",
                "",
                entity,
                1L,
                2L,
                entity.getFields().stream().map(EntityField::getName).collect(Collectors.toSet()),
                1L);
        val checkQuery = AdpCheckDataQueryFactory.createCheckDataByHashInt32Query(request);
        assertEquals(expectedSql, checkQuery);
    }

}
