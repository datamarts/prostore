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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.impl;

import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.AdbKafkaMppwTransferRequest;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MppwWithoutHistoryTableRequestFactoryTest {

    private static final String EXPECTED_UPDATE = "UPDATE datamart.tbl1_actual actual\n" +
            "SET\n" +
            "  sys_to = 54,\n" +
            "  sys_op = staging.sys_op\n" +
            "FROM (\n" +
            "  SELECT id1, id2, MAX(sys_op) as sys_op\n" +
            "  FROM datamart.tbl1_staging\n" +
            "  GROUP BY id1, id2\n" +
            ") staging\n" +
            "WHERE staging.id1=actual.id1 AND staging.id2=actual.id2 AND\n" +
            "      actual.sys_from < 55 AND actual.sys_to IS NULL";

    private static final String EXPECTED_INSERT = "INSERT INTO datamart.tbl1_actual (id1, id2, c, sys_from, sys_op)\n" +
            "  SELECT DISTINCT ON (staging.id1, staging.id2) staging.id1, staging.id2, staging.c, 55 AS sys_from, 0 AS sys_op FROM datamart.tbl1_staging staging\n" +
            "    LEFT JOIN datamart.tbl1_actual actual ON staging.id1=actual.id1 AND staging.id2=actual.id2 AND actual.sys_from = 55\n" +
            "  WHERE actual.sys_from IS NULL AND staging.sys_op <> 1";

    @Test
    void create() {
        MppwWithoutHistoryTableRequestFactory factory = new MppwWithoutHistoryTableRequestFactory();
        AdbKafkaMppwTransferRequest transferRequest = factory.create(TransferDataRequest.builder()
                .columnList(Arrays.asList("id1", "id2", "c", "sys_from", "sys_to", "sys_op"))
                .datamart("datamart")
                .tableName("tbl1")
                .hotDelta(55)
                .keyColumnList(Arrays.asList("id1", "id2", "sys_from"))
                .build());
        System.out.println(transferRequest);
        assertNotNull(transferRequest);
        assertEquals(EXPECTED_UPDATE, transferRequest.getFirstTransaction().get(0).getSql());
        assertEquals(EXPECTED_INSERT, transferRequest.getSecondTransaction().get(0).getSql());
    }
}
