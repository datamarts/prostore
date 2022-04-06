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
package ru.datamart.prostore.query.execution.plugin.adb.mppr.factory;

import lombok.val;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.adb.mppr.kafka.factory.KafkaMpprSqlFactory;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class KafkaMpprSqlFactoryTest {

    private static final String SCHEMA = "dtm";
    private static final String TABLE = "tbl";
    public static final String UUID_STRING = "4023dc32-8041-41fb-a885-f1450091eed8";

    private final KafkaMpprSqlFactory factory = new KafkaMpprSqlFactory();

    @Test
    void testGetTableName() {
        //act & assert
        assertEquals("PXF_EXT_4023dc32_8041_41fb_a885_f1450091eed8", factory.getTableName(UUID_STRING));
    }

    @Test
    void testDropWritableExtTableSqlQuery() {
        //act & assert
        assertEquals("DROP EXTERNAL TABLE IF EXISTS dtm.tbl", factory.dropWritableExtTableSqlQuery(SCHEMA, TABLE));
    }

    @Test
    void testInsertIntoWritableExtTableSqlQuery() {
        //act & assert
        assertEquals("INSERT INTO dtm.tbl enriched select", factory.insertIntoWritableExtTableSqlQuery(SCHEMA, TABLE, "enriched select"));
    }

    @Test
    void testCreateWritableExtTableSqlQuery() {
        //arrange
        val request = MpprKafkaRequest.builder()
                .datamartMnemonic(SCHEMA)
                .requestId(UUID.fromString(UUID_STRING))
                .destinationEntity(Entity.builder()
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("id")
                                        .type(ColumnType.INT)
                                        .ordinalPosition(0)
                                        .primaryOrder(0)
                                        .build(),
                                EntityField.builder()
                                        .name("varchar_col")
                                        .type(ColumnType.VARCHAR)
                                        .ordinalPosition(1)
                                        .build()))
                        .build())
                .topic("topic")
                .brokers(Collections.singletonList(new KafkaBrokerInfo("localhost", 80)))
                .downloadMetadata(DownloadExternalEntityMetadata.builder()
                        .chunkSize(1000)
                        .build())
                .build();

        //act & assert
        assertEquals("CREATE WRITABLE EXTERNAL TABLE dtm.PXF_EXT_4023dc32_8041_41fb_a885_f1450091eed8 (id int8, varchar_col varchar)\n" +
                "    LOCATION ('pxf://topic?PROFILE=kafka&BOOTSTRAP_SERVERS=localhost:80&BATCH_SIZE=1000')\n" +
                "    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')", factory.createWritableExtTableSqlQuery(request));
    }
}
