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
package ru.datamart.prostore.query.execution.plugin.adb.mppw;

import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaLoadRequest;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaRequestContext;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import io.vertx.core.json.Json;
import lombok.val;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdbMppwKafkaServiceTest {

    private MppwKafkaLoadRequest kafkaLoadRequest;
    private TransferDataRequest transferDataRequest;
    private MppwKafkaRequestContext kafkaRequestContext;

    @Test
    void convertKafkaRequestContextTest() {
        kafkaLoadRequest = MppwKafkaLoadRequest.builder()
        .requestId(UUID.randomUUID().toString())
                .datamart("test")
                .tableName("tab_")
                .schema(new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"accounts_ext_dtm_536\",\"namespace\":\"dtm_536\",\"fields\":[{\"name\":\"account_id\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"account_type\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}"))
        .build();

        transferDataRequest = TransferDataRequest.builder()
                .columnList(Arrays.asList("account_id", "account_type"))
                .keyColumnList(Arrays.asList("account_id"))
                .hotDelta(3L)
                .tableName("tab_")
                .datamart("test")
                .build();

        kafkaRequestContext = new MppwKafkaRequestContext(kafkaLoadRequest, transferDataRequest);

        val encodeRequest = Json.encode(kafkaRequestContext);
        val kafkaRequestContext = Json.decodeValue(encodeRequest, MppwKafkaRequestContext.class);
        val loadRequestDecoded = kafkaRequestContext.getMppwKafkaLoadRequest();
        val transferDataRequestDecoded = kafkaRequestContext.getTransferDataRequest();
        assertEquals(kafkaLoadRequest, loadRequestDecoded);
        assertEquals(transferDataRequest, transferDataRequestDecoded);
    }
}
