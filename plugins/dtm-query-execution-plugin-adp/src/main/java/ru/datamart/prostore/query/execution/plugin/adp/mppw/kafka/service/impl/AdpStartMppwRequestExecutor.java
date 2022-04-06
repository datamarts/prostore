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
package ru.datamart.prostore.query.execution.plugin.adp.mppw.kafka.service.impl;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.plugin.adp.base.Constants;
import ru.datamart.prostore.query.execution.plugin.adp.base.properties.AdpMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adp.connector.dto.AdpConnectorMppwStartRequest;
import ru.datamart.prostore.query.execution.plugin.adp.connector.service.AdpConnectorClient;
import ru.datamart.prostore.query.execution.plugin.adp.mppw.kafka.service.AdpMppwRequestExecutor;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

@Slf4j
@Service("adpStartMppwRequestExecutor")
public class AdpStartMppwRequestExecutor implements AdpMppwRequestExecutor {
    private static final String STAGING_POSTFIX = "_" + Constants.STAGING_TABLE;
    private final AdpConnectorClient connectorClient;
    private final AdpMppwProperties adpMppwProperties;

    public AdpStartMppwRequestExecutor(AdpConnectorClient connectorClient,
                                       AdpMppwProperties adpMppwProperties) {
        this.connectorClient = connectorClient;
        this.adpMppwProperties = adpMppwProperties;
    }

    @Override
    public Future<String> execute(MppwKafkaRequest request) {
        return Future.future(promise -> {
            log.info("[ADP] Trying to start MPPW, request: [{}]", request);
            val destinationEntity = request.getDestinationEntity();

            String table;
            String schema;
            if (destinationEntity.getEntityType() == EntityType.WRITEABLE_EXTERNAL_TABLE) {
                val schemaAndTableName = destinationEntity.getExternalTableLocationPath().split("\\.");
                schema = schemaAndTableName[0];
                table = schemaAndTableName[1];
            } else {
                schema = destinationEntity.getSchema();
                table = destinationEntity.getName() + STAGING_POSTFIX;
            }

            val connectorRequest = AdpConnectorMppwStartRequest.builder()
                    .requestId(request.getRequestId().toString())
                    .datamart(schema)
                    .tableName(table)
                    .kafkaBrokers(request.getBrokers())
                    .kafkaTopic(request.getTopic())
                    .consumerGroup(adpMppwProperties.getKafkaConsumerGroup())
                    .format(request.getUploadMetadata().getFormat().getName())
                    .schema(new Schema.Parser().parse(request.getUploadMetadata().getExternalSchema()))
                    .build();

            connectorClient.startMppw(connectorRequest)
                    .onSuccess(v -> {
                        log.info("[ADP] Mppw started successfully");
                        promise.complete(adpMppwProperties.getKafkaConsumerGroup());
                    })
                    .onFailure(t -> {
                        log.error("[ADP] Mppw failed to start", t);
                        promise.fail(t);
                    });
        });
    }
}
