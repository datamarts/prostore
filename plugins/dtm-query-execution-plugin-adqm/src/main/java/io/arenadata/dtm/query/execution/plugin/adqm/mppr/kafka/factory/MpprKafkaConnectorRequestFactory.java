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
package io.arenadata.dtm.query.execution.plugin.adqm.mppr.kafka.factory;

import io.arenadata.dtm.query.execution.plugin.adqm.mppr.kafka.dto.MpprKafkaConnectorRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
public class MpprKafkaConnectorRequestFactory {

    public MpprKafkaConnectorRequest create(MpprKafkaRequest request,
                                            String enrichedQuery) {
        val downloadMetadata =
                (DownloadExternalEntityMetadata) request.getDownloadMetadata();
        return MpprKafkaConnectorRequest.builder()
                .table(request.getSql())
                .sql(enrichedQuery)
                .datamart(request.getDatamartMnemonic())
                .kafkaBrokers(request.getBrokers())
                .kafkaTopic(request.getTopic())
                .chunkSize(downloadMetadata.getChunkSize())
                .avroSchema(downloadMetadata.getExternalSchema())
                .build();
    }
}
