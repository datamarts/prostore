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
package ru.datamart.prostore.query.execution.plugin.adp.mppr.kafka.service;

import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.plugin.adp.connector.dto.AdpConnectorMpprRequest;
import ru.datamart.prostore.query.execution.plugin.adp.connector.service.AdpConnectorClient;
import ru.datamart.prostore.query.execution.plugin.adp.mppr.AdpMpprExecutor;
import ru.datamart.prostore.query.execution.plugin.api.mppr.MpprRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Slf4j
@Service("adpMpprKafkaService")
public class AdpMpprKafkaExecutor implements AdpMpprExecutor {
    private final QueryParserService queryParserService;
    private final QueryEnrichmentService queryEnrichmentService;
    private final AdpConnectorClient adpConnectorClient;

    public AdpMpprKafkaExecutor(@Qualifier("adpCalciteDMLQueryParserService") QueryParserService queryParserService,
                                @Qualifier("adpQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                                AdpConnectorClient adpConnectorClient) {
        this.queryParserService = queryParserService;
        this.queryEnrichmentService = queryEnrichmentService;
        this.adpConnectorClient = adpConnectorClient;
    }

    @Override
    public Future<QueryResult> execute(MpprRequest request) {
        return Future.future(promise -> {
            log.info("[ADP] Trying to start MPPR, request: [{}]", request);
            val kafkaRequest = (MpprKafkaRequest) request;
            queryParserService.parse(new QueryParserRequest(kafkaRequest.getDmlSubQuery(), kafkaRequest.getLogicalSchema()))
                    .compose(parserResponse -> queryEnrichmentService.enrich(getEnrichmentRequest(kafkaRequest), parserResponse))
                    .compose(enrichedQuery -> adpConnectorClient.runMppr(getConnectorMpprRequest(kafkaRequest, enrichedQuery)))
                    .onSuccess(v -> {
                        log.info("[ADP] Mppr completed successfully");
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(t -> {
                        log.error("[ADP] Mppr failed", t);
                        promise.fail(t);
                    });
        });
    }

    private EnrichQueryRequest getEnrichmentRequest(MpprKafkaRequest request) {
        return EnrichQueryRequest.builder()
                .query(request.getDmlSubQuery())
                .deltaInformations(request.getDeltaInformations())
                .envName(request.getEnvName())
                .schema(request.getLogicalSchema())
                .build();
    }

    private AdpConnectorMpprRequest getConnectorMpprRequest(MpprKafkaRequest request, String enrichedQuery) {
        val downloadMetadata =
                (DownloadExternalEntityMetadata) request.getDownloadMetadata();
        return AdpConnectorMpprRequest.builder()
                .requestId(request.getRequestId().toString())
                .table(request.getSql())
                .datamart(request.getDatamartMnemonic())
                .sql(enrichedQuery)
                .kafkaBrokers(request.getBrokers())
                .kafkaTopic(request.getTopic())
                .chunkSize(downloadMetadata.getChunkSize())
                .avroSchema(new Schema.Parser().parse(downloadMetadata.getExternalSchema()))
                .build();
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }
}
