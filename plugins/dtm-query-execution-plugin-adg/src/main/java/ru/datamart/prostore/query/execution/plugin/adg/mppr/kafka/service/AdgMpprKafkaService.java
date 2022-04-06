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
package ru.datamart.prostore.query.execution.plugin.adg.mppr.kafka.service;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgUploadDataKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.mppr.AdgMpprExecutor;
import ru.datamart.prostore.query.execution.plugin.api.exception.MpprDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.mppr.MpprRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

@Slf4j
@Service("adgMpprKafkaService")
public class AdgMpprKafkaService implements AdgMpprExecutor {
    private final QueryParserService queryParserService;
    private final QueryEnrichmentService adgQueryEnrichmentService;
    private final AdgCartridgeClient adgCartridgeClient;

    public AdgMpprKafkaService(@Qualifier("adgCalciteDMLQueryParserService") QueryParserService queryParserService,
                               @Qualifier("adgQueryEnrichmentService") QueryEnrichmentService adgQueryEnrichmentService,
                               AdgCartridgeClient adgCartridgeClient) {
        this.queryParserService = queryParserService;
        this.adgQueryEnrichmentService = adgQueryEnrichmentService;
        this.adgCartridgeClient = adgCartridgeClient;
    }

    @Override
    public Future<QueryResult> execute(MpprRequest request) {
        return Future.future(promise -> {
            queryParserService.parse(new QueryParserRequest(((MpprKafkaRequest) request).getDmlSubQuery(), request.getLogicalSchema(), request.getEnvName()))
                    .compose(parserResponse -> {
                        val enrichRequest = EnrichQueryRequest.builder()
                                .envName(request.getEnvName())
                                .deltaInformations(request.getDeltaInformations())
                                .calciteContext(parserResponse.getCalciteContext())
                                .relNode(parserResponse.getRelNode())
                                .build();
                        return adgQueryEnrichmentService.enrich(enrichRequest);
                    })
                    .compose(enrichQuery -> uploadData((MpprKafkaRequest) request, enrichQuery))
                    .onComplete(promise);
        });
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }

    private Future<QueryResult> uploadData(MpprKafkaRequest kafkaRequest, String sql) {
        return Future.future(promise -> {
            val downloadMetadata =
                    (DownloadExternalEntityMetadata) kafkaRequest.getDownloadMetadata();
            val request = new AdgUploadDataKafkaRequest(
                    sql,
                    kafkaRequest.getTopic(),
                    downloadMetadata.getChunkSize(),
                    new JsonObject(downloadMetadata.getExternalSchema()));

            adgCartridgeClient.uploadData(request)
                    .onSuccess(queryResult -> {
                                log.info("Uploading data from ADG was successful on request: {}",
                                        kafkaRequest.getRequestId());
                                promise.complete(QueryResult.emptyResult());
                            }
                    )
                    .onFailure(fail -> promise.fail(new MpprDatasourceException(
                            String.format("Error unloading data by request %s", request),
                            fail)));
        });
    }
}
