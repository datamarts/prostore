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
package ru.datamart.prostore.query.execution.plugin.adqm.mppr.kafka.service;

import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.plugin.adqm.mppr.AdqmMpprExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.mppr.kafka.factory.MpprKafkaConnectorRequestFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.mppr.shuttle.SimpleQueryRelChecker;
import ru.datamart.prostore.query.execution.plugin.api.mppr.MpprRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.rel.RelRoot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adqmMpprKafkaService")
@Slf4j
public class AdqmMpprKafkaExecutor implements AdqmMpprExecutor {

    private final QueryEnrichmentService adqmQueryEnrichmentService;
    private final MpprKafkaConnectorService mpprKafkaConnectorService;
    private final MpprKafkaConnectorRequestFactory requestFactory;
    private final QueryParserService queryParserService;

    @Autowired
    public AdqmMpprKafkaExecutor(@Qualifier("adqmQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                                 MpprKafkaConnectorService mpprKafkaConnectorService,
                                 MpprKafkaConnectorRequestFactory requestFactory,
                                 @Qualifier("adqmCalciteDMLQueryParserService") QueryParserService queryParserService) {
        this.adqmQueryEnrichmentService = queryEnrichmentService;
        this.mpprKafkaConnectorService = mpprKafkaConnectorService;
        this.requestFactory = requestFactory;
        this.queryParserService = queryParserService;
    }

    @Override
    public Future<QueryResult> execute(MpprRequest request) {
        val kafkaRequest = (MpprKafkaRequest) request;
        return queryParserService.parse(new QueryParserRequest(kafkaRequest.getDmlSubQuery(), kafkaRequest.getLogicalSchema()))
                .compose(parserResponse ->
                {
                    boolean isSimple = isSimple(parserResponse.getRelNode());
                    return adqmQueryEnrichmentService.enrich(
                                    EnrichQueryRequest.builder()
                                            .query(kafkaRequest.getDmlSubQuery())
                                            .deltaInformations(kafkaRequest.getDeltaInformations())
                                            .envName(kafkaRequest.getEnvName())
                                            .schema(kafkaRequest.getLogicalSchema())
                                            .isLocal(isSimple)
                                            .build(),
                                    parserResponse)
                            .compose(enrichedQuery -> mpprKafkaConnectorService.call(
                                    requestFactory.create(kafkaRequest, enrichedQuery, isSimple)));
                });
    }

    private boolean isSimple(RelRoot relNode) {
        val shuttle = new SimpleQueryRelChecker();
        relNode.rel.accept(shuttle);
        return shuttle.isSimple();
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }
}
