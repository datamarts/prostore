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
package ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service;

import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.service.AdqmCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.SchemaExtender;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryGenerator;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service("adqmQueryEnrichmentService")
public class AdqmQueryEnrichmentService implements QueryEnrichmentService {
    private final AdqmCalciteContextProvider contextProvider;
    private final QueryGenerator adqmQueryGenerator;
    private final SchemaExtender schemaExtender;

    @Autowired
    public AdqmQueryEnrichmentService(@Qualifier("adqmCalciteDMLQueryParserService") QueryParserService queryParserService,
                                      AdqmCalciteContextProvider contextProvider,
                                      @Qualifier("adqmQueryGenerator") QueryGenerator adqmQueryGenerator,
                                      SchemaExtender adqmSchemaExtender) {
        this.contextProvider = contextProvider;
        this.adqmQueryGenerator = adqmQueryGenerator;
        this.schemaExtender = adqmSchemaExtender;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request, QueryParserResponse parserResponse) {
        return modifyQuery(parserResponse, request);
    }

    @Override
    public Future<SqlNode> getEnrichedSqlNode(EnrichQueryRequest request, QueryParserResponse parserResponse) {
        return Future.future(promise -> {
            contextProvider.enrichContext(parserResponse.getCalciteContext(),
                    generatePhysicalSchema(request.getSchema(), request.getEnvName()));
            // form a new sql query
            adqmQueryGenerator.getMutatedSqlNode(parserResponse.getRelNode(),
                    request.getDeltaInformations(),
                    parserResponse.getCalciteContext(),
                    request)
                    .onComplete(promise);
        });
    }

    private Future<String> modifyQuery(QueryParserResponse parserResponse, EnrichQueryRequest request) {
        return Future.future(promise -> {
            contextProvider.enrichContext(parserResponse.getCalciteContext(),
                    generatePhysicalSchema(request.getSchema(), request.getEnvName()));
            // form a new sql query
            adqmQueryGenerator.mutateQuery(parserResponse.getRelNode(),
                    request.getDeltaInformations(),
                    parserResponse.getCalciteContext(),
                    request)
                    .onComplete(enrichedQueryResult -> {
                        if (enrichedQueryResult.succeeded()) {
                            log.debug("Request generated: {}", enrichedQueryResult.result());
                            promise.complete(enrichedQueryResult.result());
                        } else {
                            promise.fail(enrichedQueryResult.cause());
                        }
                    });
        });
    }

    private List<Datamart> generatePhysicalSchema(List<Datamart> logicalSchemas, String envName) {
        return logicalSchemas.stream()
                .map(ls -> schemaExtender.createPhysicalSchema(ls, envName))
                .collect(Collectors.toList());
    }
}
