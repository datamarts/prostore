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
package ru.datamart.prostore.query.execution.plugin.adg.enrichment.service;

import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.service.AdgCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryGenerator;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.SchemaExtender;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service("adgQueryEnrichmentService")
@Slf4j
public class AdgQueryEnrichmentService implements QueryEnrichmentService {
    private final AdgCalciteContextProvider contextProvider;
    private final QueryGenerator adgQueryGenerator;
    private final SchemaExtender schemaExtender;

    @Autowired
    public AdgQueryEnrichmentService(AdgCalciteContextProvider contextProvider,
                                     @Qualifier("adgQueryGenerator") QueryGenerator adgQueryGenerator,
                                     SchemaExtender adgSchemaExtender) {
        this.contextProvider = contextProvider;
        this.adgQueryGenerator = adgQueryGenerator;
        this.schemaExtender = adgSchemaExtender;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request, QueryParserResponse parserResponse) {
        return Future.future(promise -> {
            contextProvider.enrichContext(parserResponse.getCalciteContext(),
                    generatePhysicalSchema(request.getSchema(), request.getEnvName()));
            // form a new sql query
            adgQueryGenerator.mutateQuery(parserResponse.getRelNode(),
                    request.getDeltaInformations(),
                    parserResponse.getCalciteContext(),
                    request)
                    .onSuccess(enrichResult -> {
                        log.debug("Request generated: {}", enrichResult);
                        promise.complete(enrichResult);
                    })
                    .onFailure(promise::fail);
        });
    }

    @Override
    public Future<SqlNode> getEnrichedSqlNode(EnrichQueryRequest request, QueryParserResponse parserResponse) {
        contextProvider.enrichContext(parserResponse.getCalciteContext(),
                generatePhysicalSchema(request.getSchema(), request.getEnvName()));
        return adgQueryGenerator.getMutatedSqlNode(parserResponse.getRelNode(),
                request.getDeltaInformations(),
                parserResponse.getCalciteContext(),
                request);
    }

    private List<Datamart> generatePhysicalSchema(List<Datamart> logicalSchemas, String envName) {
        return logicalSchemas.stream()
                .map(ls -> schemaExtender.createPhysicalSchema(ls, envName))
                .collect(Collectors.toList());
    }
}
