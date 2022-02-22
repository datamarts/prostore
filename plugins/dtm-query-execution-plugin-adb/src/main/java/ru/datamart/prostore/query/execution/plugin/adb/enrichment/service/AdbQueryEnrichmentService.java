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
package ru.datamart.prostore.query.execution.plugin.adb.enrichment.service;

import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
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

@Service("adbQueryEnrichmentService")
@Slf4j
public class AdbQueryEnrichmentService implements QueryEnrichmentService {
    private final AdbCalciteContextProvider contextProvider;
    private final SchemaExtender schemaExtender;
    private final QueryGenerator adbQueryGenerator;

    @Autowired
    public AdbQueryEnrichmentService(
            @Qualifier("adbQueryGenerator") QueryGenerator adbQueryGenerator,
            AdbCalciteContextProvider contextProvider,
            @Qualifier("adbSchemaExtender") SchemaExtender schemaExtender) {
        this.adbQueryGenerator = adbQueryGenerator;
        this.contextProvider = contextProvider;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request, QueryParserResponse parserResponse) {
        contextProvider.enrichContext(parserResponse.getCalciteContext(),
                generatePhysicalSchemas(request.getSchema()));
        return mutateQuery(parserResponse, request);
    }

    @Override
    public Future<SqlNode> getEnrichedSqlNode(EnrichQueryRequest request, QueryParserResponse response) {
        contextProvider.enrichContext(response.getCalciteContext(),
                generatePhysicalSchemas(request.getSchema()));
        return adbQueryGenerator.getMutatedSqlNode(response.getRelNode(),
                request.getDeltaInformations(),
                response.getCalciteContext(),
                null);
    }

    private Future<String> mutateQuery(QueryParserResponse response, EnrichQueryRequest request) {
        return Future.future(promise -> adbQueryGenerator.mutateQuery(response.getRelNode(),
                request.getDeltaInformations(),
                response.getCalciteContext(),
                null)
                .onSuccess(result -> {
                    log.trace("Request generated: {}", result);
                    promise.complete(result);
                })
                .onFailure(promise::fail));
    }

    private List<Datamart> generatePhysicalSchemas(List<Datamart> logicalSchemas) {
        return logicalSchemas.stream()
                .map(logicalSchema -> schemaExtender.createPhysicalSchema(logicalSchema, ""))
                .collect(Collectors.toList());
    }
}
