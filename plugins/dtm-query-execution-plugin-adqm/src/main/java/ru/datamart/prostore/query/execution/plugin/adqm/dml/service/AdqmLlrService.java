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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.cache.QueryTemplateKey;
import ru.datamart.prostore.common.cache.QueryTemplateValue;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlrPlanResult;
import ru.datamart.prostore.query.execution.plugin.api.request.LlrRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.LlrValidationService;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.api.service.QueryResultCacheableLlrService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.List;
import java.util.Map;

import static ru.datamart.prostore.query.execution.plugin.adqm.dml.util.AdqmDmlUtils.extendParameters;

@Service("adqmLlrService")
@Slf4j
public class AdqmLlrService extends QueryResultCacheableLlrService {
    private static final LlrPlanResult LLR_EMPTY_ESTIMATE_RESULT = new LlrPlanResult(SourceType.ADQM);
    private final QueryEnrichmentService queryEnrichmentService;
    private final DatabaseExecutor executorService;
    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final LlrValidationService adqmValidationService;

    @Autowired
    public AdqmLlrService(@Qualifier("adqmQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                          @Qualifier("adqmQueryExecutor") DatabaseExecutor adqmQueryExecutor,
                          @Qualifier("adqmQueryTemplateCacheService")
                                  CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                          @Qualifier("adqmQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                          @Qualifier("adqmSqlDialect") SqlDialect sqlDialect,
                          @Qualifier("adqmCalciteDMLQueryParserService") QueryParserService queryParserService,
                          @Qualifier("adqmPluginSpecificLiteralConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                          @Qualifier("adqmValidationService") LlrValidationService adqmValidationService) {
        super(queryCacheService, templateExtractor, sqlDialect, queryParserService);
        this.queryEnrichmentService = queryEnrichmentService;
        this.executorService = adqmQueryExecutor;
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.adqmValidationService = adqmValidationService;
    }

    @Override
    protected Future<List<Map<String, Object>>> queryExecute(String enrichedQuery,
                                                             QueryParameters queryParameters,
                                                             List<ColumnMetadata> metadata) {
        return executorService.executeWithParams(enrichedQuery, queryParameters, metadata);
    }

    @Override
    protected Future<LlrPlanResult> estimateQueryExecute(String enrichedQuery, QueryParameters queryParameters) {
        return Future.succeededFuture(LLR_EMPTY_ESTIMATE_RESULT);
    }

    @Override
    protected QueryParameters getExtendedQueryParameters(LlrRequest request) {
        return extendParameters(request.getSchema(), request.getParameters());
    }

    @Override
    protected void validateQuery(QueryParserResponse parserResponse) {
        adqmValidationService.validate(parserResponse);
    }

    @Override
    protected Future<SqlNode> enrichQuery(LlrRequest request, QueryParserResponse parserResponse) {
        val enrichRequest = EnrichQueryRequest.builder()
                .envName(request.getEnvName())
                .deltaInformations(request.getDeltaInformations())
                .calciteContext(parserResponse.getCalciteContext())
                .relNode(parserResponse.getRelNode())
                .isLocal(false)
                .build();
        return queryEnrichmentService.getEnrichedSqlNode(enrichRequest);
    }

    @Override
    protected List<SqlNode> convertParams(List<SqlNode> params, List<SqlTypeName> parameterTypes) {
        return pluginSpecificLiteralConverter.convert(params, parameterTypes);
    }
}
