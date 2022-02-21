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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.cache.QueryTemplateKey;
import ru.datamart.prostore.common.cache.QueryTemplateValue;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.QueryExecutorService;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlrPlanResult;
import ru.datamart.prostore.query.execution.plugin.api.request.LlrRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.LlrValidationService;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.api.service.QueryResultCacheableLlrService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service("adgLlrService")
public class AdgLlrService extends QueryResultCacheableLlrService {
    private static final LlrPlanResult LLR_EMPTY_ESTIMATE_RESULT = new LlrPlanResult(SourceType.ADG, null);
    private final QueryEnrichmentService queryEnrichmentService;
    private final QueryExecutorService executorService;
    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final LlrValidationService adgValidationService;

    @Autowired
    public AdgLlrService(@Qualifier("adgQueryEnrichmentService") QueryEnrichmentService adgQueryEnrichmentService,
                         QueryExecutorService executorService,
                         @Qualifier("adgQueryTemplateCacheService")
                                 CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                         @Qualifier("adgQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                         @Qualifier("adgSqlDialect") SqlDialect sqlDialect,
                         @Qualifier("adgCalciteDMLQueryParserService") QueryParserService queryParserService,
                         @Qualifier("adgTemplateParameterConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                         @Qualifier("adgValidationService") LlrValidationService adgValidationService) {
        super(queryCacheService, templateExtractor, sqlDialect, queryParserService);
        this.queryEnrichmentService = adgQueryEnrichmentService;
        this.executorService = executorService;
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.adgValidationService = adgValidationService;
    }

    @Override
    protected Future<List<Map<String, Object>>> queryExecute(String enrichedQuery,
                                                             QueryParameters queryParameters,
                                                             List<ColumnMetadata> metadata) {
        return executorService.execute(enrichedQuery, queryParameters, metadata);
    }

    @Override
    protected Future<LlrPlanResult> estimateQueryExecute(String enrichedQuery, QueryParameters queryParameters) {
        return Future.succeededFuture(LLR_EMPTY_ESTIMATE_RESULT);
    }

    @Override
    protected void validateQuery(QueryParserResponse parserResponse) {
        adgValidationService.validate(parserResponse);
    }

    @Override
    protected Future<SqlNode> enrichQuery(LlrRequest request, QueryParserResponse parserResponse) {
        return queryEnrichmentService.getEnrichedSqlNode(EnrichQueryRequest.builder()
                .deltaInformations(request.getDeltaInformations())
                .envName(request.getEnvName())
                .query(request.getWithoutViewsQuery())
                .schema(request.getSchema())
                .build(), parserResponse);
    }

    @Override
    protected List<SqlNode> convertParams(List<SqlNode> params, List<SqlTypeName> parameterTypes) {
        return pluginSpecificLiteralConverter.convert(params, parameterTypes);
    }

}
