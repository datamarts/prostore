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
package ru.datamart.prostore.query.execution.plugin.adp.dml;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.cache.QueryTemplateKey;
import ru.datamart.prostore.common.cache.QueryTemplateValue;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlrEstimateUtils;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlrPlanResult;
import ru.datamart.prostore.query.execution.plugin.api.request.LlrRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.QueryResultCacheableLlrService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;


@Service("adpLlrService")
public class AdpLlrService extends QueryResultCacheableLlrService {
    private final QueryEnrichmentService queryEnrichmentService;
    private final DatabaseExecutor queryExecutor;

    @Autowired
    public AdpLlrService(@Qualifier("adpQueryEnrichmentService") QueryEnrichmentService adpQueryEnrichmentService,
                         @Qualifier("adpQueryExecutor") DatabaseExecutor adpDatabaseExecutor,
                         @Qualifier("adpQueryTemplateCacheService")
                                 CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                         @Qualifier("adpQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                         @Qualifier("adpSqlDialect") SqlDialect sqlDialect,
                         @Qualifier("adpCalciteDMLQueryParserService") QueryParserService queryParserService) {
        super(queryCacheService, templateExtractor, sqlDialect, queryParserService);
        this.queryEnrichmentService = adpQueryEnrichmentService;
        this.queryExecutor = adpDatabaseExecutor;
    }

    @Override
    protected Future<List<Map<String, Object>>> queryExecute(String enrichedQuery,
                                                             QueryParameters queryParameters,
                                                             List<ColumnMetadata> metadata) {
        return queryExecutor.executeWithParams(enrichedQuery, queryParameters, metadata);
    }

    @Override
    protected Future<LlrPlanResult> estimateQueryExecute(String enrichedQuery, QueryParameters queryParameters) {
        return queryExecutor.executeWithParams("EXPLAIN (FORMAT JSON) " + enrichedQuery, queryParameters,
                        singletonList(LlrEstimateUtils.LLR_ESTIMATE_METADATA))
                .map(resultSet -> new LlrPlanResult(SourceType.ADP, LlrEstimateUtils.extractPlanJson(resultSet)));
    }

    @Override
    protected Future<SqlNode> enrichQuery(LlrRequest request, QueryParserResponse parserResponse) {
        return queryEnrichmentService.getEnrichedSqlNode(EnrichQueryRequest.builder()
                        .deltaInformations(request.getDeltaInformations())
                        .envName(request.getEnvName())
                        .query(request.getWithoutViewsQuery())
                        .schema(request.getSchema())
                        .build(),
                parserResponse);
    }
}
