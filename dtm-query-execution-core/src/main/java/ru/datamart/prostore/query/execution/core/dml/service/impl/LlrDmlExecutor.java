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
package ru.datamart.prostore.query.execution.core.dml.service.impl;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.async.AsyncUtils;
import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.cache.QueryTemplateKey;
import ru.datamart.prostore.common.cache.SourceQueryTemplateValue;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.QueryTemplateResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
import ru.datamart.prostore.query.calcite.core.extension.dml.DmlType;
import ru.datamart.prostore.query.calcite.core.extension.dml.LimitableSqlOrderBy;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlSelectExt;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaQueryPreprocessor;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.dml.dto.LlrRequestContext;
import ru.datamart.prostore.query.execution.core.dml.dto.PluginDeterminationRequest;
import ru.datamart.prostore.query.execution.core.dml.factory.LlrRequestContextFactory;
import ru.datamart.prostore.query.execution.core.dml.service.*;
import ru.datamart.prostore.query.execution.core.dml.service.view.ViewReplacerService;
import ru.datamart.prostore.query.execution.core.metrics.service.MetricsService;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.plugin.api.request.LlrRequest;

@Component
@Slf4j
public class LlrDmlExecutor implements DmlExecutor {

    private final DataSourcePluginService dataSourcePluginService;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final ViewReplacerService viewReplacerService;
    private final InformationSchemaExecutor infoSchemaExecutor;
    private final InformationSchemaDefinitionService infoSchemaDefService;
    private final MetricsService metricsService;
    private final QueryTemplateExtractor templateExtractor;
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService;
    private final LlrRequestContextFactory llrRequestContextFactory;
    private final PluginDeterminationService pluginDeterminationService;
    private final SqlDialect sqlDialect;
    private final SqlParametersTypeExtractor parametersTypeExtractor;

    @Autowired
    public LlrDmlExecutor(DataSourcePluginService dataSourcePluginService,
                          DeltaQueryPreprocessor deltaQueryPreprocessor,
                          ViewReplacerService viewReplacerService,
                          InformationSchemaExecutor infoSchemaExecutor,
                          InformationSchemaDefinitionService infoSchemaDefService,
                          MetricsService metricsService,
                          @Qualifier("coreQueryTmplateExtractor") QueryTemplateExtractor templateExtractor,
                          @Qualifier("coreQueryTemplateCacheService") CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService,
                          LlrRequestContextFactory llrRequestContextFactory,
                          PluginDeterminationService pluginDeterminationService,
                          @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                          SqlParametersTypeExtractor parametersTypeExtractor) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.viewReplacerService = viewReplacerService;
        this.infoSchemaExecutor = infoSchemaExecutor;
        this.infoSchemaDefService = infoSchemaDefService;
        this.metricsService = metricsService;
        this.templateExtractor = templateExtractor;
        this.queryCacheService = queryCacheService;
        this.llrRequestContextFactory = llrRequestContextFactory;
        this.pluginDeterminationService = pluginDeterminationService;
        this.sqlDialect = sqlDialect;
        this.parametersTypeExtractor = parametersTypeExtractor;
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        val queryRequest = context.getRequest().getQueryRequest();
        val sqlNode = context.getSqlNode();
        return AsyncUtils.measureMs(replaceViews(queryRequest, sqlNode),
                duration -> log.debug("Replaced views in request [{}] in [{}]ms", queryRequest.getSql(), duration))
                .compose(sqlNodeWithoutViews -> defineQueryAndExecute(sqlNodeWithoutViews, context));
    }

    private Future<SqlNode> replaceViews(QueryRequest queryRequest,
                                         SqlNode sqlNode) {
        return viewReplacerService.replace(sqlNode, queryRequest.getDatamartMnemonic())
                .map(sqlNodeWithoutViews -> {
                    queryRequest.setSql(sqlNodeWithoutViews.toSqlString(sqlDialect).toString());
                    return sqlNodeWithoutViews;
                });
    }

    private Future<QueryResult> defineQueryAndExecute(SqlNode withoutViewsQuery, DmlRequestContext context) {
        log.debug("Execute sql query [{}]", context.getRequest().getQueryRequest());
        val originalQuery = context.getSqlNode();
        val withSnapshots = SqlNodeUtil.copy(withoutViewsQuery);
        val estimate = isEstimate(context.getSqlNode());
        context.setSqlNode(withoutViewsQuery);

        return AsyncUtils.measureMs(deltaQueryPreprocessor.process(context.getSqlNode()),
                duration -> log.debug("Extracted deltas from query [{}] in [{}]ms",
                        context.getRequest().getQueryRequest().getSql(), duration))
                .compose(deltaResponse -> {
                    if (infoSchemaDefService.isInformationSchemaRequest(deltaResponse.getDeltaInformations())) {
                        return executeInformationSchemaRequest(context, originalQuery, deltaResponse);
                    } else {
                        return executeLlrRequest(context,
                                withoutViewsQuery,
                                withSnapshots,
                                deltaResponse,
                                estimate);
                    }
                });
    }

    private boolean isEstimate(SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelectExt) {
            return ((SqlSelectExt) sqlNode).isEstimate();
        }

        if (sqlNode instanceof LimitableSqlOrderBy) {
            return ((LimitableSqlOrderBy) sqlNode).isEstimate();
        }

        return false;
    }

    private Future<QueryResult> executeInformationSchemaRequest(DmlRequestContext context,
                                                                SqlNode originalQuery,
                                                                DeltaQueryPreprocessorResponse deltaResponse) {
        return initLlrRequestContext(context, deltaResponse)
                .compose(llrRequestContext -> checkAccessAndExecute(llrRequestContext, originalQuery));
    }

    private Future<QueryResult> checkAccessAndExecute(LlrRequestContext llrRequestContext, SqlNode originalQuery) {
        return Future.future(p -> metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                SqlProcessingType.LLR,
                llrRequestContext.getDmlRequestContext().getMetrics())
                .compose(v -> infoSchemaDefService.checkAccessToSystemLogicalTables(originalQuery))
                .compose(v -> infoSchemaExecutor.execute(llrRequestContext.getSourceRequest()))
                .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                        SqlProcessingType.LLR,
                        llrRequestContext.getDmlRequestContext().getMetrics(),
                        p))
        );
    }

    private Future<LlrRequestContext> initLlrRequestContext(DmlRequestContext context,
                                                            DeltaQueryPreprocessorResponse deltaResponse) {
        return llrRequestContextFactory.create(context, deltaResponse)
                .map(llrRequestContext -> {
                    llrRequestContext.getSourceRequest().setQuery(context.getSqlNode());
                    return llrRequestContext;
                });
    }

    private Future<QueryResult> executeLlrRequest(DmlRequestContext context,
                                                  SqlNode withoutViewsQuery,
                                                  SqlNode originalQuery,
                                                  DeltaQueryPreprocessorResponse deltaResponse,
                                                  boolean estimate) {
        return createLlrRequestContext(deltaResponse, withoutViewsQuery, originalQuery, context)
                .compose(llrContext -> AsyncUtils.measureMs(initQuerySourceType(llrContext),
                        duration -> log.debug("Initialized query type for query [{}] in [{}]ms",
                                llrContext.getSourceRequest().getQueryRequest().getSql(), duration))
                        .compose(ignored -> {
                            if (!estimate) {
                                return dataSourcePluginService.llr(llrContext.getExecutionPlugin(),
                                        llrContext.getDmlRequestContext().getMetrics(),
                                        createLlrRequest(llrContext));
                            } else {
                                return dataSourcePluginService.llrEstimate(llrContext.getExecutionPlugin(),
                                        llrContext.getDmlRequestContext().getMetrics(),
                                        createLlrRequest(llrContext));
                            }
                        }));
    }

    private Future<LlrRequestContext> createLlrRequestContext(DeltaQueryPreprocessorResponse deltaResponse,
                                                              SqlNode withoutViewsQuery,
                                                              SqlNode originalQuery,
                                                              DmlRequestContext context) {
        val templateResult = extractQueryTemplate(withoutViewsQuery);
        context.setSqlNode(templateResult.getTemplateNode());
        val queryTemplateValue = queryCacheService.get(QueryTemplateKey.builder()
                .sourceQueryTemplate(templateResult.getTemplate())
                .build());
        if (deltaResponse.isCachable() && queryTemplateValue != null) {
            log.debug("Found query template cache value by key [{}]", templateResult.getTemplate());
            return llrRequestContextFactory.create(context, queryTemplateValue)
                    .map(llrRequestContext -> initLlrRequestContext(deltaResponse, originalQuery, templateResult, llrRequestContext));
        }

        context.setSqlNode(templateExtractor
                .extract(deltaResponse.getSqlNode())
                .getTemplateNode());
        return llrRequestContextFactory.create(context, deltaResponse)
                .map(llrRequestContext -> initLlrRequestContext(deltaResponse, originalQuery, templateResult, llrRequestContext))
                .map(this::setParametersTypes)
                .compose(this::cacheQueryTemplateValueIfNeeded);
    }

    private LlrRequestContext initLlrRequestContext(DeltaQueryPreprocessorResponse deltaResponse, SqlNode originalQuery, QueryTemplateResult templateResult, LlrRequestContext llrRequestContext) {
        llrRequestContext.getSourceRequest().setQueryTemplate(templateResult);
        llrRequestContext.setOriginalQuery(originalQuery);
        llrRequestContext.setDeltaInformations(deltaResponse.getDeltaInformations());
        return llrRequestContext;
    }

    private Future<LlrRequestContext> initQuerySourceType(LlrRequestContext llrContext) {
        return pluginDeterminationService.determine(getPluginDeterminationRequest(llrContext))
                .map(pluginDeterminationResult -> {
                    llrContext.setExecutionPlugin(pluginDeterminationResult.getExecution());
                    return llrContext;
                });
    }

    private QueryTemplateKey createQueryTemplateKey(LlrRequestContext llrContext) {
        return QueryTemplateKey.builder()
                .sourceQueryTemplate(llrContext.getSourceRequest().getQueryTemplate().getTemplate())
                .logicalSchema(llrContext.getSourceRequest().getLogicalSchema())
                .build();
    }

    private PluginDeterminationRequest getPluginDeterminationRequest(LlrRequestContext llrContext) {
        return PluginDeterminationRequest.builder()
                .sqlNode(llrContext.getOriginalQuery())
                .query(llrContext.getDmlRequestContext().getRequest().getQueryRequest().getSql())
                .schema(llrContext.getSourceRequest().getLogicalSchema())
                .preferredSourceType(llrContext.getSourceRequest().getSourceType())
                .build();
    }

    private QueryTemplateResult extractQueryTemplate(SqlNode sqlNode) {
        val copySqlNode = SqlNodeUtil.copy(sqlNode);
        return templateExtractor.extract(copySqlNode);
    }

    private LlrRequestContext setParametersTypes(LlrRequestContext llrRequestContext) {
        val paramTypes = parametersTypeExtractor.extract(llrRequestContext.getRelNode().rel);
        llrRequestContext.setParameterTypes(paramTypes);
        return llrRequestContext;
    }

    private Future<LlrRequestContext> cacheQueryTemplateValueIfNeeded(LlrRequestContext llrRequestContext) {
        if (!llrRequestContext.isCachable()) {
            return Future.succeededFuture(llrRequestContext);
        }

        val newQueryTemplateKey = QueryTemplateKey.builder().build();
        val newQueryTemplateValue = SourceQueryTemplateValue.builder().build();
        llrRequestContext.setQueryTemplateValue(newQueryTemplateValue);
        initQueryTemplate(llrRequestContext, newQueryTemplateKey, newQueryTemplateValue);
        return queryCacheService.put(createQueryTemplateKey(llrRequestContext), llrRequestContext.getQueryTemplateValue())
                .map(v -> llrRequestContext);
    }

    private void initQueryTemplate(LlrRequestContext llrRequestContext,
                                   QueryTemplateKey newQueryTemplateKey,
                                   SourceQueryTemplateValue newQueryTemplateValue) {
        newQueryTemplateKey.setSourceQueryTemplate(llrRequestContext.getSourceRequest().getQueryTemplate().getTemplate());
        newQueryTemplateKey.setLogicalSchema(llrRequestContext.getSourceRequest().getLogicalSchema());
        newQueryTemplateValue.setMetadata(llrRequestContext.getSourceRequest().getMetadata());
        newQueryTemplateValue.setLogicalSchema(llrRequestContext.getSourceRequest().getLogicalSchema());
        newQueryTemplateValue.setSql(llrRequestContext.getSourceRequest().getQueryRequest().getSql());
        newQueryTemplateValue.setParameterTypes(llrRequestContext.getParameterTypes());
    }

    private LlrRequest createLlrRequest(LlrRequestContext context) {
        val queryRequest = context.getDmlRequestContext().getRequest().getQueryRequest();
        return LlrRequest.builder()
                .sourceQueryTemplateResult(context.getSourceRequest().getQueryTemplate())
                .parameters(context.getSourceRequest().getQueryRequest().getParameters())
                .parameterTypes(context.getQueryTemplateValue() == null ? context.getParameterTypes() : context.getQueryTemplateValue().getParameterTypes())
                .withoutViewsQuery(context.getDmlRequestContext().getSqlNode())
                .schema(context.getSourceRequest().getLogicalSchema())
                .envName(context.getDmlRequestContext().getEnvName())
                .datamartMnemonic(queryRequest.getDatamartMnemonic())
                .deltaInformations(context.getDeltaInformations())
                .metadata(context.getSourceRequest().getMetadata())
                .deltaInformations(context.getDeltaInformations())
                .originalQuery(context.getOriginalQuery())
                .requestId(queryRequest.getRequestId())
                .cachable(context.isCachable())
                .build();
    }

    @Override
    public DmlType getType() {
        return DmlType.LLR;
    }

}
