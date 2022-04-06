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

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDelete;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.dml.DmlType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.dml.service.SqlParametersTypeExtractor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.api.request.DeleteRequest;

import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class DeleteExecutor extends AbstractLlwExecutor {

    private final DataSourcePluginService pluginService;
    private final DeltaServiceDao deltaServiceDao;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final QueryTemplateExtractor templateExtractor;
    private final QueryParserService queryParserService;
    private final SqlParametersTypeExtractor parametersTypeExtractor;

    public DeleteExecutor(DataSourcePluginService pluginService,
                          ServiceDbFacade serviceDbFacade,
                          RestoreStateService restoreStateService,
                          LogicalSchemaProvider logicalSchemaProvider,
                          @Qualifier("coreQueryTmplateExtractor") QueryTemplateExtractor templateExtractor,
                          @Qualifier("coreCalciteDMLQueryParserService") QueryParserService queryParserService,
                          SqlParametersTypeExtractor parametersTypeExtractor) {
        super(serviceDbFacade.getServiceDbDao().getEntityDao(),
                pluginService,
                serviceDbFacade.getDeltaServiceDao(),
                restoreStateService);
        this.pluginService = pluginService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.templateExtractor = templateExtractor;
        this.queryParserService = queryParserService;
        this.parametersTypeExtractor = parametersTypeExtractor;
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        return validate(context)
                .compose(ignored -> getDestinationEntity(context))
                .compose(this::validateEntityType)
                .compose(this::checkConfiguration)
                .compose(entity -> entity.getEntityType() == EntityType.TABLE ?
                        executeLogical(context, entity) : executeWriteableExternal(context, entity))
                .map(QueryResult.emptyResult());
    }

    private Future<Void> executeWriteableExternal(DmlRequestContext context, Entity entity) {
        String datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        return logicalSchemaProvider.getSchemaFromQuery(context.getSqlNode(), datamart)
                .compose(datamarts -> buildRequest(context, entity, null, null, datamarts))
                .compose(request -> pluginService.delete(entity.getDestination().iterator().next(), context.getMetrics(), request));
    }

    private Future<Void> executeLogical(DmlRequestContext context, Entity entity) {
        String datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        return deltaServiceDao.getDeltaHot(datamart)
                .compose(ignored -> deltaServiceDao.getDeltaOk(datamart))
                .compose(okDelta -> {
                    if (okDelta == null) {
                        return handleDeleteWhenDatamartHasNoData();
                    }
                    return logicalSchemaProvider.getSchemaFromQuery(context.getSqlNode(), datamart)
                            .compose(datamarts -> produceOrRetryWriteOperation(context, entity)
                                    .map(sysCn -> new ParameterHolder(entity, sysCn, okDelta.getCnTo(), datamarts)))
                            .compose(parameterHolder -> runDelete(context, parameterHolder));
                });
    }

    private Future<DeleteRequest> buildRequest(DmlRequestContext context,
                                               Entity entity,
                                               Long sysCn,
                                               Long cnTo,
                                               List<Datamart> datamarts) {
        val uuid = context.getRequest().getQueryRequest().getRequestId();
        val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val env = context.getEnvName();
        val parameters = context.getRequest().getQueryRequest().getParameters();

        val templateResult = templateExtractor.extract(context.getSqlNode());
        return queryParserService.parse(new QueryParserRequest(templateResult.getTemplateNode(), datamarts))
                .map(parserResponse -> parametersTypeExtractor.extract(parserResponse.getRelNode().rel))
                .map(parametersTypes -> new DeleteRequest(uuid, env, datamart, entity, (SqlDelete) context.getSqlNode(), sysCn, cnTo, datamarts, parameters, templateResult.getParams(), parametersTypes));
    }

    private Future<Void> validate(DmlRequestContext context) {
        if (!(context.getSqlNode() instanceof SqlDelete)) {
            return Future.failedFuture(new DtmException("Unsupported sql node"));
        }

        return Future.succeededFuture();
    }

    private Future<Void> handleDeleteWhenDatamartHasNoData() {
        return Future.succeededFuture();
    }

    private Future<Void> runDelete(DmlRequestContext context, ParameterHolder parameterHolder) {
        val operation = buildRequest(context, parameterHolder.entity, parameterHolder.sysCn, parameterHolder.cnTo, parameterHolder.datamarts)
                .compose(request -> {
                    log.info("Executing LL-W[{}] request: {}", getType(), request);
                    return runOperation(context, request);
                });
        return handleOperation(operation, parameterHolder.sysCn, context.getRequest().getQueryRequest().getDatamartMnemonic(), parameterHolder.entity);
    }

    private Future<?> runOperation(DmlRequestContext context, DeleteRequest request) {
        List<Future> futures = new ArrayList<>();
        request.getEntity().getDestination().forEach(destination ->
                futures.add(pluginService.delete(destination, context.getMetrics(), request)));
        return CompositeFuture.join(futures);
    }

    @Override
    public DmlType getType() {
        return DmlType.DELETE;
    }

    @AllArgsConstructor
    protected static class ParameterHolder {
        private final Entity entity;
        private final Long sysCn;
        private final Long cnTo;
        private final List<Datamart> datamarts;
    }
}
