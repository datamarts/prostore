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
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.base.exception.table.ValidationDtmException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.dml.service.impl.validate.UpdateColumnsValidator;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;
import ru.datamart.prostore.query.execution.plugin.api.request.LlwRequest;

@Slf4j
public abstract class AbstractUpdateExecutor<REQ extends LlwRequest<?>> extends AbstractLlwExecutor {
    private final DeltaServiceDao deltaServiceDao;
    private final UpdateColumnsValidator updateColumnsValidator;

    protected AbstractUpdateExecutor(DataSourcePluginService pluginService,
                                     ServiceDbFacade serviceDbFacade,
                                     RestoreStateService restoreStateService,
                                     UpdateColumnsValidator updateColumnsValidator) {
        super(serviceDbFacade.getServiceDbDao().getEntityDao(), pluginService,
                serviceDbFacade.getDeltaServiceDao(), restoreStateService);
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.updateColumnsValidator = updateColumnsValidator;
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        return validateSqlNode(context)
                .compose(ignored -> getDestinationEntity(context))
                .compose(this::validateEntityType)
                .compose(entity -> updateColumnsValidator.validate(context.getSqlNode(), entity))
                .compose(this::checkConfiguration)
                .compose(entity -> deltaServiceDao.getDeltaHot(context.getRequest().getQueryRequest().getDatamartMnemonic())
                        .compose(ignored -> produceOrResumeWriteOperation(context, entity))
                        .map(sysCn -> new SysCnEntityHolder(entity, sysCn)))
                .compose(sysCnEntityHolder -> runLlw(context, sysCnEntityHolder))
                .map(QueryResult.emptyResult());
    }

    protected abstract boolean isValidSource(SqlNode sqlInsert);

    protected abstract Future<REQ> buildRequest(DmlRequestContext context, Long sysCn, Entity entity);

    protected abstract Future<?> runOperation(DmlRequestContext context, REQ request);

    private Future<Void> validateSqlNode(DmlRequestContext context) {
        if (!(context.getSqlNode() instanceof SqlInsert)) {
            return Future.failedFuture(new ValidationDtmException("Unsupported sql node"));
        }

        val originalSqlInsert = (SqlInsert) context.getSqlNode();
        if (!isValidSource(originalSqlInsert.getSource())) {
            return Future.failedFuture(new ValidationDtmException(String.format("Invalid source for [%s]", getType())));
        }

        return Future.succeededFuture();
    }

    private Future<Void> runLlw(DmlRequestContext context, SysCnEntityHolder sysCnEntityHolder) {
        val operation = buildRequest(context, sysCnEntityHolder.sysCn, sysCnEntityHolder.entity)
                .compose(request -> {
                    log.info("Executing LL-W[{}] request: {}", getType(), request);
                    return runOperation(context, request);
                });
        return handleOperation(operation, sysCnEntityHolder.sysCn, context.getRequest().getQueryRequest().getDatamartMnemonic(), sysCnEntityHolder.entity);
    }

    @AllArgsConstructor
    protected static class SysCnEntityHolder {
        private final Entity entity;
        private final Long sysCn;
    }
}
