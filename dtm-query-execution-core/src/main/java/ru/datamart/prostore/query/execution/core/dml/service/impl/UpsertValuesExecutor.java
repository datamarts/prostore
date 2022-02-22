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
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.calcite.core.extension.dml.DmlType;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.dml.service.impl.validate.BasicUpdateColumnsValidator;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.UpsertValuesRequest;

import java.util.ArrayList;
import java.util.List;

@Component
public class UpsertValuesExecutor extends AbstractUpdateExecutor<UpsertValuesRequest> {

    private final DataSourcePluginService pluginService;

    public UpsertValuesExecutor(DataSourcePluginService pluginService,
                                ServiceDbFacade serviceDbFacade,
                                RestoreStateService restoreStateService,
                                BasicUpdateColumnsValidator updateColumnsValidator) {
        super(pluginService, serviceDbFacade, restoreStateService, updateColumnsValidator);
        this.pluginService = pluginService;
    }

    @Override
    protected boolean isValidSource(SqlNode sqlInsert) {
        return LlwUtils.isValuesSqlNode(sqlInsert);
    }

    @Override
    protected Future<UpsertValuesRequest> buildRequest(DmlRequestContext context, Long sysCn, Entity entity) {
        val uuid = context.getRequest().getQueryRequest().getRequestId();
        val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val env = context.getEnvName();
        val params = context.getRequest().getQueryRequest().getParameters();
        return Future.succeededFuture(new UpsertValuesRequest(uuid, env, datamart, sysCn, entity, (SqlInsert) context.getSqlNode(), params));
    }

    @Override
    protected Future<?> runOperation(DmlRequestContext context, UpsertValuesRequest request) {
        List<Future> futures = new ArrayList<>();
        request.getEntity().getDestination().forEach(destination ->
                futures.add(pluginService.upsert(destination, context.getMetrics(), request)));
        return CompositeFuture.join(futures);
    }

    @Override
    public DmlType getType() {
        return DmlType.UPSERT_VALUES;
    }
}
