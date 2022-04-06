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
package ru.datamart.prostore.query.execution.core.eddl.service.standalone;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.eddl.dto.CreateStandaloneExternalTableQuery;
import ru.datamart.prostore.query.execution.core.eddl.dto.EddlQuery;
import ru.datamart.prostore.query.execution.core.eddl.service.EddlExecutor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

public abstract class CreateStandaloneExternalTableExecutor implements EddlExecutor {

    private static final String AUTO_CREATE_TABLE_ENABLE = "auto.create.table.enable";

    private final DataSourcePluginService dataSourcePluginService;
    private final EntityDao entityDao;
    private final DatamartDao datamartDao;
    private final UpdateInfoSchemaStandalonePostExecutor postExecutor;

    @Autowired
    protected CreateStandaloneExternalTableExecutor(DataSourcePluginService dataSourcePluginService,
                                                    EntityDao entityDao,
                                                    DatamartDao datamartDao,
                                                    UpdateInfoSchemaStandalonePostExecutor postExecutor) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.entityDao = entityDao;
        this.datamartDao = datamartDao;
        this.postExecutor = postExecutor;
    }

    @Override
    public Future<QueryResult> execute(EddlQuery query) {
        return Future.future(promise -> {
            val createQuery = (CreateStandaloneExternalTableQuery) query;
            val entity = createQuery.getEntity();
            val datamart = entity.getSchema();
            val isAutoCreateEnable = Boolean.parseBoolean(entity.getExternalTableOptions().get(AUTO_CREATE_TABLE_ENABLE));
            datamartDao.existsDatamart(datamart)
                    .compose(datamartExists -> datamartExists ? Future.succeededFuture()
                            : Future.failedFuture(new DatamartNotExistsException(datamart)))
                    .compose(result -> executeInPluginIfNeeded(isAutoCreateEnable, createQuery))
                    .compose(ignored -> entityDao.createEntity(createQuery.getEntity()))
                    .map(ignored -> {
                        postExecutor.execute(query);
                        return QueryResult.emptyResult();
                    })
                    .onComplete(promise);
        });
    }

    private Future<Void> executeInPluginIfNeeded(boolean isAutoCreateEnable, CreateStandaloneExternalTableQuery createQuery) {
        if (!isAutoCreateEnable) {
            return Future.succeededFuture();
        }

        val request = EddlRequest.builder()
                .datamartMnemonic(createQuery.getSchemaName())
                .envName(createQuery.getEnvName())
                .entity(createQuery.getEntity())
                .createRequest(true)
                .build();
        return dataSourcePluginService.eddl(createQuery.getSourceType(), createQuery.getMetrics(), request);
    }
}
