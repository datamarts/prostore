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
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.eddl.dto.DropStandaloneExternalTableQuery;
import ru.datamart.prostore.query.execution.core.eddl.dto.EddlQuery;
import ru.datamart.prostore.query.execution.core.eddl.service.EddlExecutor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

public abstract class DropStandaloneExternalTableExecutor implements EddlExecutor {

    private static final String AUTO_DROP_TABLE_ENABLE = "auto.drop.table.enable";

    private final DataSourcePluginService dataSourcePluginService;
    private final EntityDao entityDao;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;
    private final UpdateInfoSchemaStandalonePostExecutor postExecutor;

    protected DropStandaloneExternalTableExecutor(DataSourcePluginService dataSourcePluginService,
                                                  EntityDao entityDao,
                                                  EvictQueryTemplateCacheService evictQueryTemplateCacheService,
                                                  UpdateInfoSchemaStandalonePostExecutor postExecutor) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.entityDao = entityDao;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
        this.postExecutor = postExecutor;
    }

    @Override
    public Future<QueryResult> execute(EddlQuery query) {
        return Future.future(promise -> {
            val dropQuery = (DropStandaloneExternalTableQuery) query;
            val datamart = dropQuery.getSchemaName();
            val tableName = dropQuery.getTableName();
            val isAutoCreateEnable = Boolean.parseBoolean(dropQuery.getOptions().get(AUTO_DROP_TABLE_ENABLE));
            entityDao.getEntity(datamart, tableName)
                    .compose(this::validateEntity)
                    .compose(entity -> executeInPluginIfNeeded(isAutoCreateEnable, dropQuery, entity))
                    .compose(ignored -> entityDao.deleteEntity(datamart, tableName))
                    .compose(ignored -> evictQueryTemplateTemplate(datamart, tableName))
                    .map(ignored -> {
                        postExecutor.execute(query);
                        return QueryResult.emptyResult();
                    })
                    .onComplete(promise);
        });
    }

    private Future<Void> evictQueryTemplateTemplate(String datamart, String name) {
        try {
            evictQueryTemplateCacheService.evictByEntityName(datamart, name);
            return Future.succeededFuture();
        } catch (Exception e) {
            return Future.failedFuture(new DtmException("Evict cache error", e));
        }
    }

    private Future<Void> executeInPluginIfNeeded(boolean isAutoCreateEnable, DropStandaloneExternalTableQuery dropQuery, Entity entity) {
        if (!isAutoCreateEnable) {
            return Future.succeededFuture();
        }

        return Future.future(promise -> {
            val request = EddlRequest.builder()
                    .datamartMnemonic(dropQuery.getSchemaName())
                    .envName(dropQuery.getEnvName())
                    .entity(entity)
                    .createRequest(false)
                    .build();
            dataSourcePluginService.eddl(SourceType.fromExternalTableLocationType(entity.getExternalTableLocationType()), dropQuery.getMetrics(), request)
                    .onComplete(promise);
        });
    }

    protected abstract Future<Entity> validateEntity(Entity entity);
}
