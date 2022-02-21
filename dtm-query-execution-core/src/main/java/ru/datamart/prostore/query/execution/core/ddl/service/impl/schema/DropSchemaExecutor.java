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
package ru.datamart.prostore.query.execution.core.ddl.service.impl.schema;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.calcite.core.extension.eddl.DropDatabase;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.service.hsql.HSQLClient;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.base.utils.InformationSchemaUtils;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.QueryResultDdlExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.core.ddl.dto.DdlType.DROP_SCHEMA;

@Slf4j
@Component
public class DropSchemaExecutor extends QueryResultDdlExecutor {

    private static final String MATERIALIZED_VIEW_PREFIX = "SYS_";

    private final CacheService<String, HotDelta> hotDeltaCacheService;
    private final CacheService<String, OkDelta> okDeltaCacheService;
    private final CacheService<EntityKey, Entity> entityCacheService;
    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;
    private final DatamartDao datamartDao;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;
    private final HSQLClient hsqlClient;

    @Autowired
    public DropSchemaExecutor(MetadataExecutor metadataExecutor,
                              ServiceDbFacade serviceDbFacade,
                              @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                              @Qualifier("hotDeltaCacheService") CacheService<String, HotDelta> hotDeltaCacheService,
                              @Qualifier("okDeltaCacheService") CacheService<String, OkDelta> okDeltaCacheService,
                              @Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                              @Qualifier("materializedViewCacheService") CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService,
                              EvictQueryTemplateCacheService evictQueryTemplateCacheService, HSQLClient hsqlClient) {
        super(metadataExecutor, serviceDbFacade, sqlDialect);
        this.hotDeltaCacheService = hotDeltaCacheService;
        this.okDeltaCacheService = okDeltaCacheService;
        this.entityCacheService = entityCacheService;
        this.materializedViewCacheService = materializedViewCacheService;
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
        this.hsqlClient = hsqlClient;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            String datamartName = ((DropDatabase) context.getSqlNode()).getName().getSimple();
            clearCacheByDatamartName(datamartName);
            context.getRequest().getQueryRequest().setDatamartMnemonic(datamartName);
            context.setDatamartName(datamartName);
            checkRelatedView(datamartName)
                    .compose(ignore -> datamartDao.existsDatamart(datamartName))
                    .compose(isExists -> {
                        if (isExists) {
                            try {
                                evictQueryTemplateCacheService.evictByDatamartName(datamartName);
                                context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
                                context.setDdlType(DROP_SCHEMA);
                                return executeRequest(context);
                            } catch (Exception e) {
                                return Future.failedFuture(new DtmException("Evict cache error", e));
                            }
                        } else {
                            return getNotExistsDatamartFuture(datamartName);
                        }
                    })
                    .compose(r -> dropDatamart(datamartName))
                    .onSuccess(success -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    private Future<Void> checkRelatedView(String datamartName) {
        return Future.future(promise -> hsqlClient.getQueryResult(String.format(InformationSchemaUtils.CHECK_VIEW_BY_TABLE_SCHEMA, datamartName.toUpperCase(), datamartName.toUpperCase()))
                .onSuccess(resultSet -> {
                    if (resultSet.getResults().isEmpty()) {
                        promise.complete();
                    } else {
                        List<String> viewNames = resultSet.getResults().stream()
                                .map(array -> {
                                    String datamart = array.getString(0);
                                    String view = array.getString(1);
                                    if (view.startsWith(MATERIALIZED_VIEW_PREFIX)) {
                                        view = view.substring(MATERIALIZED_VIEW_PREFIX.length());
                                    }
                                    return datamart + "." + view;
                                })
                                .collect(Collectors.toList());
                        promise.fail(new DtmException(String.format("Views %s using the '%s' must be dropped first", viewNames, datamartName.toUpperCase())));
                    }
                })
                .onFailure(promise::fail));
    }

    private void clearCacheByDatamartName(String datamartName) {
        entityCacheService.removeIf(ek -> ek.getDatamartName().equals(datamartName));
        materializedViewCacheService.forEach(((entityKey, cacheValue) -> {
            if (entityKey.getDatamartName().equals(datamartName)) {
                cacheValue.markForDeletion();
            }
        }));
        hotDeltaCacheService.remove(datamartName);
        okDeltaCacheService.remove(datamartName);
    }

    private Future<Void> getNotExistsDatamartFuture(String datamartName) {
        return Future.failedFuture(new DatamartNotExistsException(datamartName));
    }

    private Future<Void> dropDatamart(String datamartName) {
        log.debug("Delete schema [{}] in data sources", datamartName);
        return datamartDao.deleteDatamart(datamartName)
                .onSuccess(success -> log.debug("Deleted datamart [{}] from datamart registry", datamartName));
    }

    @Override
    public String getOperationKind() {
        return OperationNames.DROP_DATABASE;
    }
}
