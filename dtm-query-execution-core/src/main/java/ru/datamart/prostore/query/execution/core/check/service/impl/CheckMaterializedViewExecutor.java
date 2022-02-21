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
package ru.datamart.prostore.query.execution.core.check.service.impl;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckMaterializedView;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.factory.CheckMaterializedViewResultFactory;
import ru.datamart.prostore.query.execution.core.check.service.CheckExecutor;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service("checkMaterializedViewExecutor")
public class CheckMaterializedViewExecutor implements CheckExecutor {

    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;
    private final EntityDao entityDao;
    private final DeltaServiceDao deltaServiceDao;
    private final CheckMaterializedViewResultFactory resultFactory;

    @Autowired
    public CheckMaterializedViewExecutor(EntityDao entityDao,
                                         CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService,
                                         DeltaServiceDao deltaServiceDao,
                                         CheckMaterializedViewResultFactory resultFactory) {
        this.materializedViewCacheService = materializedViewCacheService;
        this.entityDao = entityDao;
        this.deltaServiceDao = deltaServiceDao;
        this.resultFactory = resultFactory;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        return Future.future(p -> {
            val sqlNode = (SqlCheckMaterializedView) context.getSqlNode();
            val materializedView = sqlNode.getMaterializedView();
            val datamart = getDatamartByPriority(sqlNode.getSchema(), context);

            if (StringUtils.isBlank(datamart)) {
                throw new DtmException("Default datamart must be set or present in query");
            }

            getEntities(datamart, materializedView)
                    .compose(entities -> prepareResult(datamart, entities))
                    .onComplete(p);
        });
    }

    private Future<QueryResult> prepareResult(String datamart, List<Entity> entities) {
        return deltaServiceDao.getDeltaOk(datamart)
                .map(okDelta -> {
                    Long deltaNum = okDelta != null ? okDelta.getDeltaNum() : null;
                    List<CheckMaterializedViewResultFactory.MatviewEntry> matviewEntries = new ArrayList<>();
                    for (Entity entity : entities) {
                        if (entity.getEntityType() != EntityType.MATERIALIZED_VIEW) {
                            continue;
                        }

                        val key = new EntityKey(entity.getSchema(), entity.getName());
                        val cacheValue = materializedViewCacheService.get(key);
                        if (cacheValue == null) {
                            continue;
                        }

                        matviewEntries.add(new CheckMaterializedViewResultFactory.MatviewEntry(entity, cacheValue));
                    }

                    return resultFactory.create(deltaNum, matviewEntries);
                });
    }

    private Future<List<Entity>> getEntities(String datamart, String materializedView) {
        if (materializedView != null) {
            return entityDao.getEntity(datamart, materializedView)
                    .map(entity -> {
                        if (entity.getEntityType() != EntityType.MATERIALIZED_VIEW) {
                            throw new DtmException(String.format("Entity [%s] is not MATERIALIZED_VIEW", entity.getNameWithSchema()));
                        }

                        return Collections.singletonList(entity);
                    });
        }

        return entityDao.getEntityNamesByDatamart(datamart)
                .compose(entityNames -> {
                    List<Future> entitiesFutures = entityNames.stream()
                            .map(entity -> entityDao.getEntity(datamart, entity))
                            .collect(Collectors.toList());
                    return CompositeFuture.join(entitiesFutures)
                            .map(CompositeFuture::list);
                });
    }

    private String getDatamartByPriority(String schema, CheckContext context) {
        if (schema != null) {
            return schema;
        }

        return context.getRequest().getQueryRequest().getDatamartMnemonic();
    }

    @Override
    public CheckType getType() {
        return CheckType.MATERIALIZED_VIEW;
    }
}
