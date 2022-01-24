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
package io.arenadata.dtm.query.execution.core.ddl.service.impl.view;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.OperationNames;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.SetEntityState;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.ddl.utils.SqlPreparer;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DropViewExecutor extends QueryResultDdlExecutor {
    private final CacheService<EntityKey, Entity> entityCacheService;
    protected final EntityDao entityDao;

    @Autowired
    public DropViewExecutor(MetadataExecutor metadataExecutor,
                            ServiceDbFacade serviceDbFacade,
                            @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                            @Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService) {
        super(metadataExecutor, serviceDbFacade, sqlDialect);
        this.entityCacheService = entityCacheService;
        entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            val tree = new SqlSelectTree(context.getSqlNode());
            val viewNameNode = SqlPreparer.getViewNameNode(tree);
            val datamartName = viewNameNode.tryGetSchemaName()
                    .orElseThrow(() -> new DtmException("Unable to get schema of view"));
            val viewName = viewNameNode.tryGetTableName()
                    .orElseThrow(() -> new DtmException("Unable to get name of view"));
            val changeQuery = sqlNodeToString(context.getSqlNode());
            context.setDatamartName(datamartName);
            entityCacheService.remove(new EntityKey(datamartName, viewName));
            entityDao.getEntity(datamartName, viewName)
                    .map(entity -> {
                        context.setEntity(entity);
                        return entity;
                    })
                    .compose(this::checkEntityType)
                    .compose(v -> writeNewChangelogRecord(datamartName, viewName, changeQuery))
                    .compose(deltaOk -> entityDao.setEntityState(context.getEntity(), deltaOk, changeQuery, SetEntityState.DELETE))
                    .onSuccess(success -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    private Future<Void> checkEntityType(Entity entity) {
        if (EntityType.VIEW == entity.getEntityType()) {
            return Future.succeededFuture();
        } else {
            return Future.failedFuture(new EntityNotExistsException(entity.getNameWithSchema()));
        }
    }

    @Override
    public String getOperationKind() {
        return OperationNames.DROP_VIEW;
    }
}
