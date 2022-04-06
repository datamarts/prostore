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
package ru.datamart.prostore.query.execution.core.ddl.service.impl.table;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.post.PostSqlActionType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlDropTable;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.SetEntityState;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.QueryResultDdlExecutor;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.validate.RelatedViewChecker;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class DropTableExecutor extends QueryResultDdlExecutor {
    private static final String IF_EXISTS = "if exists";

    private final DataSourcePluginService dataSourcePluginService;
    private final CacheService<EntityKey, Entity> entityCacheService;
    private final EntityDao entityDao;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;
    private final RelatedViewChecker relatedViewChecker;

    @Autowired
    public DropTableExecutor(MetadataExecutor metadataExecutor,
                             ServiceDbFacade serviceDbFacade,
                             @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                             @Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                             DataSourcePluginService dataSourcePluginService,
                             EvictQueryTemplateCacheService evictQueryTemplateCacheService,
                             RelatedViewChecker relatedViewChecker) {
        super(metadataExecutor, serviceDbFacade, sqlDialect);
        this.entityCacheService = entityCacheService;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.dataSourcePluginService = dataSourcePluginService;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
        this.relatedViewChecker = relatedViewChecker;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            val datamartName = getSchemaName(context.getDatamartName(), sqlNodeName);
            val tableName = getTableName(sqlNodeName);
            entityCacheService.remove(new EntityKey(datamartName, tableName));

            val entity = createClassTable(datamartName, tableName);
            context.setEntity(entity);
            context.setDatamartName(datamartName);
            context.setSourceType(getSourceType(context));

            dropTable(context, containsIfExistsCheck(context.getRequest().getQueryRequest().getSql()))
                    .onSuccess(r -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    protected SourceType getSourceType(DdlRequestContext context) {
        return ((SqlDropTable) context.getSqlNode()).getDestination().getValue();
    }

    protected Entity createClassTable(String schema, String tableName) {
        return Entity.builder()
                .schema(schema)
                .name(tableName)
                .entityType(EntityType.TABLE)
                .build();
    }

    protected Future<Void> dropTable(DdlRequestContext context, boolean ifExists) {
        return getEntity(context, ifExists)
                .compose(entity -> entity != null ?
                        checkViewsAndUpdateEntity(context, entity, ifExists) :
                        Future.succeededFuture());
    }

    private boolean containsIfExistsCheck(String sql) {
        return sql.toLowerCase().contains(IF_EXISTS);
    }

    private Future<Entity> getEntity(DdlRequestContext context, boolean ifExists) {
        return Future.future(entityPromise -> {
            val datamartName = context.getDatamartName();
            val entityName = context.getEntity().getName();
            entityDao.getEntity(datamartName, entityName)
                    .map(this::checkEntityType)
                    .onSuccess(entityPromise::complete)
                    .onFailure(error -> {
                        if (error instanceof EntityNotExistsException && ifExists) {
                            entityPromise.complete(null);
                        } else {
                            entityPromise.fail(new EntityNotExistsException(datamartName, entityName));
                        }
                    });
        });
    }

    protected Entity checkEntityType(Entity entity) {
        if (EntityType.TABLE != entity.getEntityType()) {
            throw new EntityNotExistsException(entity.getNameWithSchema());
        }

        return entity;
    }

    private Future<Void> checkViewsAndUpdateEntity(DdlRequestContext context, Entity entity, boolean ifExists) {
        val changeQuery = sqlNodeToString(context.getSqlNode());
        return relatedViewChecker.checkRelatedViews(entity, context.getSourceType())
                .compose(ignored -> writeNewChangelogRecord(context.getDatamartName(), entity.getName(), changeQuery)
                        .compose(delta -> updateEntity(context, entity, ifExists, delta, changeQuery)));
    }

    private Future<Void> updateEntity(DdlRequestContext context, Entity entity, boolean ifExists, OkDelta deltaOk, String changeQuery) {
        try {
            evictQueryTemplateCacheService.evictByEntityName(entity.getSchema(), entity.getName());
        } catch (Exception e) {
            return Future.failedFuture(new DtmException("Evict cache error", e));
        }

        val requestDestination = context.getSourceType();
        if (requestDestination == null) {
            val dataSourcesForDeletion = dataSourcePluginService.getSourceTypes();
            dataSourcesForDeletion.retainAll(entity.getDestination());
            context.getEntity().setDestination(dataSourcesForDeletion);
            return dropEntityFromEverywhere(context, deltaOk, changeQuery);
        } else {
            return dropFromDataSource(context, entity, requestDestination, ifExists, deltaOk, changeQuery);
        }
    }

    private Future<Void> dropFromDataSource(DdlRequestContext context,
                                            Entity entity,
                                            SourceType requestDestination,
                                            boolean ifExists,
                                            OkDelta deltaOk,
                                            String changeQuery) {
        if (!entity.getDestination().contains(requestDestination)) {
            return ifExists ? Future.succeededFuture() : Future.failedFuture(
                    new DtmException(String.format("Table [%s] doesn't exist in [%s]",
                            entity.getName(),
                            requestDestination)));
        }

        //find corresponding datasources in request and active plugins configuration
        val activeDataSources = dataSourcePluginService.getSourceTypes();
        if (!activeDataSources.contains(requestDestination)) {
            entity.setDestination(entity.getDestination().stream()
                    .filter(type -> !requestDestination.equals(type))
                    .collect(Collectors.toSet()));
            context.getEntity().setDestination(entity.getDestination());
            return entityDao.setEntityState(entity, deltaOk, changeQuery, SetEntityState.UPDATE);
        }

        entity.setDestination(entity.getDestination().stream()
                .filter(type -> !requestDestination.equals(type))
                .collect(Collectors.toSet()));
        context.getEntity().setDestination(Collections.singleton(requestDestination));
        if (entity.getDestination().isEmpty()) {
            return dropEntityFromEverywhere(context, deltaOk, changeQuery);
        }

        return executeRequest(context)
                .map(v -> {
                    context.getEntity().setDestination(entity.getDestination());
                    return v;
                })
                .compose(v -> entityDao.setEntityState(entity, deltaOk, changeQuery, SetEntityState.UPDATE));


    }

    private Future<Void> dropEntityFromEverywhere(DdlRequestContext context, OkDelta deltaOk, String changeQuery) {
        return executeRequest(context)
                .map(v -> {
                    context.getEntity().setDestination(Collections.emptySet());
                    return v;
                })
                .compose(v -> entityDao.setEntityState(context.getEntity(), deltaOk, changeQuery, SetEntityState.DELETE));
    }

    @Override
    public String getOperationKind() {
        return OperationNames.DROP_TABLE;
    }

    @Override
    public List<PostSqlActionType> getPostActions() {
        return Arrays.asList(PostSqlActionType.PUBLISH_STATUS, PostSqlActionType.UPDATE_INFORMATION_SCHEMA);
    }
}
