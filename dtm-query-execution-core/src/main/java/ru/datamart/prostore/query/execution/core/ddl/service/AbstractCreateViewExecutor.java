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
package ru.datamart.prostore.query.execution.core.ddl.service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicatePart;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicates;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.node.SqlTreeNode;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.exception.view.ViewDisalowedOrDirectiveException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.base.utils.InformationSchemaUtils;

import java.util.*;

public abstract class AbstractCreateViewExecutor extends QueryResultDdlExecutor {

    private static final Set<EntityType> ALLOWED_ENTITY_TYPES = EnumSet.of(EntityType.TABLE, EntityType.READABLE_EXTERNAL_TABLE);
    private static final SqlPredicates VIEW_AND_TABLE_PREDICATE = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eqWithNum(SqlKind.JOIN), SqlPredicatePart.eq(SqlKind.SELECT))
            .maybeOf(SqlPredicatePart.eq(SqlKind.AS))
            .anyOf(SqlPredicatePart.eq(SqlKind.SNAPSHOT), SqlPredicatePart.eq(SqlKind.IDENTIFIER))
            .build();

    protected final EntityDao entityDao;
    protected final CacheService<EntityKey, Entity> entityCacheService;

    protected AbstractCreateViewExecutor(MetadataExecutor metadataExecutor, ServiceDbFacade serviceDbFacade, SqlDialect sqlDialect, EntityDao entityDao, CacheService<EntityKey, Entity> entityCacheService) {
        super(metadataExecutor, serviceDbFacade, sqlDialect);
        this.entityDao = entityDao;
        this.entityCacheService = entityCacheService;
    }

    protected Future<Void> checkSnapshotNotExist(SqlSelectTree selectTree) {
        if (!selectTree.findSnapshots().isEmpty()) {
            return Future.failedFuture(new ViewDisalowedOrDirectiveException(selectTree.getRoot().getNode().toSqlString(sqlDialect).getSql()));
        }
        return Future.succeededFuture();
    }

    protected Future<Void> checkEntitiesType(SqlNode sqlNode, String contextDatamartName) {
        return Future.future(promise -> {
            final SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
            checkEntitiesType(selectTree, contextDatamartName)
                    .onComplete(promise);
        });
    }

    protected Future<Void> checkEntitiesType(SqlSelectTree selectTree, String contextDatamartName) {
        return Future.future(promise -> {
            final List<SqlTreeNode> nodes = selectTree.findNodes(VIEW_AND_TABLE_PREDICATE, true);
            final List<Future> entityFutures = getEntitiesFutures(contextDatamartName, selectTree.getRoot().getNode(), nodes);
            CompositeFuture.join(entityFutures)
                    .onSuccess(result -> {
                        final List<Entity> entities = result.list();
                        if (entities.stream().anyMatch(entity -> !ALLOWED_ENTITY_TYPES.contains(entity.getEntityType()))) {
                            promise.fail(new ViewDisalowedOrDirectiveException(
                                    selectTree.getRoot().getNode().toSqlString(sqlDialect).getSql()));
                        }
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }

    private List<Future> getEntitiesFutures(String contextDatamartName, SqlNode sqlNode, List<SqlTreeNode> nodes) {
        final List<Future> entityFutures = new ArrayList<>();
        nodes.forEach(node -> {
            String datamartName = contextDatamartName;
            String tableName;
            final Optional<String> schema = node.tryGetSchemaName();
            final Optional<String> table = node.tryGetTableName();
            if (schema.isPresent()) {
                datamartName = schema.get();
            }

            tableName = table.orElseThrow(() -> new DtmException(String.format("Can't extract table name from query %s",
                    sqlNode.toSqlString(sqlDialect).toString())));

            if (InformationSchemaUtils.INFORMATION_SCHEMA.equalsIgnoreCase(datamartName)) {
                throw new DtmException(String.format("Using of INFORMATION_SCHEMA is forbidden [%s.%s]", datamartName, tableName));
            }

            entityCacheService.remove(new EntityKey(datamartName, tableName));
            entityFutures.add(entityDao.getEntity(datamartName, tableName));
        });
        return entityFutures;
    }
}
