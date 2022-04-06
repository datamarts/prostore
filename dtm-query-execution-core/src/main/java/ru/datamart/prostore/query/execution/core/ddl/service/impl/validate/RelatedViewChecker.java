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
package ru.datamart.prostore.query.execution.core.ddl.service.impl.validate;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.ext.sql.ResultSet;
import lombok.val;
import lombok.var;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.service.hsql.HSQLClient;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class RelatedViewChecker {
    private static final String MATERIALIZED_VIEW_PREFIX = "SYS_";
    private static final String CHECK_VIEW_BY_TABLE_NAME =
            "SELECT VIEW_SCHEMA, VIEW_NAME\n" +
                    "FROM   INFORMATION_SCHEMA.VIEW_TABLE_USAGE\n" +
                    "WHERE  TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';";
    private static final String CHECK_VIEW_BY_TABLE_SCHEMA =
            "SELECT VIEW_SCHEMA, VIEW_NAME\n" +
                    "FROM   INFORMATION_SCHEMA.VIEW_TABLE_USAGE\n" +
                    "WHERE  TABLE_SCHEMA = '%s' AND VIEW_SCHEMA != '%s';";
    private static final String ERROR_TEMPLATE = "Views %s using the '%s' must be dropped first";

    private final EntityDao entityDao;
    private final HSQLClient hsqlClient;

    public RelatedViewChecker(EntityDao entityDao,
                              HSQLClient hsqlClient) {
        this.entityDao = entityDao;
        this.hsqlClient = hsqlClient;
    }

    public Future<Void> checkRelatedViews(String datamartName) {
        return Future.future(promise -> {
            val upperDatamartName = datamartName.toUpperCase();
            hsqlClient.getQueryResult(String.format(CHECK_VIEW_BY_TABLE_SCHEMA, upperDatamartName, upperDatamartName))
                    .map(this::toViewNames)
                    .onFailure(promise::fail)
                    .onSuccess(viewNames -> {
                        if (viewNames.isEmpty()) {
                            promise.complete();
                            return;
                        }

                        promise.fail(new DtmException(String.format(ERROR_TEMPLATE, viewNames, upperDatamartName)));
                    });
        });
    }

    public Future<Void> checkRelatedViews(Entity entity, SourceType deletedFrom) {
        return Future.future(promise -> {
            val upperDatamartName = entity.getSchema().toUpperCase();
            val upperEntityName = entity.getName().toUpperCase();
            hsqlClient.getQueryResult(String.format(CHECK_VIEW_BY_TABLE_NAME, upperDatamartName, upperEntityName))
                    .map(this::toViewNames)
                    .compose(this::toViewEntities)
                    .map(entities -> toBlockingViews(entity, entities, deletedFrom))
                    .onFailure(promise::fail)
                    .onSuccess(blockingViews -> {
                        if (blockingViews.isEmpty()) {
                            promise.complete();
                            return;
                        }
                        promise.fail(new DtmException(String.format(ERROR_TEMPLATE, blockingViews, upperEntityName)));
                    });
        });
    }

    private List<EntityName> toBlockingViews(Entity entity, List<Entity> views, SourceType deletedFrom) {
        val canBeSafelyDeletedForMatview = entity.getDestination().size() > 1 && deletedFrom != null;
        return views.stream()
                .filter(view -> isCannotBeDeletedSafely(view, canBeSafelyDeletedForMatview, deletedFrom))
                .map(e -> new EntityName(e.getSchema(), e.getName()))
                .collect(Collectors.toList());
    }

    private boolean isCannotBeDeletedSafely(Entity view, boolean canBeDeletedForMatviews, SourceType deletedFrom) {
        if (!canBeDeletedForMatviews || view.getEntityType() != EntityType.MATERIALIZED_VIEW) {
            return true;
        }

        return view.getMaterializedDataSource() == deletedFrom;
    }

    private Future<List<Entity>> toViewEntities(List<EntityName> entityNames) {
        val collect = entityNames.stream()
                .map(entityName -> entityDao.getEntity(entityName.datamart.toLowerCase(), entityName.name.toLowerCase()))
                .collect(Collectors.<Future>toList());
        return CompositeFuture.join(collect)
                .map(CompositeFuture::list);
    }

    private List<EntityName> toViewNames(ResultSet resultSet) {
        if (resultSet.getResults().isEmpty()) {
            return Collections.emptyList();
        }

        return resultSet.getResults().stream()
                .map(array -> {
                    val datamart = array.getString(0);
                    var view = array.getString(1);
                    if (view.startsWith(MATERIALIZED_VIEW_PREFIX)) {
                        view = view.substring(MATERIALIZED_VIEW_PREFIX.length());
                    }
                    return new EntityName(datamart, view);
                })
                .collect(Collectors.toList());
    }

    private static class EntityName {
        private final String datamart;
        private final String name;

        public EntityName(String datamart, String name) {
            this.datamart = datamart;
            this.name = name;
        }

        @Override
        public String toString() {
            return datamart + "." + name;
        }
    }
}
