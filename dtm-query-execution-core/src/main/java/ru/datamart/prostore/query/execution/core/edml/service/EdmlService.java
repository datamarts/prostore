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
package ru.datamart.prostore.query.execution.core.edml.service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.dto.TableInfo;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.service.DatamartExecutionService;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlAction;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Processing EDML-request service
 */
@Slf4j
@Service("coreEdmlService")
public class EdmlService implements DatamartExecutionService<EdmlRequestContext> {
    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private static final Set<EntityType> DOWNLOAD_SOURCES = EnumSet.of(EntityType.TABLE, EntityType.VIEW, EntityType.MATERIALIZED_VIEW, EntityType.READABLE_EXTERNAL_TABLE);

    private final EntityDao entityDao;
    private final Map<EdmlAction, EdmlExecutor> executors;

    @Autowired
    public EdmlService(ServiceDbFacade serviceDbFacade,
                       List<EdmlExecutor> edmlExecutors) {
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.executors = edmlExecutors.stream().collect(Collectors.toMap(EdmlExecutor::getAction, it -> it));
    }

    public Future<QueryResult> execute(EdmlRequestContext request) {
        return defineEdmlAction(request)
                .compose(edmlType -> executeInternal(request, edmlType));
    }

    private Future<EdmlAction> defineEdmlAction(EdmlRequestContext context) {
        if (context.getSqlNode().getKind() == SqlKind.ROLLBACK) {
            return Future.succeededFuture(EdmlAction.ROLLBACK);
        } else {
            return getDestinationAndSourceEntities(context)
                    .map(entities -> validateAndDefineType(entities, context));
        }
    }

    private Future<List<Entity>> getDestinationAndSourceEntities(EdmlRequestContext context) {
        val tableAndSnapshots = new SqlSelectTree(context.getSqlNode()).findAllTableAndSnapshots();
        val defaultDatamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val tableInfos = tableAndSnapshots.stream()
                .map(n -> new TableInfo(n.tryGetSchemaName().orElse(defaultDatamartMnemonic),
                        n.tryGetTableName().orElseThrow(() -> getCantGetTableNameError(context))))
                .collect(Collectors.toList());
        val destinationTable = tableInfos.get(0);
        val sourceTable = tableInfos.get(1);
        if (!destinationTable.getSchemaName().equals(sourceTable.getSchemaName())) {
            return Future.failedFuture(new DtmException(String.format("Unsupported operation for tables in different datamarts: [%s] and [%s]",
                    destinationTable.getSchemaName(), sourceTable.getSchemaName())));
        }
        return CompositeFuture.join(
                        entityDao.getEntity(destinationTable.getSchemaName(), destinationTable.getTableName()),
                        entityDao.getEntity(sourceTable.getSchemaName(), sourceTable.getTableName()))
                .map(CompositeFuture::list);
    }

    private RuntimeException getCantGetTableNameError(EdmlRequestContext context) {
        val sql = context.getRequest().getQueryRequest().getSql();
        return new DtmException(String.format("Can't determine table from query [%s]", sql));
    }

    private EdmlAction validateAndDefineType(List<Entity> entities, EdmlRequestContext context) {
        val destination = entities.get(0);
        val source = entities.get(1);
        context.setDestinationEntity(destination);
        context.setSourceEntity(source);
        if (EntityType.MATERIALIZED_VIEW == destination.getEntityType()) {
            throw new DtmException("MPPW operation doesn't support materialized views");
        }

        if (destination.getEntityType() == EntityType.DOWNLOAD_EXTERNAL_TABLE) {
            if (!DOWNLOAD_SOURCES.contains(source.getEntityType())) {
                throw new DtmException(String.format("DOWNLOAD_EXTERNAL_TABLE source entity type mismatch. %s found, but %s expected.",
                        source.getEntityType(), DOWNLOAD_SOURCES));
            }

            return EdmlAction.DOWNLOAD;
        } else if (source.getEntityType() == EntityType.UPLOAD_EXTERNAL_TABLE) {
            if (destination.getEntityType() == EntityType.TABLE) {
                return EdmlAction.UPLOAD_LOGICAL;
            } else if (destination.getEntityType() == EntityType.WRITEABLE_EXTERNAL_TABLE) {
                return EdmlAction.UPLOAD_STANDALONE;
            }

            throw new DtmException(String.format("UPLOAD_EXTERNAL_TABLE destination entity type mismatch. %s found, but [%s,%s] expected.",
                    destination.getEntityType(), EntityType.TABLE, EntityType.WRITEABLE_EXTERNAL_TABLE));
        }

        throw new DtmException(String.format("Can't determine external table from query [%s]",
                context.getSqlNode().toSqlString(SQL_DIALECT).toString()));
    }

    private Future<QueryResult> executeInternal(EdmlRequestContext context, EdmlAction edmlAction) {
        return executors.get(edmlAction).execute(context);
    }

    @Override
    public SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.EDML;
    }
}
