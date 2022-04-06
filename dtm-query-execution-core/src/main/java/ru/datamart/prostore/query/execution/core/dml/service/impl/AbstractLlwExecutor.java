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

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.codec.digest.DigestUtils;
import ru.datamart.prostore.common.dto.TableInfo;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlRetryable;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicatePart;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicates;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOpRequest;
import ru.datamart.prostore.query.execution.core.delta.exception.TableBlockedException;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.dml.service.DmlExecutor;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractLlwExecutor implements DmlExecutor {

    private static final List<EntityType> ALLOWED_ENTITY_TYPES = Arrays.asList(EntityType.TABLE, EntityType.WRITEABLE_EXTERNAL_TABLE);
    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private static final Pattern INSERT_VALUES_PATTERN = Pattern.compile("(?i)(INSERT|UPSERT)( INTO .+ VALUES )(.+)");
    private static final SqlPredicates DYNAMIC_PARAM_PREDICATE = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eq(SqlKind.DYNAMIC_PARAM))
            .build();

    private final EntityDao entityDao;
    private final DataSourcePluginService pluginService;
    private final DeltaServiceDao deltaServiceDao;
    private final RestoreStateService restoreStateService;

    protected AbstractLlwExecutor(EntityDao entityDao,
                                  DataSourcePluginService pluginService,
                                  DeltaServiceDao deltaServiceDao,
                                  RestoreStateService restoreStateService) {
        this.entityDao = entityDao;
        this.pluginService = pluginService;
        this.deltaServiceDao = deltaServiceDao;
        this.restoreStateService = restoreStateService;
    }

    protected Future<Entity> getDestinationEntity(DmlRequestContext context) {
        String defaultDatamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val tableAndSnapshots = new SqlSelectTree(context.getSqlNode()).findAllTableAndSnapshots();
        val destination = tableAndSnapshots
                .stream()
                .map(item -> new TableInfo(item.tryGetSchemaName().orElse(defaultDatamartMnemonic),
                        item.tryGetTableName().orElseThrow(() ->
                                new DtmException(String.format("Can't determine table from query [%s]", context.getRequest().getQueryRequest().getSql())))))
                .findFirst()
                .orElseThrow(() -> new DtmException(String.format("Can't determine table from query [%s]", context.getRequest().getQueryRequest().getSql())));
        return entityDao.getEntity(destination.getSchemaName(), destination.getTableName());
    }

    protected Future<Entity> validateEntityType(Entity entity) {
        if (!ALLOWED_ENTITY_TYPES.contains(entity.getEntityType())) {
            return Future.failedFuture(new DtmException("Forbidden. Write operations allowed for logical and writeable external tables only."));
        }
        return Future.succeededFuture(entity);
    }

    protected Future<Entity> checkConfiguration(Entity destination) {
        final Set<SourceType> nonExistDestinationTypes = destination.getDestination().stream()
                .filter(type -> !pluginService.hasSourceType(type))
                .collect(Collectors.toSet());
        if (!nonExistDestinationTypes.isEmpty()) {
            final String failureMessage = String.format("Plugins: %s for the table [%s] datamart [%s] are not configured",
                    nonExistDestinationTypes,
                    destination.getName(),
                    destination.getSchema());
            return Future.failedFuture(new DtmException(failureMessage));
        } else {
            return Future.succeededFuture(destination);
        }
    }

    protected Future<Long> produceOrRetryWriteOperation(DmlRequestContext context, Entity entity) {
        return Future.future(p -> {
            val writeOpRequest = createDeltaOp(context, entity);
            deltaServiceDao.writeNewOperation(writeOpRequest)
                    .onSuccess(p::complete)
                    .onFailure(t -> {
                        if (isNodeWithRetry(context.getSqlNode()) && t instanceof TableBlockedException) {
                            deltaServiceDao.getDeltaWriteOperations(context.getRequest().getQueryRequest().getDatamartMnemonic())
                                    .map(writeOps -> findEqualWriteOp(writeOps, entity.getName(), writeOpRequest.getQuery(), t))
                                    .onComplete(p);
                            return;
                        }

                        p.fail(t);
                    });
        });
    }

    private boolean isNodeWithRetry(SqlNode sqlNode) {
        return sqlNode instanceof SqlRetryable && ((SqlRetryable) sqlNode).isRetry();
    }

    private Long findEqualWriteOp(List<DeltaWriteOp> writeOps, String tableName, String query, Throwable originalException) {
        return writeOps.stream()
                .filter(deltaWriteOp -> deltaWriteOp.getStatus() == WriteOperationStatus.EXECUTING.getValue())
                .filter(deltaWriteOp -> Objects.equals(deltaWriteOp.getTableName(), tableName))
                .filter(deltaWriteOp -> Objects.equals(query, deltaWriteOp.getQuery()))
                .findFirst()
                .map(DeltaWriteOp::getSysCn)
                .orElseThrow(() -> new DtmException("Table blocked and could not find equal writeOp for resume", originalException));
    }

    protected DeltaWriteOpRequest createDeltaOp(DmlRequestContext context, Entity entity) {
        return DeltaWriteOpRequest.builder()
                .datamart(entity.getSchema())
                .tableName(entity.getName())
                .query(hashQuery(context))
                .build();
    }

    private String hashQuery(DmlRequestContext context) {
        val parameters = context.getRequest().getQueryRequest().getParameters();
        val sqlNode = parameters != null ? SqlNodeUtil.copy(context.getSqlNode()) : context.getSqlNode();
        if (parameters != null) {
            val dynamicNodes = new SqlSelectTree(sqlNode)
                    .findNodes(DYNAMIC_PARAM_PREDICATE, false);
            for (int i = 0; i < dynamicNodes.size(); i++) {
                val treeNode = dynamicNodes.get(i);
                val value = parameters.getValues().get(i);
                val columnType = parameters.getTypes().get(i);
                treeNode.getSqlNodeSetter().accept(SqlNodeTemplates.literalForParameter(value, columnType));
            }
        }

        val query = sqlNode.toSqlString(SQL_DIALECT).toString()
                .replaceAll("\r\n|\r|\n", " ");

        val matcher = INSERT_VALUES_PATTERN.matcher(query);
        if (matcher.matches()) {
            return matcher.group(1) + matcher.group(2) + DigestUtils.md5Hex(matcher.group(3));
        }

        return query;
    }

    protected Future<Void> handleOperation(Future<?> llwFuture, long sysCn, String datamart, Entity entity) {
        return Future.future(promise -> {
            llwFuture.compose(ignored -> deltaServiceDao.writeOperationSuccess(datamart, sysCn))
                    .onSuccess(ar -> {
                        log.info("LL-W request succeeded [{}], sysCn: {}", entity.getNameWithSchema(), sysCn);
                        promise.complete();
                    })
                    .onFailure(error -> {
                        log.error("LL-W request failed [{}], sysCn: {}. Cleaning up", entity.getNameWithSchema(), sysCn, error);
                        deltaServiceDao.writeOperationError(datamart, sysCn)
                                .compose(ignored -> restoreStateService.restoreErase(datamart))
                                .onComplete(rollbackAr -> {
                                    if (rollbackAr.failed()) {
                                        log.error("Rollback for LL-W [{}] failed, sysCn: {}", entity.getNameWithSchema(), sysCn, rollbackAr.cause());
                                    }
                                    promise.fail(error);
                                });
                    });
        });
    }

}
