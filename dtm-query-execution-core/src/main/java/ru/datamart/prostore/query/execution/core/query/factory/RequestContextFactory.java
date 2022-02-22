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
package ru.datamart.prostore.query.execution.core.query.factory;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.*;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.dto.TableInfo;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.RequestStatus;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckCall;
import ru.datamart.prostore.query.calcite.core.extension.config.SqlConfigCall;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlChanges;
import ru.datamart.prostore.query.calcite.core.extension.ddl.truncate.SqlBaseTruncate;
import ru.datamart.prostore.query.calcite.core.extension.delta.SqlDeltaCall;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlDataSourceTypeGetter;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.execution.core.base.configuration.AppConfiguration;
import ru.datamart.prostore.query.execution.core.base.dto.request.CoreRequestContext;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.config.dto.ConfigRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.delta.dto.operation.DeltaRequestContext;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequest;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.eddl.dto.EddlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.plugin.api.request.ConfigRequest;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class RequestContextFactory {
    private final AppConfiguration coreConfiguration;
    private final EntityDao entityDao;

    public RequestContextFactory(AppConfiguration coreConfiguration,
                                 EntityDao entityDao) {
        this.coreConfiguration = coreConfiguration;
        this.entityDao = entityDao;
    }

    public Future<CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>> create(QueryRequest request,
                                                                                           SqlNode node) {
        return Future.future(promise -> {
            val envName = coreConfiguration.getEnvName();
            if (isConfigRequest(node)) {
                promise.complete(
                        ConfigRequestContext.builder()
                                .request(new ConfigRequest(request))
                                .envName(envName)
                                .metrics(createRequestMetrics(request))
                                .sqlConfigCall((SqlConfigCall) node)
                                .build()
                );
                return;
            }

            if (isDdlRequest(node)) {
                switch (node.getKind()) {
                    case OTHER_DDL:
                        if (node instanceof SqlBaseTruncate || node instanceof SqlChanges) {
                            promise.complete(
                                    new DdlRequestContext(
                                            createRequestMetrics(request),
                                            new DatamartRequest(request),
                                            node,
                                            null,
                                            envName)
                            );
                            return;
                        }

                        promise.complete(
                                EddlRequestContext.builder()
                                        .request(new DatamartRequest(request))
                                        .envName(envName)
                                        .metrics(createRequestMetrics(request))
                                        .sqlNode(node)
                                        .build()
                        );
                        return;
                    default:
                        promise.complete(
                                new DdlRequestContext(
                                        createRequestMetrics(request),
                                        new DatamartRequest(request),
                                        node,
                                        null,
                                        envName)
                        );
                        return;
                }
            }

            if (node instanceof SqlDeltaCall) {
                promise.complete(
                        new DeltaRequestContext(
                                createRequestMetrics(request),
                                new DatamartRequest(request),
                                envName,
                                (SqlDeltaCall) node)
                );
                return;
            }

            if (SqlKind.CHECK.equals(node.getKind())) {
                SqlCheckCall sqlCheckCall = (SqlCheckCall) node;
                Optional.ofNullable(sqlCheckCall.getSchema()).ifPresent(request::setDatamartMnemonic);
                promise.complete(
                        CheckContext.builder()
                                .request(new DatamartRequest(request))
                                .envName(envName)
                                .metrics(createRequestMetrics(request))
                                .checkType(sqlCheckCall.getType())
                                .sqlCheckCall(sqlCheckCall)
                                .build()
                );
                return;
            }


            determineEdmlOperation(node, request)
                    .map(isEdml -> {
                        if (isEdml) {
                            return new EdmlRequestContext(
                                    createRequestMetrics(request),
                                    new DatamartRequest(request),
                                    node,
                                    envName);
                        }

                        return DmlRequestContext.builder()
                                .request(new DmlRequest(request))
                                .envName(envName)
                                .metrics(createRequestMetrics(request))
                                .sourceType(getDmlSourceType(node))
                                .sqlNode(node)
                                .build();
                    })
                    .onSuccess(promise::complete)
                    .onFailure(promise::fail);
        });
    }

    private Future<Boolean> determineEdmlOperation(SqlNode node, QueryRequest request) {
        if (node.getKind() == SqlKind.ROLLBACK) {
            return Future.succeededFuture(Boolean.TRUE);
        }

        if (node.getKind() != SqlKind.INSERT) {
            return Future.succeededFuture(Boolean.FALSE);
        }

        return hasAnyEdmlEntity((SqlInsert) node, request);
    }

    private SourceType getDmlSourceType(SqlNode node) {
        if (node instanceof SqlDataSourceTypeGetter) {
            return ((SqlDataSourceTypeGetter) node).getDatasourceType().getValue();
        }

        return null;
    }

    private RequestMetrics createRequestMetrics(QueryRequest request) {
        return RequestMetrics.builder()
                .startTime(LocalDateTime.now(CoreConstants.CORE_ZONE_ID))
                .requestId(request.getRequestId())
                .status(RequestStatus.IN_PROCESS)
                .isActive(true)
                .build();
    }

    private boolean isConfigRequest(SqlNode node) {
        return node instanceof SqlConfigCall;
    }

    private boolean isDdlRequest(SqlNode node) {
        return node instanceof SqlDdl ||
                node instanceof SqlAlter ||
                node instanceof SqlBaseTruncate ||
                node instanceof SqlChanges;
    }

    protected Future<Boolean> hasAnyEdmlEntity(SqlInsert sqlInsert, QueryRequest queryRequest) {
        val defaultDatamartMnemonic = queryRequest.getDatamartMnemonic();
        val tableAndSnapshots = new SqlSelectTree(sqlInsert).findAllTableAndSnapshots();
        List<Future> entityFutures = tableAndSnapshots
                .stream()
                .map(item -> new TableInfo(item.tryGetSchemaName().orElse(defaultDatamartMnemonic),
                        item.tryGetTableName().orElseThrow(() -> cantDetermineTableException(queryRequest))))
                .distinct()
                .map(this::getEntityAndCheckType)
                .collect(Collectors.toList());
        return CompositeFuture.join(entityFutures)
                .map(CompositeFuture::<Boolean>list)
                .map(booleans -> booleans.stream().anyMatch(isEdml -> isEdml));
    }

    private Future<Boolean> getEntityAndCheckType(TableInfo tableInfo) {
        return entityDao.getEntity(tableInfo.getSchemaName(), tableInfo.getTableName())
                .map(entity -> entity.getEntityType() == EntityType.DOWNLOAD_EXTERNAL_TABLE || entity.getEntityType() == EntityType.UPLOAD_EXTERNAL_TABLE);
    }

    private DtmException cantDetermineTableException(QueryRequest queryRequest) {
        return new DtmException(String.format("Can't determine table from query [%s]", queryRequest.getSql()));
    }
}
