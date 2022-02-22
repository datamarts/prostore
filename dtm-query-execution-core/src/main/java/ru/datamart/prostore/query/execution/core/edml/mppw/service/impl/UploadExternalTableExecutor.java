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
package ru.datamart.prostore.query.execution.core.edml.mppw.service.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOpRequest;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlAction;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.EdmlUploadExecutor;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.UploadFailedExecutor;
import ru.datamart.prostore.query.execution.core.edml.service.EdmlExecutor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.core.edml.dto.EdmlAction.UPLOAD;

@Service
@Slf4j
public class UploadExternalTableExecutor implements EdmlExecutor {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final DeltaServiceDao deltaServiceDao;
    private final Map<ExternalTableLocationType, EdmlUploadExecutor> executors;
    private final UploadFailedExecutor uploadFailedExecutor;
    private final DataSourcePluginService pluginService;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public UploadExternalTableExecutor(DeltaServiceDao deltaServiceDao,
                                       UploadFailedExecutor uploadFailedExecutor,
                                       List<EdmlUploadExecutor> uploadExecutors,
                                       DataSourcePluginService pluginService,
                                       LogicalSchemaProvider logicalSchemaProvider,
                                       EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.deltaServiceDao = deltaServiceDao;
        this.uploadFailedExecutor = uploadFailedExecutor;
        this.executors = uploadExecutors.stream()
                .collect(Collectors.toMap(EdmlUploadExecutor::getUploadType, it -> it));
        this.pluginService = pluginService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<QueryResult> execute(EdmlRequestContext context) {
        return Future.future(promise -> isEntitySourceTypesExistsInConfiguration(context.getDestinationEntity())
                .compose(v -> {
                    try {
                        evictQueryTemplateCacheService.evictByDatamartName(context.getDestinationEntity().getSchema());
                        return Future.succeededFuture(v);
                    } catch (Exception e) {
                        return Future.failedFuture(new DtmException("Evict cache error"));
                    }
                })
                .compose(v -> writeNewOperationIfNeeded(context))
                .compose(v -> executeAndWriteOp(context))
                .compose(queryResult -> writeOpSuccess(context.getDestinationEntity().getSchema(),
                        context.getSysCn(),
                        queryResult))
                .compose(v -> {
                    try {
                        evictQueryTemplateCacheService.evictByDatamartName(context.getDestinationEntity().getSchema());
                        return Future.succeededFuture(v);
                    } catch (Exception e) {
                        return Future.failedFuture(new DtmException("Evict cache error"));
                    }
                })
                .onComplete(f -> {
                    if (context.getSysCn() != null) {
                        BreakMppwContext.removeTask(
                                context.getDestinationEntity().getSchema(),
                                context.getSysCn());
                    }
                })
                .onSuccess(result -> promise.complete(QueryResult.emptyResult()))
                .onFailure(promise::fail));
    }

    private Future<Void> isEntitySourceTypesExistsInConfiguration(Entity destination) {
        final Set<SourceType> nonExistDestionationTypes = destination.getDestination().stream()
                .filter(type -> !pluginService.hasSourceType(type))
                .collect(Collectors.toSet());
        if (!nonExistDestionationTypes.isEmpty()) {
            final String failureMessage = String.format("Plugins: %s for the table [%s] datamart [%s] are not configured",
                    nonExistDestionationTypes,
                    destination.getName(),
                    destination.getSchema());
            return Future.failedFuture(new DtmException(failureMessage));
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Long> writeNewOperationIfNeeded(EdmlRequestContext context) {
        if (context.getSysCn() != null) {
            return Future.succeededFuture();
        } else {
            return Future.future(writePromise -> deltaServiceDao.writeNewOperation(createDeltaOp(context))
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            long sysCn = ar.result();
                            context.setSysCn(sysCn);
                            writePromise.complete();
                        } else {
                            writePromise.fail(ar.cause());
                        }
                    }));
        }
    }

    private DeltaWriteOpRequest createDeltaOp(EdmlRequestContext context) {
        return DeltaWriteOpRequest.builder()
                .datamart(context.getDestinationEntity().getSchema())
                .tableName(context.getDestinationEntity().getName())
                .tableNameExt(context.getSourceEntity().getName())
                .query(context.getSqlNode().toSqlString(SQL_DIALECT).toString())
                .build();
    }

    private Future<QueryResult> executeAndWriteOp(EdmlRequestContext context) {
        return Future.future(promise ->
                initLogicalSchema(context)
                        .compose(ctx -> executeInternal(context))
                        .onSuccess(promise::complete)
                        .onFailure(error -> {
                            deltaServiceDao.writeOperationError(context.getDestinationEntity().getSchema(), context.getSysCn())
                                    .compose(v -> uploadFailedExecutor.execute(context))
                                    .onComplete(writeErrorOpAr -> {
                                        if (writeErrorOpAr.failed()) {
                                            log.error("Failed writing operation error", writeErrorOpAr.cause());
                                        }
                                        promise.fail(error);
                                    });
                        }));
    }

    private Future<QueryResult> executeInternal(EdmlRequestContext context) {
        return Future.future((Promise<QueryResult> promise) -> {
            if (ExternalTableLocationType.KAFKA == context.getSourceEntity().getExternalTableLocationType()) {
                executors.get(context.getSourceEntity().getExternalTableLocationType())
                        .execute(context)
                        .onComplete(promise);
            } else {
                promise.fail(new DtmException("Other download types are not yet implemented"));
            }
        });
    }

    private Future<QueryResult> writeOpSuccess(String datamartName, Long sysCn, QueryResult result) {
        return Future.future(promise ->
                deltaServiceDao.writeOperationSuccess(datamartName, sysCn)
                        .onSuccess(v -> promise.complete(result))
                        .onFailure(promise::fail));
    }

    private Future<Void> initLogicalSchema(EdmlRequestContext context) {
        return Future.future(promise -> {
            String datamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
            logicalSchemaProvider.getSchemaFromQuery(context.getSqlNode(), datamartMnemonic)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            final List<Datamart> logicalSchema = ar.result();
                            context.setLogicalSchema(logicalSchema);
                            promise.complete();
                        } else {
                            promise.fail(ar.cause());
                        }
                    });
        });
    }

    @Override
    public EdmlAction getAction() {
        return UPLOAD;
    }
}
