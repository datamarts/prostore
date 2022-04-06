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
package ru.datamart.prostore.query.execution.core.rollback.service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.RequestStatus;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.core.base.configuration.properties.RestorationProperties;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.dto.EraseWriteOpResult;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.UploadFailedExecutor;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.impl.UploadLogicalExternalTableExecutor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class RestoreStateService {

    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final DeltaServiceDao deltaServiceDao;
    private final UploadFailedExecutor uploadFailedExecutor;
    private final UploadLogicalExternalTableExecutor uploadLogicalExternalTableExecutor;
    private final DefinitionService<SqlNode> definitionService;
    private final String envName;

    @Getter
    private final boolean isAutoRestoreState;

    @Autowired
    public RestoreStateService(ServiceDbFacade serviceDbFacade,
                               UploadFailedExecutor uploadFailedExecutor,
                               UploadLogicalExternalTableExecutor uploadLogicalExternalTableExecutor,
                               @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                               @Value("${core.env.name}") String envName,
                               RestorationProperties restorationProperties) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.uploadFailedExecutor = uploadFailedExecutor;
        this.uploadLogicalExternalTableExecutor = uploadLogicalExternalTableExecutor;
        this.definitionService = definitionService;
        this.envName = envName;
        this.isAutoRestoreState = restorationProperties.isAutoRestoreState();
    }

    public Future<Void> restoreState() {
        return datamartDao.getDatamarts()
                .compose(this::restoreDatamarts)
                .onSuccess(success -> log.info("State successfully restored"))
                .onFailure(err -> log.error("Error while trying to restore state", err));
    }

    public Future<List<EraseWriteOpResult>> restoreErase(String datamart) {
        return restoreErase(datamart, null);
    }

    public Future<List<EraseWriteOpResult>> restoreErase(String datamart, Long sysCn) {
        return deltaServiceDao.getDeltaWriteOperations(datamart)
                .map(ops -> ops.stream()
                        .filter(op -> op.getStatus() == WriteOperationStatus.ERROR.getValue())
                        .filter(op -> sysCn == null || op.getSysCn().equals(sysCn))
                        .collect(Collectors.toList()))
                .compose(failedOps -> eraseOperations(datamart, failedOps));
    }

    public Future<List<QueryResult>> restoreUpload(String datamart) {
        return restoreUpload(datamart, null);
    }

    public Future<List<QueryResult>> restoreUpload(String datamart, Long sysCn) {
        return deltaServiceDao.getDeltaWriteOperations(datamart)
                .map(ops -> ops.stream()
                        .filter(op -> op.getStatus() == WriteOperationStatus.EXECUTING.getValue() && op.getTableNameExt() != null)
                        .filter(op -> sysCn == null || op.getSysCn().equals(sysCn))
                        .collect(Collectors.toList()))
                .compose(runningOps -> uploadOperations(datamart, runningOps));
    }

    private Future<Void> restoreDatamarts(List<String> datamarts) {
        return Future.future(p -> CompositeFuture.join(Stream.concat(datamarts.stream().map(this::restoreErase),
                datamarts.stream().map(this::restoreUpload))
                .collect(Collectors.toList()))
                .onSuccess(success -> p.complete())
                .onFailure(p::fail));
    }

    private Future<List<EraseWriteOpResult>> eraseOperations(String datamart, List<DeltaWriteOp> ops) {
        return Future.future(p -> {
            CompositeFuture.join(ops.stream()
                    .map(op -> entityDao.getEntity(datamart, op.getTableName())
                            .compose(entity -> eraseWriteOperation(entity, op)))
                    .collect(Collectors.toList()))
                    .onSuccess(success -> {
                        List<Optional<EraseWriteOpResult>> erOptList = success.list();
                        p.complete(erOptList.stream().filter(Optional::isPresent)
                                .map(Optional::get).collect(Collectors.toList()));
                    })
                    .onFailure(p::fail);
        });
    }

    private Future<List<QueryResult>> uploadOperations(String datamart, List<DeltaWriteOp> ops) {
        return Future.future(p -> {
            CompositeFuture.join(ops.stream()
                    .map(op -> getDestinationSourceEntities(datamart, op.getTableName(), op.getTableNameExt())
                            .compose(entities -> uploadWriteOperation(entities.get(0), entities.get(1), op)))
                    .collect(Collectors.toList()))
                    .onSuccess(success -> p.complete(success.list()))
                    .onFailure(p::fail);
        });
    }

    private Future<List<Entity>> getDestinationSourceEntities(String datamart, String dest, String source) {
        return Future.future(p -> CompositeFuture.join(entityDao.getEntity(datamart, dest), entityDao.getEntity(datamart, source))
                .onSuccess(res -> p.complete(res.list()))
                .onFailure(p::fail));
    }

    private Future<Optional<EraseWriteOpResult>> eraseWriteOperation(Entity dest, DeltaWriteOp op) {
        EdmlRequestContext context = createDeleteContext(dest, op);
        return uploadFailedExecutor.execute(context)
                .map(v -> Optional.of(new EraseWriteOpResult(dest.getName(), op.getSysCn())));

    }

    private Future<QueryResult> uploadWriteOperation(Entity dest, Entity source, DeltaWriteOp op) {
        EdmlRequestContext context = createUploadContext(dest, source, op);
        return uploadLogicalExternalTableExecutor.execute(context);
    }

    private EdmlRequestContext createUploadContext(Entity dest, Entity source, DeltaWriteOp op) {
        val queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSql(op.getQuery());
        //if will need in future, use method extract() of DatamartMnemonicExtractor
        queryRequest.setDatamartMnemonic(dest.getSchema());
        val datamartRequest = new DatamartRequest(queryRequest);
        val sqlNode = definitionService.processingQuery(op.getQuery());
        val context = new EdmlRequestContext(
                RequestMetrics.builder()
                        .startTime(LocalDateTime.now(CoreConstants.CORE_ZONE_ID))
                        .requestId(queryRequest.getRequestId())
                        .status(RequestStatus.IN_PROCESS)
                        .isActive(true)
                        .build(),
                datamartRequest,
                sqlNode,
                envName);
        context.setSysCn(op.getSysCn());
        context.setSourceEntity(source);
        context.setDestinationEntity(dest);
        return context;
    }

    private EdmlRequestContext createDeleteContext(Entity dest, DeltaWriteOp op) {
        val queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSql(op.getQuery());
        queryRequest.setDatamartMnemonic(dest.getSchema());
        val datamartRequest = new DatamartRequest(queryRequest);
        val context = new EdmlRequestContext(
                RequestMetrics.builder()
                        .startTime(LocalDateTime.now(CoreConstants.CORE_ZONE_ID))
                        .requestId(queryRequest.getRequestId())
                        .status(RequestStatus.IN_PROCESS)
                        .isActive(true)
                        .build(),
                datamartRequest,
                null,
                envName);
        context.setSysCn(op.getSysCn());
        context.setDestinationEntity(dest);
        return context;
    }
}
