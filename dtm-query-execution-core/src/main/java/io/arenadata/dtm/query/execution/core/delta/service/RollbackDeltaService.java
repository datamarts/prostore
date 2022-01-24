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
package io.arenadata.dtm.query.execution.core.delta.service;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaRecord;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.dto.operation.WriteOpFinish;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.dto.query.RollbackDeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaAlreadyIsRollingBackException;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.UploadFailedExecutor;
import io.arenadata.dtm.query.execution.core.rollback.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.core.rollback.dto.RollbackRequestContext;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

import static io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaAction.ROLLBACK_DELTA;

@Component
@Slf4j
public class RollbackDeltaService implements DeltaService, StatusEventPublisher {

    private final UploadFailedExecutor uploadFailedExecutor;
    private final DeltaQueryResultFactory deltaQueryResultFactory;
    private final DeltaServiceDao deltaServiceDao;
    private final Vertx vertx;
    private final EntityDao entityDao;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;
    private final RestoreStateService restoreStateService;
    private final BreakMppwService breakMppwService;
    private final BreakLlwService breakLlwService;

    @Autowired
    public RollbackDeltaService(UploadFailedExecutor uploadFailedExecutor,
                                ServiceDbFacade serviceDbFacade,
                                @Qualifier("beginDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
                                @Qualifier("coreVertx") Vertx vertx,
                                EvictQueryTemplateCacheService evictQueryTemplateCacheService,
                                RestoreStateService restoreStateService,
                                BreakMppwService breakMppwService,
                                BreakLlwService breakLlwService) {
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.uploadFailedExecutor = uploadFailedExecutor;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
        this.vertx = vertx;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
        this.restoreStateService = restoreStateService;
        this.breakMppwService = breakMppwService;
        this.breakLlwService = breakLlwService;
    }

    @Override
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return restoreStateService.restoreErase(deltaQuery.getDatamart())
                .compose(ar -> breakMppwService.breakMppw(deltaQuery.getDatamart()))
                .compose(ar -> rollbackDelta(deltaQuery));
    }

    private Future<QueryResult> rollbackDelta(DeltaQuery deltaQuery) {
        return Future.future(promise ->
                deltaServiceDao.writeDeltaError(deltaQuery.getDatamart(), null)
                        .otherwise(this::skipDeltaAlreadyIsRollingBackError)
                        .compose(v -> deltaServiceDao.getDeltaHot(deltaQuery.getDatamart()))
                        .compose(hotDelta -> {
                            try {
                                evictQueryTemplateCacheService.evictByDatamartName(deltaQuery.getDatamart());
                            } catch (Exception e) {
                                return Future.failedFuture(new DtmException("Evict cache error"));
                            }
                            return Future.succeededFuture(hotDelta);
                        })
                        .compose(hotDelta -> rollbackTables((RollbackDeltaQuery) deltaQuery, hotDelta)
                                .map(v -> hotDelta))
                        .compose(hotDelta -> deltaServiceDao.deleteDeltaHot(deltaQuery.getDatamart())
                                .map(hotDelta.getDeltaNum()))
                        .onSuccess(deltaNum -> {
                            try {
                                publishStatus(StatusEventCode.DELTA_CANCEL, deltaQuery.getDatamart(), deltaNum);
                                val res = deltaQueryResultFactory.create(getDeltaRecord(deltaQuery.getDatamart(),
                                        deltaNum));
                                promise.complete(res);
                            } catch (Exception e) {
                                promise.fail(new DtmException(String.format("Can't publish result of delta rollback by datamart [%s]",
                                        deltaQuery.getDatamart()), e));
                            }
                        })
                        .onFailure(error -> promise.fail(new DtmException(String.format("Can't rollback delta by datamart [%s]",
                                deltaQuery.getDatamart()), error))));
    }

    @SneakyThrows
    private Void skipDeltaAlreadyIsRollingBackError(Throwable error) {
        if (error instanceof DeltaAlreadyIsRollingBackException) {
            return null;
        } else {
            throw error;
        }
    }

    private Future<Void> rollbackTables(RollbackDeltaQuery deltaQuery,
                                        HotDelta hotDelta) {
        val operationsFinished = hotDelta.getWriteOperationsFinished();
        return operationsFinished != null ?
                getRollbackTablesFuture(deltaQuery, operationsFinished) : Future.succeededFuture();
    }

    private Future<Void> getRollbackTablesFuture(RollbackDeltaQuery deltaQuery,
                                                 List<WriteOpFinish> operationsFinished) {
        List<Future> futures = new ArrayList<>(operationsFinished.size());
        for (WriteOpFinish writeOpFinish : operationsFinished) {
            futures.add(rollbackTable(deltaQuery, writeOpFinish));
        }
        return CompositeFuture.join(futures).mapEmpty();
    }

    private Future<Void> rollbackTable(RollbackDeltaQuery deltaQuery, WriteOpFinish writeOpFinish) {
        return entityDao.getEntity(deltaQuery.getDatamart(), writeOpFinish.getTableName())
                .compose(entity -> rollbackTableWriteOperations(deltaQuery, writeOpFinish, entity));
    }

    private Future<Void> rollbackTableWriteOperations(RollbackDeltaQuery deltaQuery,
                                                      WriteOpFinish writeOpFinish,
                                                      Entity entity) {
        List<Future> futures = new ArrayList<>();

        writeOpFinish.getCnList().stream()
                .map(sysCn -> RollbackRequest.builder()
                        .destinationTable(entity.getName())
                        .queryRequest(deltaQuery.getRequest())
                        .datamart(deltaQuery.getDatamart())
                        .entity(entity)
                        .sysCn(sysCn)
                        .build())
                .map(rollbackRequest -> new RollbackRequestContext(deltaQuery.getRequestMetrics(),
                        deltaQuery.getEnvName(),
                        rollbackRequest,
                        deltaQuery.getSqlNode()
                ))
                .forEach(rollbackRequestContext -> futures.add(uploadFailedExecutor.eraseWriteOp(rollbackRequestContext)));

        return CompositeFuture.join(futures).mapEmpty();
    }

    private DeltaRecord getDeltaRecord(String datamart, long deltaNum) {
        return DeltaRecord.builder()
                .datamart(datamart)
                .deltaNum(deltaNum)
                .build();
    }

    @Override
    public DeltaAction getAction() {
        return ROLLBACK_DELTA;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}
