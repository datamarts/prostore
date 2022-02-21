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
package ru.datamart.prostore.query.execution.core.delta.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaRecord;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.operation.WriteOpFinish;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.dto.query.RollbackDeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaAlreadyIsRollingBackException;
import ru.datamart.prostore.query.execution.core.delta.factory.DeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.factory.impl.CommitDeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.impl.DeltaServiceDaoImpl;
import ru.datamart.prostore.query.execution.core.delta.utils.DeltaQueryUtil;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.UploadFailedExecutor;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;
import ru.datamart.prostore.query.execution.core.utils.QueryResultUtils;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
class RollbackDeltaServiceTest {
    private static final String DATAMART = "test_datamart";
    private static final QueryRequest QUERY_REQUEST = QueryRequest.builder()
            .datamartMnemonic(DATAMART)
            .requestId(UUID.randomUUID())
            .sql("ROLLBACK DELTA")
            .build();
    private static final DeltaRecord DELTA_RECORD = DeltaRecord.builder()
            .datamart(DATAMART)
            .build();
    private static final String ENTITY_NAME = "test_entity";
    private static final Entity ENTITY = Entity.builder()
            .name(ENTITY_NAME)
            .schema(DATAMART)
            .build();
    private static final String DELTA_DATE_STR = "2020-06-16 14:00:11";
    private static final LocalDateTime DELTA_DATE = LocalDateTime.parse(DELTA_DATE_STR, DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
    private static final DeltaQuery DELTA_QUERY = RollbackDeltaQuery.builder()
            .request(QUERY_REQUEST)
            .datamart(DATAMART)
            .build();
    private static final QueryResult QUERY_RESULT = QueryResult.builder()
            .requestId(QUERY_REQUEST.getRequestId())
            .result(createResult())
            .build();
    private static final HotDelta DELTA_HOT = getHotDelta();
    private static final String ERROR = "error";

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(CommitDeltaQueryResultFactory.class);
    private final UploadFailedExecutor uploadFailedExecutor = mock(UploadFailedExecutor.class);
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService =
            mock(EvictQueryTemplateCacheService.class);
    private final RestoreStateService restoreStateService = mock(RestoreStateService.class);
    private final BreakMppwService breakMppwService = mock(BreakMppwService.class);
    private RollbackDeltaService rollbackDeltaService;

    @BeforeEach
    void beforeAll() {
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        rollbackDeltaService = new RollbackDeltaService(uploadFailedExecutor, serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx(), evictQueryTemplateCacheService, restoreStateService, breakMppwService);
    }

    @Test
    void executeShouldSuccess(VertxTestContext testContext) {
        //arrange
        when(restoreStateService.restoreErase(DATAMART)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(breakMppwService.breakMppw(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.writeDeltaError(eq(DATAMART), nullable(Long.class)))
                .thenReturn(Future.succeededFuture());

        when(deltaServiceDao.getDeltaHot(DATAMART))
                .thenReturn(Future.succeededFuture(DELTA_HOT));
        when(entityDao.getEntity(eq(DATAMART), any())).thenReturn(Future.succeededFuture(ENTITY));
        when(uploadFailedExecutor.eraseWriteOp(any())).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteDeltaHot(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaQueryResultFactory.create(any())).thenReturn(QUERY_RESULT);

        //act
        rollbackDeltaService.execute(DELTA_QUERY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(DELTA_DATE, ar.result().getResult().get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
                    verifyEvictCacheExecuted();
                    verify(restoreStateService).restoreErase(DATAMART);
                    verify(breakMppwService).breakMppw(DATAMART);
                }).completeNow());
    }

    @Test
    void executeShouldFailedWhenCreateResultFailed(VertxTestContext testContext) {
        //arrange
        when(restoreStateService.restoreErase(DATAMART)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(breakMppwService.breakMppw(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.writeDeltaError(eq(DATAMART), nullable(Long.class)))
                .thenReturn(Future.succeededFuture());

        when(deltaServiceDao.getDeltaHot(DATAMART))
                .thenReturn(Future.succeededFuture(DELTA_HOT));
        when(entityDao.getEntity(eq(DATAMART), any())).thenReturn(Future.succeededFuture(ENTITY));
        when(uploadFailedExecutor.eraseWriteOp(any())).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteDeltaHot(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaQueryResultFactory.create(any())).thenThrow(new DtmException("create result error"));

        //act
        rollbackDeltaService.execute(DELTA_QUERY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Can't publish result of delta rollback by datamart"));
                }).completeNow());
    }

    @Test
    void executeShouldFailedWhenDeleteDeltaHotFailed(VertxTestContext testContext) {
        //arrange
        when(restoreStateService.restoreErase(DATAMART)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(breakMppwService.breakMppw(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.writeDeltaError(eq(DATAMART), nullable(Long.class)))
                .thenReturn(Future.succeededFuture());

        when(deltaServiceDao.getDeltaHot(DATAMART))
                .thenReturn(Future.succeededFuture(DELTA_HOT));
        when(entityDao.getEntity(eq(DATAMART), any())).thenReturn(Future.succeededFuture(ENTITY));
        when(uploadFailedExecutor.eraseWriteOp(any())).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteDeltaHot(DATAMART)).thenReturn(Future.failedFuture(new DtmException("delete delta hot error")));

        //act
        rollbackDeltaService.execute(DELTA_QUERY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Can't rollback delta by datamart"));
                }).completeNow());
    }

    @Test
    void executeShouldFailedWhenEraseWriteOpFailed(VertxTestContext testContext) {
        //arrange
        when(restoreStateService.restoreErase(DATAMART)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(breakMppwService.breakMppw(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.writeDeltaError(eq(DATAMART), nullable(Long.class)))
                .thenReturn(Future.succeededFuture());

        when(deltaServiceDao.getDeltaHot(DATAMART))
                .thenReturn(Future.succeededFuture(DELTA_HOT));
        when(entityDao.getEntity(eq(DATAMART), any())).thenReturn(Future.succeededFuture(ENTITY));
        when(uploadFailedExecutor.eraseWriteOp(any())).thenReturn(Future.failedFuture(new DtmException("erase write op error")));

        //act
        rollbackDeltaService.execute(DELTA_QUERY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Can't rollback delta by datamart"));
                }).completeNow());
    }

    @Test
    void executeShouldFailedWhenGetEntityFailed(VertxTestContext testContext) {
        //arrange
        when(restoreStateService.restoreErase(DATAMART)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(breakMppwService.breakMppw(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.writeDeltaError(eq(DATAMART), nullable(Long.class)))
                .thenReturn(Future.succeededFuture());

        when(deltaServiceDao.getDeltaHot(DATAMART))
                .thenReturn(Future.succeededFuture(DELTA_HOT));
        when(entityDao.getEntity(eq(DATAMART), any())).thenReturn(Future.failedFuture(new DtmException("get entity error")));

        //act
        rollbackDeltaService.execute(DELTA_QUERY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Can't rollback delta by datamart"));
                }).completeNow());
    }

    @Test
    void executeShouldFailedWhenEvictCacheFailed(VertxTestContext testContext) {
        //arrange
        when(restoreStateService.restoreErase(DATAMART)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(breakMppwService.breakMppw(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.writeDeltaError(eq(DATAMART), nullable(Long.class)))
                .thenReturn(Future.succeededFuture());

        when(deltaServiceDao.getDeltaHot(DATAMART))
                .thenReturn(Future.succeededFuture(DELTA_HOT));
        doThrow(new DtmException("evict cache error")).when(evictQueryTemplateCacheService).evictByDatamartName(DATAMART);

        //act
        rollbackDeltaService.execute(DELTA_QUERY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Can't rollback delta by datamart"));
                }).completeNow());
    }

    @Test
    void executeShouldFailedWhenGetDeltaHotFailed(VertxTestContext testContext) {
        //arrange
        when(restoreStateService.restoreErase(DATAMART)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(breakMppwService.breakMppw(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.writeDeltaError(eq(DATAMART), nullable(Long.class)))
                .thenReturn(Future.succeededFuture());

        when(deltaServiceDao.getDeltaHot(DATAMART))
                .thenReturn(Future.failedFuture(new DtmException("get delta hot error")));

        //act
        rollbackDeltaService.execute(DELTA_QUERY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Can't rollback delta by datamart"));
                }).completeNow());
    }

    @Test
    void executeShouldFailedWhenWriteDeltaErrorFailed(VertxTestContext testContext) {
        //arrange
        when(restoreStateService.restoreErase(DATAMART)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(breakMppwService.breakMppw(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.writeDeltaError(eq(DATAMART), nullable(Long.class)))
                .thenReturn(Future.failedFuture(new DtmException("write delta error error")));

        //act
        rollbackDeltaService.execute(DELTA_QUERY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Can't rollback delta by datamart"));
                }).completeNow());
    }

    @Test
    void executeShouldSuccessWhenWriteDeltaErrorDeltaAlreadyIsRollingBackException(VertxTestContext testContext) {
        //arrange
        when(restoreStateService.restoreErase(DATAMART)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(breakMppwService.breakMppw(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.writeDeltaError(eq(DATAMART), nullable(Long.class)))
                .thenReturn(Future.failedFuture(new DeltaAlreadyIsRollingBackException("error")));

        when(deltaServiceDao.getDeltaHot(DATAMART))
                .thenReturn(Future.succeededFuture(DELTA_HOT));
        when(entityDao.getEntity(eq(DATAMART), any())).thenReturn(Future.succeededFuture(ENTITY));
        when(uploadFailedExecutor.eraseWriteOp(any())).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteDeltaHot(DATAMART)).thenReturn(Future.succeededFuture());
        when(deltaQueryResultFactory.create(any())).thenReturn(QUERY_RESULT);

        //act
        rollbackDeltaService.execute(DELTA_QUERY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(DELTA_DATE, ar.result().getResult().get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
                    verifyEvictCacheExecuted();
                    verify(restoreStateService).restoreErase(DATAMART);
                    verify(breakMppwService).breakMppw(DATAMART);
                }).completeNow());
    }

    @Test
    void executeShouldFailedWhenBreakMppwFailed(VertxTestContext testContext) {
        //arrange
        when(restoreStateService.restoreErase(DATAMART)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(breakMppwService.breakMppw(DATAMART))
                .thenReturn(Future.failedFuture(new DtmException("break mppw error")));

        //act
        rollbackDeltaService.execute(DELTA_QUERY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("break mppw error"));
                }).completeNow());
    }

    @Test
    void executeShouldFailedWhenRestoreEraseFailed(VertxTestContext testContext) {
        //arrange
        when(restoreStateService.restoreErase(DATAMART))
                .thenReturn(Future.failedFuture(new DtmException("restore erase error")));

        //act
        rollbackDeltaService.execute(DELTA_QUERY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("restore erase error"));
                }).completeNow());
    }

    private static HotDelta getHotDelta() {
        val writeOp = new WriteOpFinish();
        writeOp.setTableName(ENTITY_NAME);
        writeOp.setCnList(Arrays.asList(1L ,2L, 3L));
        return HotDelta.builder()
                .deltaNum(1)
                .cnFrom(1)
                .cnTo(3L)
                .writeOperationsFinished(Collections.singletonList(writeOp))
                .build();
    }

    private static List<Map<String, Object>> createResult() {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.DATE_TIME_FIELD),
                Collections.singletonList(DELTA_DATE));
    }

    private void verifyEvictCacheExecuted() {
        verify(evictQueryTemplateCacheService, times(1)).evictByDatamartName(DATAMART);
    }
}
