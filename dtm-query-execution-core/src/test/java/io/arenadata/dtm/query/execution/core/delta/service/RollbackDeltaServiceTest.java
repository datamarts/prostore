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
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaRecord;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.dto.query.RollbackDeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.factory.impl.CommitDeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.delta.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.UploadFailedExecutor;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RollbackDeltaServiceTest {
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(CommitDeltaQueryResultFactory.class);
    private final UploadFailedExecutor uploadFailedExecutor = mock(UploadFailedExecutor.class);
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService =
            mock(EvictQueryTemplateCacheService.class);
    private final RestoreStateService restoreStateService = mock(RestoreStateService.class);
    private BreakMppwService breakMppwService = mock(BreakMppwService.class);
    private BreakLlwService breakLlwService = mock(BreakLlwService.class);
    private RollbackDeltaService rollbackDeltaService;
    private final QueryRequest req = new QueryRequest();
    private final DeltaRecord delta = new DeltaRecord();
    private final String datamart = "test_datamart";

    @BeforeEach
    void beforeAll() {
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        delta.setDatamart(req.getDatamartMnemonic());
        Entity entity = Entity.builder()
                .name("test_entity")
                .schema(datamart)
                .fields(Collections.emptyList())
                .build();
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(entityDao.getEntity(eq(datamart), any())).thenReturn(Future.succeededFuture(entity));
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(uploadFailedExecutor.eraseWriteOp(any())).thenReturn(Future.succeededFuture());
        rollbackDeltaService = new RollbackDeltaService(uploadFailedExecutor, serviceDbFacade,
                deltaQueryResultFactory, Vertx.vertx(), evictQueryTemplateCacheService, restoreStateService, breakMppwService, breakLlwService);
        when(deltaServiceDao.writeDeltaError(eq(datamart), eq(null)))
                .thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteDeltaHot(datamart))
                .thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase(datamart)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(breakMppwService.breakMppw(datamart)).thenReturn(Future.succeededFuture());
        when(breakLlwService.breakLlw(datamart)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.getDeltaWriteOperations(datamart)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        doNothing().when(evictQueryTemplateCacheService).evictByDatamartName(anyString());
    }

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        req.setSql("ROLLBACK DELTA");
        String deltaDateStr = "2020-06-16 14:00:11";
        final LocalDateTime deltaDate = LocalDateTime.parse(deltaDateStr,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        RollbackDeltaQuery deltaQuery = RollbackDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));
        HotDelta hotDelta = HotDelta.builder()
                .deltaNum(1)
                .cnFrom(1)
                .cnTo(3L)
                .build();
        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture(hotDelta));
        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);
        rollbackDeltaService.execute(deltaQuery)
                .onComplete(promise);
        assertEquals(deltaDate, ((QueryResult) promise.future().result()).getResult()
                .get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
        verifyEvictCacheExecuted();
        verify(restoreStateService).restoreErase(datamart);
        verify(breakMppwService).breakMppw(datamart);
        verifyNoInteractions(breakLlwService);
    }

    @Test
    void executeHotDeltaNotExistError() {
        Promise promise = Promise.promise();
        req.setSql("ROLLBACK DELTA");
        String deltaDateStr = "2020-06-16 14:00:11";
        final LocalDateTime deltaDate = LocalDateTime.parse(deltaDateStr,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        RollbackDeltaQuery deltaQuery = RollbackDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));
        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture());
        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);
        rollbackDeltaService.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verifyEvictCacheExecuted();
    }

    @Test
    void executeDeltaQueryResultFactoryError() {
        Promise promise = Promise.promise();
        req.setSql("ROLLBACK DELTA");
        String deltaDateStr = "2020-06-16 14:00:11";
        final LocalDateTime deltaDate = LocalDateTime.parse(deltaDateStr,
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        RollbackDeltaQuery deltaQuery = RollbackDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaDate));
        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture());
        when(deltaQueryResultFactory.create(any())).thenThrow(new DtmException(""));
        rollbackDeltaService.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        verifyEvictCacheExecuted();
    }

    private List<Map<String, Object>> createResult(LocalDateTime deltaDate) {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.DATE_TIME_FIELD),
                Collections.singletonList(deltaDate));
    }

    private void verifyEvictCacheExecuted() {
        verify(evictQueryTemplateCacheService, times(1)).evictByDatamartName(datamart);
    }
}
