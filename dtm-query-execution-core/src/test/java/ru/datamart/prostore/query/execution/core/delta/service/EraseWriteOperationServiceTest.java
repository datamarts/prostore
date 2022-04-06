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
package ru.datamart.prostore.query.execution.core.delta.service;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaAction;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.dto.query.EraseWriteOperationDeltaQuery;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;


@ExtendWith({VertxExtension.class, MockitoExtension.class})
class EraseWriteOperationServiceTest {
    private static final String QUERY_DTM = "dtm";
    private static final String CONTEXT_DTM = "abc";
    private static final Long SYS_CN = 5L;
    private static final DeltaAction DELTA_ACTION = DeltaAction.ERASE_WRITE_OPERATION;
    @Captor
    private ArgumentCaptor<String> datamartCapture;

    @Mock
    private BreakMppwService breakMppwService;

    @Mock
    private BreakLlwService breakLlwService;

    @Mock
    private RestoreStateService restoreStateService;

    @Mock
    private DatamartDao datamartDao;

    @InjectMocks
    private EraseWriteOperationService service;

    @Test
    void shouldSuccess(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(QUERY_DTM)).thenReturn(Future.succeededFuture(true));
        when(breakMppwService.breakMppw(QUERY_DTM, SYS_CN)).thenReturn(Future.succeededFuture());
        when(breakLlwService.breakLlw(QUERY_DTM, SYS_CN)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase(QUERY_DTM, SYS_CN)).thenReturn(Future.succeededFuture(Collections.emptyList()));

        //act
        service.execute(getDeltaQuery())
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                    verify(breakMppwService, times(1)).breakMppw(QUERY_DTM, SYS_CN);
                    verify(breakLlwService, times(1)).breakLlw(QUERY_DTM, SYS_CN);
                    verify(restoreStateService, times(1)).restoreErase(QUERY_DTM, SYS_CN);
                }).completeNow());

    }

    @Test
    void shouldFailWhenBreakMppwFailed(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(QUERY_DTM)).thenReturn(Future.succeededFuture(true));
        when(breakMppwService.breakMppw(QUERY_DTM, SYS_CN)).thenReturn(Future.failedFuture(new RuntimeException("error in mppw")));

        //act
        service.execute(getDeltaQuery())
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertEquals("error in mppw", ar.cause().getMessage());

                    verify(breakMppwService, times(1)).breakMppw(QUERY_DTM, SYS_CN);
                    verifyNoInteractions(breakLlwService, restoreStateService);
                }).completeNow());
    }

    @Test
    void shouldFailWhenRestoreFailed(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(QUERY_DTM)).thenReturn(Future.succeededFuture(true));
        when(breakMppwService.breakMppw(QUERY_DTM, SYS_CN)).thenReturn(Future.succeededFuture());
        when(breakLlwService.breakLlw(QUERY_DTM, SYS_CN)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase(QUERY_DTM, SYS_CN)).thenReturn(Future.failedFuture(new RuntimeException("error in restore")));

        //act
        service.execute(getDeltaQuery())
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertEquals("error in restore", ar.cause().getMessage());

                    verify(breakMppwService, times(1)).breakMppw(QUERY_DTM, SYS_CN);
                    verify(breakLlwService, times(1)).breakLlw(QUERY_DTM, SYS_CN);
                    verify(restoreStateService, times(1)).restoreErase(QUERY_DTM, SYS_CN);
                }).completeNow());
    }

    @Test
    void shouldFailWhenBreakLlwFailed(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(QUERY_DTM)).thenReturn(Future.succeededFuture(true));
        when(breakMppwService.breakMppw(QUERY_DTM, SYS_CN)).thenReturn(Future.succeededFuture());
        when(breakLlwService.breakLlw(QUERY_DTM, SYS_CN)).thenReturn(Future.failedFuture(new RuntimeException("error in llw")));

        //act
        service.execute(getDeltaQuery())
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertEquals("error in llw", ar.cause().getMessage());

                    verify(breakMppwService, times(1)).breakMppw(QUERY_DTM, SYS_CN);
                    verify(breakLlwService, times(1)).breakLlw(QUERY_DTM, SYS_CN);
                    verifyNoInteractions(restoreStateService);
                }).completeNow());
    }

    @Test
    void shouldExecuteInQueryDtm(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(any())).thenReturn(Future.succeededFuture(true));
        when(breakMppwService.breakMppw(datamartCapture.capture(), any())).thenReturn(Future.succeededFuture());
        when(breakLlwService.breakLlw(any(), any())).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase(any(), any())).thenReturn(Future.succeededFuture(Collections.emptyList()));

        val request = QueryRequest.builder()
                .datamartMnemonic(CONTEXT_DTM)
                .build();

        //act
        service.execute(getDeltaQueryWithQueryRequest(QUERY_DTM, request))
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(QUERY_DTM, datamartCapture.getValue());
                }).completeNow());
    }

    @Test
    void shouldExecuteInContextDtmWhenNullQueryDtm(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(any())).thenReturn(Future.succeededFuture(true));
        when(breakMppwService.breakMppw(datamartCapture.capture(), any())).thenReturn(Future.succeededFuture());
        when(breakLlwService.breakLlw(any(), any())).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase(any(), any())).thenReturn(Future.succeededFuture(Collections.emptyList()));

        val request = QueryRequest.builder()
                .datamartMnemonic(CONTEXT_DTM)
                .build();

        //act
        service.execute(getDeltaQueryWithQueryRequest(null, request))
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(CONTEXT_DTM, datamartCapture.getValue());
                }).completeNow());
    }

    @Test
    void shouldFailWhenDatamartNotExist(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(QUERY_DTM)).thenReturn(Future.succeededFuture(false));

        //act
        service.execute(getDeltaQuery())
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertEquals("Database dtm does not exist", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldReturnCorrectDeltaAction() {
        assertEquals(DELTA_ACTION, service.getAction());
    }

    private DeltaQuery getDeltaQuery() {
        return EraseWriteOperationDeltaQuery.builder()
                .sysCn(SYS_CN)
                .datamart(QUERY_DTM)
                .build();
    }

    private DeltaQuery getDeltaQueryWithQueryRequest(String datamart, QueryRequest queryRequest) {
        return EraseWriteOperationDeltaQuery.builder()
                .sysCn(SYS_CN)
                .request(queryRequest)
                .datamart(datamart)
                .build();
    }
}