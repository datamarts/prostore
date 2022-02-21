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
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaRecord;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.query.CommitDeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.factory.DeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.factory.impl.CommitDeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.impl.DeltaServiceDaoImpl;
import ru.datamart.prostore.query.execution.core.delta.utils.DeltaQueryUtil;
import ru.datamart.prostore.query.execution.core.utils.QueryResultUtils;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
class CommitDeltaServiceTest {

    private final QueryRequest req = new QueryRequest();
    private final DeltaRecord delta = new DeltaRecord();
    private final String datamart = "test_datamart";
    private final String deltaDateStr = "2020-06-16 14:00:11";
    private final LocalDateTime deltaDate = LocalDateTime.parse(deltaDateStr,
            DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
    private final OkDelta okDelta = OkDelta.builder()
            .deltaNum(1)
            .deltaDate(deltaDate)
            .build();

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(CommitDeltaQueryResultFactory.class);
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService =
            mock(EvictQueryTemplateCacheService.class);
    private CommitDeltaService commitDeltaService;

    @BeforeEach
    void beforeAll() {
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        delta.setDatamart(req.getDatamartMnemonic());
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        commitDeltaService = new CommitDeltaService(serviceDbFacade, deltaQueryResultFactory, Vertx.vertx(),
                evictQueryTemplateCacheService);
        doNothing().when(evictQueryTemplateCacheService).evictByDatamartName(anyString());
    }

    @Test
    void executeSuccess(VertxTestContext testContext) {
        //arrange
        req.setSql("COMMIT DELTA");
        val deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        val queryResult = createQueryResult(deltaDate);

        when(deltaServiceDao.writeDeltaHotSuccess(datamart, null))
                .thenReturn(Future.succeededFuture(okDelta));
        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);

        //act
        commitDeltaService.execute(deltaQuery).onComplete(ar -> testContext.verify(() -> {
            // assert
            assertTrue(ar.succeeded());
            assertEquals(deltaDate, ar.result().getResult()
                    .get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
            verifyEvictCacheExecuted();
        }).completeNow());
    }

    @Test
    void executeWithDatetimeSuccess(VertxTestContext testContext) {
        //arrange
        req.setSql("COMMIT DELTA '" + deltaDateStr + "'");
        val deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .deltaDate(deltaDate)
                .build();
        val queryResult = createQueryResult(deltaDate);

        when(deltaServiceDao.writeDeltaHotSuccess(any(), any()))
                .thenReturn(Future.succeededFuture(okDelta));
        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);

        //act
        commitDeltaService.execute(deltaQuery).onComplete(ar -> testContext.verify(() -> {
            // assert
            assertTrue(ar.succeeded());
            assertEquals(deltaDate, ar.result().getResult()
                    .get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
            verifyEvictCacheExecuted();
        }).completeNow());
    }

    @Test
    void executeWriteDeltaHotSuccessError(VertxTestContext testContext) {
        //arrange
        req.setSql("COMMIT DELTA");
        val deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        val queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        val errorMessage = "write delta hot success";

        when(deltaServiceDao.writeDeltaHotSuccess(datamart, null))
                .thenReturn(Future.failedFuture(new DtmException(errorMessage)));

        //act
        commitDeltaService.execute(deltaQuery).onComplete(ar -> testContext.verify(() -> {
            // assert
            assertTrue(ar.failed());
            assertEquals(errorMessage, ar.cause().getMessage());
            verifyEvictCacheExecuted();
        }).completeNow());
    }

    @Test
    void executeWithDatetimeWriteDeltaHotSuccessError(VertxTestContext testContext) {
        //arrange
        req.setSql("COMMIT DELTA '" + deltaDateStr + "'");
        val deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .deltaDate(deltaDate)
                .build();

        val errorMessage = "write delta hot success";
        when(deltaServiceDao.writeDeltaHotSuccess(datamart, deltaDate))
                .thenReturn(Future.failedFuture(new RuntimeException(errorMessage)));

        //act
        commitDeltaService.execute(deltaQuery).onComplete(ar -> testContext.verify(() -> {
            // assert
            assertTrue(ar.failed());
            assertEquals(errorMessage, ar.cause().getMessage());
            verifyEvictCacheExecuted();
        }).completeNow());
    }

    @Test
    void executeDeltaQueryResultFactoryError(VertxTestContext testContext) {
        //arrange
        req.setSql("COMMIT DELTA");
        val deltaQuery = CommitDeltaQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        when(deltaServiceDao.writeDeltaHotSuccess(any(), any()))
                .thenReturn(Future.succeededFuture(okDelta));

        val errorMessage = "error create query result";
        when(deltaQueryResultFactory.create(any()))
                .thenThrow(new DtmException(errorMessage));

        //act
        commitDeltaService.execute(deltaQuery).onComplete(ar -> testContext.verify(() -> {
            // assert
            assertTrue(ar.failed());
            verifyEvictCacheExecuted();
        }).completeNow());
    }

    private QueryResult createQueryResult(LocalDateTime deltaDate) {
        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.DATE_TIME_FIELD),
                Collections.singletonList(deltaDate)));
        return queryResult;
    }

    private void verifyEvictCacheExecuted() {
        verify(evictQueryTemplateCacheService, times(1)).evictByDatamartName(datamart);
    }
}
