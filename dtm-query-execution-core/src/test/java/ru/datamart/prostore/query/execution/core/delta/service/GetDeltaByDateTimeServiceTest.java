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
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.query.GetDeltaByDateTimeQuery;
import ru.datamart.prostore.query.execution.core.delta.factory.DeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.factory.impl.BeginDeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.impl.DeltaServiceDaoImpl;
import ru.datamart.prostore.query.execution.core.delta.utils.DeltaQueryUtil;
import ru.datamart.prostore.query.execution.core.utils.QueryResultUtils;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GetDeltaByDateTimeServiceTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(BeginDeltaQueryResultFactory.class);
    private DeltaService deltaByDateTimeService;
    private final QueryRequest req = new QueryRequest();
    private String datamart;

    @BeforeEach
    void setUp() {
        datamart = "test_datamart";
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);

        deltaByDateTimeService = new GetDeltaByDateTimeService(serviceDbFacade, deltaQueryResultFactory);
    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        final LocalDateTime deltaDate = LocalDateTime.parse("2020-06-15 14:00:11",
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        final long cnFrom = 0L;
        final long deltaNum = 1L;
        GetDeltaByDateTimeQuery deltaQuery = GetDeltaByDateTimeQuery.builder()
                .deltaDate(deltaDate)
                .request(req)
                .datamart(datamart)
                .build();

        OkDelta deltaOk = OkDelta.builder()
                .cnFrom(cnFrom)
                .deltaNum(deltaNum)
                .deltaDate(deltaDate)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaNum, deltaDate, cnFrom, null));

        when(deltaServiceDao.getDeltaByDateTime(eq(datamart), eq(deltaDate)))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(deltaQueryResultFactory.create(any()))
                .thenReturn(queryResult);

        deltaByDateTimeService.execute(deltaQuery)
                .onComplete(promise);

        assertEquals(deltaNum, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.NUM_FIELD));
        assertEquals(deltaDate, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.DATE_TIME_FIELD));
        assertEquals(cnFrom, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.CN_FROM_FIELD));
        assertNull(promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.CN_TO_FIELD));
    }

    @Test
    void executeEmptySuccess() {
        Promise<QueryResult> promise = Promise.promise();
        final LocalDateTime deltaDate = LocalDateTime.parse("2020-06-15 14:00:11",
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        GetDeltaByDateTimeQuery deltaQuery = GetDeltaByDateTimeQuery.builder()
                .request(req)
                .deltaDate(deltaDate)
                .datamart(datamart)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(new ArrayList<>());

        when(deltaServiceDao.getDeltaByDateTime(eq(datamart), eq(deltaDate)))
                .thenReturn(Future.succeededFuture(null));

        when(deltaQueryResultFactory.createEmpty())
                .thenReturn(queryResult);

        deltaByDateTimeService.execute(deltaQuery)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeDeltaByDateTimeError() {
        Promise<QueryResult> promise = Promise.promise();
        final LocalDateTime deltaDate = LocalDateTime.parse("2020-06-15 14:00:11",
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        GetDeltaByDateTimeQuery deltaQuery = GetDeltaByDateTimeQuery.builder()
                .deltaDate(deltaDate)
                .request(req)
                .datamart(datamart)
                .build();

        when(deltaServiceDao.getDeltaByDateTime(eq(datamart), eq(deltaDate)))
                .thenReturn(Future.failedFuture(new DtmException("")));

        deltaByDateTimeService.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeDeltaQueryResultFactoryError() {
        Promise<QueryResult> promise = Promise.promise();
        final LocalDateTime deltaDate = LocalDateTime.parse("2020-06-15 14:00:11",
                DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER);
        final long cnFrom = 0L;
        final long deltaNum = 1L;
        GetDeltaByDateTimeQuery deltaQuery = GetDeltaByDateTimeQuery.builder()
                .request(req)
                .deltaDate(deltaDate)
                .datamart(datamart)
                .build();

        OkDelta deltaOk = OkDelta.builder()
                .cnFrom(cnFrom)
                .deltaNum(deltaNum)
                .deltaDate(deltaDate)
                .build();

        when(deltaServiceDao.getDeltaByDateTime(eq(datamart), eq(deltaDate)))
                .thenReturn(Future.succeededFuture(deltaOk));

        when(deltaQueryResultFactory.create(any()))
                .thenThrow(new DtmException(""));

        deltaByDateTimeService.execute(deltaQuery)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }

    private List<Map<String, Object>> createResult(long deltaNum, LocalDateTime deltaDate, long cnFrom, Long cnTo) {
        return QueryResultUtils.createResultWithSingleRow(Arrays.asList(
                DeltaQueryUtil.NUM_FIELD,
                DeltaQueryUtil.DATE_TIME_FIELD,
                DeltaQueryUtil.CN_FROM_FIELD,
                DeltaQueryUtil.CN_TO_FIELD),
                Arrays.asList(deltaNum, deltaDate, cnFrom, cnTo));
    }
}
