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
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.operation.WriteOpFinish;
import ru.datamart.prostore.query.execution.core.delta.dto.query.GetDeltaHotQuery;
import ru.datamart.prostore.query.execution.core.delta.factory.DeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.factory.impl.BeginDeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.impl.DeltaServiceDaoImpl;
import ru.datamart.prostore.query.execution.core.delta.utils.DeltaQueryUtil;
import ru.datamart.prostore.query.execution.core.utils.QueryResultUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GetDeltaHotServiceTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(BeginDeltaQueryResultFactory.class);
    private DeltaService deltaHotService;
    private final QueryRequest req = new QueryRequest();
    private String datamart;

    @BeforeEach
    void setUp() {
        datamart = "test_datamart";
        req.setDatamartMnemonic(datamart);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);

        deltaHotService = new GetDeltaHotService(serviceDbFacade, deltaQueryResultFactory);
    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaHotQuery deltaQuery = GetDeltaHotQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        final long cnFrom = 0L;
        final long deltaNum = 1L;
        final long cnMax = 10L;
        final boolean isRollingBack = false;
        final List<WriteOpFinish> writeOpFinishList = new ArrayList<>();
        writeOpFinishList.add(new WriteOpFinish("t1", Arrays.asList(0L, 1L)));
        writeOpFinishList.add(new WriteOpFinish("t2", Arrays.asList(0L, 1L)));
        JsonArray wrOpArr = new JsonArray();
        writeOpFinishList.forEach(wo -> {
            wrOpArr.add(JsonObject.mapFrom(wo));
        });
        String writeOpFinishListStr = wrOpArr.toString();
        HotDelta deltaHot = HotDelta.builder()
                .cnFrom(cnFrom)
                .cnMax(cnMax)
                .deltaNum(deltaNum)
                .rollingBack(isRollingBack)
                .writeOperationsFinished(writeOpFinishList)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaNum, cnFrom, null, cnMax, isRollingBack, writeOpFinishListStr));

        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaHot));

        when(deltaQueryResultFactory.create(any()))
                .thenReturn(queryResult);

        deltaHotService.execute(deltaQuery)
                .onComplete(promise);

        assertEquals(deltaNum, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.NUM_FIELD));
        assertEquals(cnFrom, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.CN_FROM_FIELD));
        assertNull(promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.CN_TO_FIELD));
        assertEquals(cnMax, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.CN_MAX_FIELD));
        assertEquals(isRollingBack, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.IS_ROLLING_BACK_FIELD));
        assertEquals(writeOpFinishListStr, promise.future().result().getResult()
                .get(0).get(DeltaQueryUtil.WRITE_OP_FINISHED_FIELD));
    }

    @Test
    void executeEmptySuccess() {
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaHotQuery deltaQuery = GetDeltaHotQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(new ArrayList<>());

        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture(null));

        when(deltaQueryResultFactory.createEmpty())
                .thenReturn(queryResult);

        deltaHotService.execute(deltaQuery)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeGetDeltaHotError() {
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaHotQuery deltaQuery = GetDeltaHotQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();

        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.failedFuture(new DtmException("")));

        deltaHotService.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeDeltaQueryResultFactoryError() {
        Promise<QueryResult> promise = Promise.promise();
        GetDeltaHotQuery deltaQuery = GetDeltaHotQuery.builder()
                .request(req)
                .datamart(datamart)
                .build();
        final long cnFrom = 0L;
        final long deltaNum = 1L;
        final long cnMax = 10L;
        final boolean isRollingBack = false;
        final List<WriteOpFinish> writeOpFinishList = new ArrayList<>();
        writeOpFinishList.add(new WriteOpFinish("t1", Arrays.asList(0L, 1L)));
        writeOpFinishList.add(new WriteOpFinish("t2", Arrays.asList(0L, 1L)));

        HotDelta deltaHot = HotDelta.builder()
                .cnFrom(cnFrom)
                .cnMax(cnMax)
                .deltaNum(deltaNum)
                .rollingBack(isRollingBack)
                .writeOperationsFinished(writeOpFinishList)
                .build();

        when(deltaServiceDao.getDeltaHot(eq(datamart)))
                .thenReturn(Future.succeededFuture(deltaHot));

        when(deltaQueryResultFactory.create(any()))
                .thenThrow(new DtmException(""));

        deltaHotService.execute(deltaQuery)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    private List<Map<String, Object>> createResult(long deltaNum, long cnFrom, Long cnTo, long cnMax,
                                                   boolean isRollingBack, String wrOpList) {
        return QueryResultUtils.createResultWithSingleRow(Arrays.asList(
                DeltaQueryUtil.NUM_FIELD,
                DeltaQueryUtil.CN_FROM_FIELD,
                DeltaQueryUtil.CN_TO_FIELD,
                DeltaQueryUtil.CN_MAX_FIELD,
                DeltaQueryUtil.IS_ROLLING_BACK_FIELD,
                DeltaQueryUtil.WRITE_OP_FINISHED_FIELD),
                Arrays.asList(deltaNum, cnFrom, cnTo, cnMax, isRollingBack, wrOpList));
    }
}
