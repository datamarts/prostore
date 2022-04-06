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

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.delta.EraseWriteOperation;
import ru.datamart.prostore.query.calcite.core.extension.delta.SqlDeltaCall;
import ru.datamart.prostore.query.calcite.core.extension.eddl.SqlNodeUtils;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import ru.datamart.prostore.query.execution.core.delta.dto.operation.DeltaRequestContext;
import ru.datamart.prostore.query.execution.core.delta.dto.query.*;
import ru.datamart.prostore.query.execution.core.delta.factory.DeltaQueryFactory;
import ru.datamart.prostore.query.execution.core.metrics.service.MetricsService;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DeltaExecutionServiceTest {
    private final MetricsService metricsService = mock(MetricsService.class);
    private DeltaExecutionService deltaExecutionService;
    private final DeltaQueryFactory deltaQueryFactory = mock(DeltaQueryFactory.class);
    private final DeltaService beginDeltaService = mock(BeginDeltaService.class);
    private final DeltaService commitDeltaService = mock(CommitDeltaService.class);
    private final DeltaService rollbackDeltaService = mock(RollbackDeltaService.class);
    private final DeltaService getDeltaByDateTimeExecutor = mock(GetDeltaByDateTimeService.class);
    private final DeltaService getDeltaByNumExecutor = mock(GetDeltaByNumService.class);
    private final DeltaService getDeltaByHotExecutor = mock(GetDeltaHotService.class);
    private final DeltaService getDeltaByOkExecutor = mock(GetDeltaOkService.class);
    private final DeltaService eraseWriteOperationService = mock(EraseWriteOperationService.class);
    private final String envName = "test";
    private static final String CONTEXT_DTM = "CONTEXT_DTM";
    private static final String SQL_QUERY_DTM = "SQL_DTM";


    @BeforeEach
    void setUp() {
        List<DeltaService> executors = Arrays.asList(
                beginDeltaService,
                commitDeltaService,
                rollbackDeltaService,
                getDeltaByDateTimeExecutor,
                getDeltaByNumExecutor,
                getDeltaByHotExecutor,
                getDeltaByOkExecutor,
                eraseWriteOperationService
        );
        executors.forEach(this::setUpExecutor);
        when(metricsService.sendMetrics(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(metricsService.sendMetrics(any(), any(), any(), any())).thenAnswer(answer -> {
            Handler<AsyncResult<QueryResult>> promise = answer.getArgument(3);
            return (Handler<AsyncResult<QueryResult>>) ar -> promise.handle(Future.succeededFuture(ar.result()));
        });
        deltaExecutionService = new DeltaExecutionService(executors,
                metricsService, deltaQueryFactory);
    }

    void setUpExecutor(DeltaService executor) {
        when(executor.getAction()).thenCallRealMethod();
        when(executor.execute(any())).thenReturn(Future.succeededFuture(QueryResult.emptyResult()));
    }

    @Test
    void executeWithNullDatamart() {
        DeltaQuery deltaQuery = new BeginDeltaQuery(new QueryRequest(), null, null, null);
        executeTest(getContext(null), deltaQuery, ar -> assertTrue(ar.failed()));
    }

    @Test
    void executeWithEmptyDatamart() {
        DeltaQuery deltaQuery = new BeginDeltaQuery(new QueryRequest(), null, null, null);
        executeTest(getContext(""), deltaQuery, ar -> assertTrue(ar.failed()));
    }

    @Test
    void checkBeginDelta() {
        DeltaQuery deltaQuery = new BeginDeltaQuery(new QueryRequest(), null, null, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(beginDeltaService, times(1)).execute(any());
        });
    }

    @Test
    void checkCommitDelta() {
        DeltaQuery deltaQuery = new CommitDeltaQuery(new QueryRequest(), null, null, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(commitDeltaService, times(1)).execute(any());
        });
    }

    @Test
    void checkRollbackDelta() {
        DeltaQuery deltaQuery = new RollbackDeltaQuery(new QueryRequest(), null, null, null, "test", null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(rollbackDeltaService, times(1)).execute(any());
        });
    }

    @Test
    void checkGetDeltaByDateTime() {
        DeltaQuery deltaQuery = new GetDeltaByDateTimeQuery(new QueryRequest(), null, null, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(getDeltaByDateTimeExecutor, times(1)).execute(any());
        });
    }

    @Test
    void checkGetDeltaByNum() {
        DeltaQuery deltaQuery = new GetDeltaByNumQuery(new QueryRequest(), null, null, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(getDeltaByNumExecutor, times(1)).execute(any());
        });
    }

    @Test
    void checkGetDeltaHot() {
        DeltaQuery deltaQuery = new GetDeltaHotQuery(new QueryRequest(), null, null, null,
                null, null, null, false, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(getDeltaByHotExecutor, times(1)).execute(any());
        });
    }

    @Test
    void checkGetDeltaOk() {
        DeltaQuery deltaQuery = new GetDeltaOkQuery(new QueryRequest(), null, null, null, null, null);
        executeTest(getContext("test"), deltaQuery, ar -> {
            assertTrue(ar.succeeded());
            verify(getDeltaByOkExecutor, times(1)).execute(any());
        });
    }

    @Test
    void checkEraseWriteOperation() {
        //arrange
        val deltaQuery = new EraseWriteOperationDeltaQuery(new QueryRequest(), null, null, null, null);

        //act
        executeTest(getContext("test"), deltaQuery, ar -> {

            //assert
            assertTrue(ar.succeeded());
            verify(eraseWriteOperationService, times(1)).execute(any());
        });
    }

    @Test
    void shouldSuccessWhenEraseWriteOperationWithDifferentDatamarts() {
        //arrange
        val deltaQuery = new EraseWriteOperationDeltaQuery(new QueryRequest(), null, null, null, null);
        val context = getEraseWriteOperationContext(CONTEXT_DTM, SQL_QUERY_DTM);

        //act
        executeTest(context, deltaQuery, ar -> {

            //assert
            assertTrue(ar.succeeded());
            verify(eraseWriteOperationService, times(1)).execute(any());
        });
    }

    @Test
    void shouldSuccessWhenEraseWriteOperationNullDtmAndContextDtm() {
        //arrange
        val deltaQuery = new EraseWriteOperationDeltaQuery(new QueryRequest(), null, null, null, null);
        val context = getEraseWriteOperationContext(CONTEXT_DTM, null);

        //act
        executeTest(context, deltaQuery, ar -> {

            //assert
            assertTrue(ar.succeeded());
            verify(eraseWriteOperationService, times(1)).execute(any());
        });
    }

    @Test
    void shouldSuccessWhenEraseWriteOperationDtmAndEmptyContextDtm() {
        //arrange
        val deltaQuery = new EraseWriteOperationDeltaQuery(new QueryRequest(), null, null, null, null);
        val context = getEraseWriteOperationContext("", SQL_QUERY_DTM);

        //act
        executeTest(context, deltaQuery, ar -> {

            //assert
            assertTrue(ar.succeeded());
            verify(eraseWriteOperationService, times(1)).execute(any());
        });
    }

    @Test
    void shouldFailWhenEraseWriteOperationNullDtmAndEmptyContextDtm() {
        //arrange
        val deltaQuery = new EraseWriteOperationDeltaQuery(new QueryRequest(), null, null, null, null);
        val context = getEraseWriteOperationContext("", null);

        //act
        executeTest(context, deltaQuery, ar -> {

            //assert
            assertTrue(ar.failed());
            assertEquals("Datamart must be not empty!\n" +
                    "For setting datamart you can use the following command: \"USE datamartName\"", ar.cause().getMessage());
            verify(eraseWriteOperationService, times(0)).execute(any());
        });
    }

    void executeTest(DeltaRequestContext context, DeltaQuery deltaQuery, Consumer<AsyncResult<QueryResult>> validate) {
        when(deltaQueryFactory.create(any())).thenReturn(deltaQuery);
        deltaExecutionService.execute(context)
                .onComplete(validate::accept);
    }

    private DeltaRequestContext getEraseWriteOperationContext(String contextDtm, String queryDtm){
        val request = new QueryRequest();
        request.setDatamartMnemonic(contextDtm);
        val datamartRequest = new DatamartRequest(request);

        val pos = new SqlParserPos(0, 0);
        val sqlQueryDtm = queryDtm == null ? null : SqlNodeTemplates.identifier(queryDtm);
        val sysCn = SqlNodeTemplates.longLiteral(5L);
        val eraseWriteOperationNode = new EraseWriteOperation(pos, sysCn, sqlQueryDtm);

        return new DeltaRequestContext(new RequestMetrics(), datamartRequest, envName, eraseWriteOperationNode);
    }

    private DeltaRequestContext getContext(String datamart) {
        QueryRequest request = new QueryRequest();
        request.setDatamartMnemonic(datamart);
        DatamartRequest datamartRequest = new DatamartRequest(request);
        return new DeltaRequestContext(new RequestMetrics(), datamartRequest, envName, null);
    }
}
