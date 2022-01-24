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

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.core.delta.dto.operation.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.delta.dto.query.BeginDeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.dto.query.CommitDeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.dto.query.GetDeltaByDateTimeQuery;
import io.arenadata.dtm.query.execution.core.delta.dto.query.GetDeltaByNumQuery;
import io.arenadata.dtm.query.execution.core.delta.dto.query.GetDeltaHotQuery;
import io.arenadata.dtm.query.execution.core.delta.dto.query.GetDeltaOkQuery;
import io.arenadata.dtm.query.execution.core.delta.dto.query.RollbackDeltaQuery;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryFactory;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    private final String envName = "test";

    @BeforeEach
    void setUp() {
        List<DeltaService> executors = Arrays.asList(
                beginDeltaService,
                commitDeltaService,
                rollbackDeltaService,
                getDeltaByDateTimeExecutor,
                getDeltaByNumExecutor,
                getDeltaByHotExecutor,
                getDeltaByOkExecutor
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
        DeltaQuery deltaQuery = new RollbackDeltaQuery(new QueryRequest(), null, null, null,  "test", null);
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

    void executeTest(DeltaRequestContext context, DeltaQuery deltaQuery, Consumer<AsyncResult<QueryResult>> validate) {
        when(deltaQueryFactory.create(any())).thenReturn(deltaQuery);
        deltaExecutionService.execute(context)
                .onComplete(validate::accept);
    }

    private DeltaRequestContext getContext(String datamart) {
        QueryRequest request = new QueryRequest();
        request.setDatamartMnemonic(datamart);
        DatamartRequest datamartRequest = new DatamartRequest(request);
        return new DeltaRequestContext(new RequestMetrics(), datamartRequest, envName, null);
    }
}
