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
package io.arenadata.dtm.query.execution.core.eddl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlAction;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlQuery;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlRequestContext;
import io.arenadata.dtm.query.execution.core.eddl.service.EddlExecutor;
import io.arenadata.dtm.query.execution.core.eddl.service.EddlQueryParamExtractor;
import io.arenadata.dtm.query.execution.core.eddl.service.EddlService;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsService;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EddlServiceTest {

    private static final String INFORMATION_SCHEMA = "information_schema";
    private static final String SCHEMA_NAME = "dtm";
    private static final String ERROR_MESSAGE = "ERROR";
    private final EddlQueryParamExtractor paramExtractor = mock(EddlQueryParamExtractor.class);
    private final EddlExecutor eddlExecutor = mock(EddlExecutor.class);
    private final MetricsService metricsService = mock(MetricsService.class);
    private final EddlQuery eddlQuery = mock(EddlQuery.class);
    private final EddlRequestContext context = EddlRequestContext.builder().build();
    private final QueryResult queryResult = QueryResult.builder()
            .requestId(UUID.randomUUID())
            .build();
    private EddlService eddlService;

    @BeforeEach
    void setUp() {
        when(eddlExecutor.getAction()).thenReturn(EddlAction.CREATE_DOWNLOAD_EXTERNAL_TABLE);
        eddlService = new EddlService(paramExtractor, Collections.singletonList(eddlExecutor), metricsService);
    }

    @Test
    void shouldSucceed() {
        //arrange
        when(paramExtractor.extract(any())).thenReturn(Future.succeededFuture(eddlQuery));
        when(eddlQuery.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(metricsService.sendMetrics(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(eddlQuery.getAction()).thenReturn(EddlAction.CREATE_DOWNLOAD_EXTERNAL_TABLE);
        when(eddlExecutor.execute(any())).thenReturn(Future.succeededFuture(queryResult));

        //act
        eddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(queryResult.getRequestId(), ar.result().getRequestId());
                });
    }

    @Test
    void shouldFailWhenExtractParamError() {
        //arrange
        when(paramExtractor.extract(any())).thenReturn(Future.failedFuture(new DtmException(ERROR_MESSAGE)));

        //act
        eddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(ERROR_MESSAGE, ar.cause().getMessage());
                });
    }

    @Test
    void shouldFailWhenSendMetricsError() {
        //arrange
        when(paramExtractor.extract(any())).thenReturn(Future.succeededFuture(eddlQuery));
        when(eddlQuery.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(metricsService.sendMetrics(any(), any(), any())).thenReturn(Future.failedFuture(new DtmException(ERROR_MESSAGE)));

        //act
        eddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(ERROR_MESSAGE, ar.cause().getMessage());
                });
    }

    @Test
    void shouldFailWhenExecutorError() {
        //arrange
        when(paramExtractor.extract(any())).thenReturn(Future.succeededFuture(eddlQuery));
        when(eddlQuery.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(metricsService.sendMetrics(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(eddlQuery.getAction()).thenReturn(EddlAction.CREATE_DOWNLOAD_EXTERNAL_TABLE);
        when(eddlExecutor.execute(any())).thenReturn(Future.failedFuture(new DtmException(ERROR_MESSAGE)));

        //act
        eddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(ERROR_MESSAGE, ar.cause().getMessage());
                });
    }

    @Test
    void shouldFailWhenNoExecutorFound() {
        //arrange
        when(paramExtractor.extract(any())).thenReturn(Future.succeededFuture(eddlQuery));
        when(eddlQuery.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(metricsService.sendMetrics(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(eddlQuery.getAction()).thenReturn(EddlAction.DROP_DOWNLOAD_EXTERNAL_TABLE);

        //act
        eddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.failed());
                });
    }

    @Test
    void shouldFailWhenInformationSchema() {
        //arrange
        when(paramExtractor.extract(any())).thenReturn(Future.succeededFuture(eddlQuery));
        when(eddlQuery.getSchemaName()).thenReturn(INFORMATION_SCHEMA);

        //act
        eddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(ValidationDtmException.class, ar.cause().getClass());
                });
    }
}
