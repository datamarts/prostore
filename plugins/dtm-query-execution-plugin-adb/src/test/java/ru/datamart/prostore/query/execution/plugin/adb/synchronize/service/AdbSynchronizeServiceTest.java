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
package ru.datamart.prostore.query.execution.plugin.adb.synchronize.service;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutorDelegate;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.impl.AdbSynchronizeService;
import ru.datamart.prostore.query.execution.plugin.api.exception.SynchronizeDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.EnumSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdbSynchronizeServiceTest {

    private static final Long SUCCEED_DELTA_NUM = 42L;
    private static final Long FAILED_DELTA_NUM = 43L;

    @Mock
    private SynchronizeDestinationExecutorDelegate executorDelegate;

    private AdbSynchronizeService adbSynchronizeService;

    @BeforeEach
    void setUp(Vertx vertx) {
        adbSynchronizeService = new AdbSynchronizeService(executorDelegate, vertx);
    }

    @Test
    void shouldSuccessWhenAllDestinationsReturnSameDeltaNum(VertxTestContext ctx) {
        // arrange
        Entity entity = Entity.builder()
                .destination(EnumSet.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM))
                .build();
        SynchronizeRequest request = new SynchronizeRequest(UUID.randomUUID(), "dev", "datamart",
                Collections.emptyList(), entity, null, null, null);

        when(executorDelegate.execute(Mockito.any(), Mockito.same(request))).thenReturn(Future.succeededFuture(SUCCEED_DELTA_NUM));

        // act
        Future<Long> future = adbSynchronizeService.execute(request);

        // assert
        future.onComplete(result -> {
            if (!result.succeeded()) {
                ctx.failNow(result.cause());
                return;
            }

            ctx.verify(() -> {
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADB), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADG), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADQM), Mockito.same(request));

                assertEquals(SUCCEED_DELTA_NUM, result.result());
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenOneDestinationReturnDifferentDeltaNum(VertxTestContext ctx) {
        // arrange
        Entity entity = Entity.builder()
                .destination(EnumSet.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM))
                .build();
        SynchronizeRequest request = new SynchronizeRequest(UUID.randomUUID(), "dev", "datamart", null, entity, null, null, null);

        when(executorDelegate.execute(Mockito.any(), Mockito.same(request))).thenReturn(Future.succeededFuture(SUCCEED_DELTA_NUM));
        when(executorDelegate.execute(Mockito.eq(SourceType.ADB), Mockito.same(request))).thenReturn(Future.succeededFuture(FAILED_DELTA_NUM));

        // act
        Future<Long> future = adbSynchronizeService.execute(request);

        // assert
        future.onComplete(result -> {
            if (result.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success"));
                return;
            }

            ctx.verify(() -> {
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADB), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADG), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADQM), Mockito.same(request));

                assertSame(SynchronizeDatasourceException.class, result.cause().getClass());
                assertTrue(result.cause().getMessage().contains("result deltaNum not equal"));
            }).completeNow();
        });
    }

    @Test
    void shouldFailWhenOneDestinationThrowsException(VertxTestContext ctx) {
        // arrange
        Entity entity = Entity.builder()
                .destination(EnumSet.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM))
                .build();
        SynchronizeRequest request = new SynchronizeRequest(UUID.randomUUID(), "dev", "datamart", null, entity, null, null, null);

        when(executorDelegate.execute(Mockito.any(), Mockito.same(request))).thenReturn(Future.succeededFuture(SUCCEED_DELTA_NUM));
        when(executorDelegate.execute(Mockito.eq(SourceType.ADB), Mockito.same(request))).thenReturn(Future.failedFuture(new SynchronizeDatasourceException("Not implemented")));

        // act
        Future<Long> future = adbSynchronizeService.execute(request);

        // assert
        future.onComplete(result -> {
            if (result.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success"));
                return;
            }

            ctx.verify(() -> {
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADB), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADG), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADQM), Mockito.same(request));

                assertSame(SynchronizeDatasourceException.class, result.cause().getClass());
                assertTrue(result.cause().getMessage().contains("Not implemented"));
            }).completeNow();
        });
    }

    @Test
    void shouldNotPanicWhenNoDestinations(VertxTestContext ctx) {
        // arrange
        Entity entity = Entity.builder()
                .destination(Collections.emptySet())
                .build();
        SynchronizeRequest request = new SynchronizeRequest(UUID.randomUUID(), "dev", "datamart", null, entity, null, null, null);


        // act
        Future<Long> future = adbSynchronizeService.execute(request);

        // assert
        future.onComplete(result -> {
            if (result.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success"));
                return;
            }

            ctx.verify(() -> {
                assertSame(SynchronizeDatasourceException.class, result.cause().getClass());
                assertTrue(result.cause().getMessage().contains("result deltaNum not equal"));
            }).completeNow();
        });
    }

}