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
package ru.datamart.prostore.query.execution.plugin.adg.db.verticle;

import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import ru.datamart.prostore.query.execution.plugin.adg.db.service.AdgClientProvider;
import ru.datamart.prostore.query.execution.plugin.adg.db.service.AdgResultTranslator;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientOps;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdgQueryExecutorVerticleTest {
    private static final String SQL = "select * from tbl";
    private static final String QUERY_CALL = "query";
    private static final List<Object> NOT_TRANSLATED_RESULT = new ArrayList<>();
    private static final List<Object> TRANSLATED_RESULT = new ArrayList<>();

    @Mock
    private TarantoolDatabaseProperties tarantoolDatabaseProperties;
    @Mock
    private AdgClientProvider adgClientProvider;
    @Mock
    private AdgResultTranslator resultTranslator;
    @Mock
    private TarantoolClient adgClient;
    @Mock
    private TarantoolClientOps<Integer, List<?>, Object, CompletionStage<List<?>>> adgAsyncOps;
    @InjectMocks
    private AdgQueryExecutorVerticle adgQueryExecutorVerticle;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        lenient().when(tarantoolDatabaseProperties.getVertxWorkers()).thenReturn(10);
        lenient().when(adgClientProvider.getClient()).thenReturn(adgClient);
        lenient().when(adgClient.composableAsyncOps()).thenReturn(adgAsyncOps);
        lenient().when(adgAsyncOps.call(Mockito.any(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(NOT_TRANSLATED_RESULT));
        lenient().when(resultTranslator.translate(Mockito.any())).thenReturn(TRANSLATED_RESULT);

        vertx.deployVerticle(adgQueryExecutorVerticle, event -> {
            if (event.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(event.cause());
            }
        });
    }

    @Test
    void shouldSuccessWhenNoParams(VertxTestContext testContext) {
        // act
        adgQueryExecutorVerticle.callQuery(SQL, null)
                .onComplete(ar -> {
                    // assert
                    if (ar.failed()) {
                        testContext.failNow(ar.cause());
                        return;
                    }

                    testContext.verify(() -> {
                        assertSame(TRANSLATED_RESULT, ar.result());

                        verify(adgClientProvider).getClient();
                        verify(adgClient).composableAsyncOps();
                        verify(adgAsyncOps).call(Mockito.eq(QUERY_CALL), Mockito.eq(SQL));
                        verify(resultTranslator).translate(Mockito.same(NOT_TRANSLATED_RESULT));
                    }).completeNow();
                });
    }

    @Test
    void shouldSuccessWhenHasParams(VertxTestContext testContext) {
        // arrange
        Object[] params = {1, 2, 3};

        // act
        adgQueryExecutorVerticle.callQuery(SQL, params)
                .onComplete(ar -> {
                    // assert
                    if (ar.failed()) {
                        testContext.failNow(ar.cause());
                        return;
                    }

                    testContext.verify(() -> {
                        assertSame(TRANSLATED_RESULT, ar.result());

                        verify(adgClientProvider).getClient();
                        verify(adgClient).composableAsyncOps();
                        verify(adgAsyncOps).call(Mockito.eq(QUERY_CALL), Mockito.eq(SQL), Mockito.same(params));
                        verify(resultTranslator).translate(Mockito.same(NOT_TRANSLATED_RESULT));
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenGettingClientFailed(VertxTestContext testContext) {
        // arrange
        reset(adgClientProvider);
        when(adgClientProvider.getClient()).thenThrow(new IllegalStateException("Can't get client"));

        // act
        adgQueryExecutorVerticle.callQuery(SQL, null)
                .onComplete(ar -> {
                    // assert
                    if (ar.succeeded()) {
                        testContext.failNow("Unexpected success");
                        return;
                    }

                    testContext.verify(() -> {
                        assertSame(IllegalStateException.class, ar.cause().getClass());
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenAsyncOpsFailed(VertxTestContext testContext) {
        // arrange
        reset(adgClient);
        when(adgClient.composableAsyncOps()).thenThrow(new IllegalStateException("Can't get async ops"));

        // act
        adgQueryExecutorVerticle.callQuery(SQL, null)
                .onComplete(ar -> {
                    // assert
                    if (ar.succeeded()) {
                        testContext.failNow("Unexpected success");
                        return;
                    }

                    testContext.verify(() -> {
                        assertSame(IllegalStateException.class, ar.cause().getClass());
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenCallFailed(VertxTestContext testContext) {
        // arrange
        reset(adgAsyncOps);
        when(adgAsyncOps.call(Mockito.any(), Mockito.any())).thenThrow(new IllegalStateException("Can't call"));

        // act
        adgQueryExecutorVerticle.callQuery(SQL, null)
                .onComplete(ar -> {
                    // assert
                    if (ar.succeeded()) {
                        testContext.failNow("Unexpected success");
                        return;
                    }

                    testContext.verify(() -> {
                        assertSame(IllegalStateException.class, ar.cause().getClass());
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenCallFutureFailed(VertxTestContext testContext) {
        // arrange
        reset(adgAsyncOps);
        CompletableFuture<List<?>> exceptionFuture = new CompletableFuture<>();
        exceptionFuture.completeExceptionally(new IllegalStateException("Failed future"));
        when(adgAsyncOps.call(Mockito.any(), Mockito.any())).thenReturn(exceptionFuture);

        // act
        adgQueryExecutorVerticle.callQuery(SQL, null)
                .onComplete(ar -> {
                    // assert
                    if (ar.succeeded()) {
                        testContext.failNow("Unexpected success");
                        return;
                    }

                    testContext.verify(() -> {
                        assertSame(DataSourceException.class, ar.cause().getClass());
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenTranslateFailed(VertxTestContext testContext) {
        // arrange
        reset(resultTranslator);
        lenient().when(resultTranslator.translate(Mockito.any())).thenThrow(new IllegalStateException("Failed translate"));

        // act
        adgQueryExecutorVerticle.callQuery(SQL, null)
                .onComplete(ar -> {
                    // assert
                    if (ar.succeeded()) {
                        testContext.failNow("Unexpected success");
                        return;
                    }

                    testContext.verify(() -> {
                        assertSame(IllegalStateException.class, ar.cause().getClass());
                    }).completeNow();
                });
    }
}