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
package ru.datamart.prostore.query.execution.plugin.adb.query.service.pool;

import ru.datamart.prostore.query.execution.plugin.adb.base.configuration.properties.AdbProperties;
import io.vertx.core.*;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.SqlConnection;
import lombok.val;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdbConnectionTest {

    @Mock
    private AdbConnectionFactory connectionFactory;
    @Mock
    private PgConnection pgConnection;
    @Mock
    private AdbProperties adbProperties;

    @Captor
    private ArgumentCaptor<Handler<Void>> closeHandlerCaptor;

    private AdbConnection adbConnection;

    @BeforeEach
    void setUp(Vertx vertx) {
        lenient().when(connectionFactory.createPgConnection(any())).thenReturn(Future.succeededFuture(pgConnection));
        lenient().when(pgConnection.close()).thenReturn(Future.succeededFuture());
        lenient().when(adbProperties.getMaxReconnections()).thenReturn(1);
        lenient().when(adbProperties.getQueriesByConnectLimit()).thenReturn(100);
        lenient().when(adbProperties.getReconnectionInterval()).thenReturn(0);
        lenient().when(pgConnection.closeHandler(closeHandlerCaptor.capture())).thenReturn(pgConnection);

        adbConnection = new AdbConnection(connectionFactory, vertx, adbProperties);
    }

    @Test
    void shouldQueueAllTasksAndRunThemInSameOrderAfterRelease(VertxTestContext testContext) {
        // arrange
        List<Integer> completed = new CopyOnWriteArrayList<>();
        List<Future> operations = new ArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            Future<SqlConnection> operation = adbConnection.acquire()
                    .onSuccess(event -> completed.add(finalI));
            operations.add(operation);
        }

        for (val operation : operations) {
            operation.onComplete(i -> adbConnection.release());
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(connectionFactory).createPgConnection(any());

                    Integer[] expectedOrder = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
                    assertThat(completed, Matchers.contains(expectedOrder)); // assert correct order
                }).completeNow());
    }

    @Test
    void shouldQueueAllTasksAndRunThemInSameOrderAndReconnectOnTheMiddle(VertxTestContext testContext) {
        // arrange
        List<Integer> completed = new CopyOnWriteArrayList<>();
        List<Future> operations = new ArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;

            if (i == 5) {
                closeHandlerCaptor.getValue().handle(null);
            }

            Future<SqlConnection> operation = adbConnection.acquire()
                    .onSuccess(event -> completed.add(finalI));
            operations.add(operation);
        }

        for (val operation : operations) {
            operation.onComplete(i -> adbConnection.release());
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(connectionFactory, times(2)).createPgConnection(any());

                    Integer[] expectedOrder = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
                    assertThat(completed, Matchers.contains(expectedOrder)); // assert correct order
                }).completeNow());
    }

    @Test
    void shouldQueueAllTasksSequentiallyWithoutQueue(VertxTestContext testContext, Vertx vertx) {
        // arrange
        List<Integer> completed = new CopyOnWriteArrayList<>();

        // act
        Promise<Void> promise = Promise.promise();
        vertx.setTimer(50L, t0 -> {
            adbConnection.acquire()
                    .onSuccess(i -> completed.add(0))
                    .onComplete(i0 -> {
                        adbConnection.release();
                        vertx.setTimer(50L, t1 -> {
                            adbConnection.acquire()
                                    .onSuccess(i -> completed.add(1))
                                    .onComplete(i1 -> {
                                        adbConnection.release();
                                        vertx.setTimer(50L, t2 -> {
                                            adbConnection.acquire()
                                                    .onSuccess(i -> completed.add(2))
                                                    .onComplete(i2 -> {
                                                        adbConnection.release();
                                                        promise.complete();
                                                    });
                                        });
                                    });
                        });
                    });
        });

        // assert
        promise.future().onComplete(ar -> testContext.verify(() -> {
            verify(connectionFactory).createPgConnection(any());

            Integer[] expectedOrder = {0, 1, 2};
            assertThat(completed, Matchers.contains(expectedOrder)); // assert correct order
        }).completeNow());
    }

    @Test
    void shouldQueueAllTasksAndRunThemInSameOrderWithReconnections(VertxTestContext testContext) {
        // arrange
        when(adbProperties.getQueriesByConnectLimit()).thenReturn(3);
        List<Integer> completed = new CopyOnWriteArrayList<>();
        List<Future> operations = new ArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            Future<SqlConnection> operation = adbConnection.acquire()
                    .onSuccess(event -> completed.add(finalI));
            operations.add(operation);
        }

        for (val operation : operations) {
            operation.onComplete(i -> adbConnection.release());
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(connectionFactory, times(4)).createPgConnection(any());
                    verify(pgConnection, times(3)).close();

                    Integer[] expectedOrder = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
                    assertThat(completed, Matchers.contains(expectedOrder)); // assert correct order
                }).completeNow());
    }

    @Test
    void shouldFailWhenCouldNotReconnectOnInit(VertxTestContext testContext, Vertx vertx) {
        // arrange
        reset(connectionFactory);
        when(connectionFactory.createPgConnection(any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));
        when(adbProperties.getMaxReconnections()).thenReturn(5);
        adbConnection = new AdbConnection(connectionFactory, vertx, adbProperties);
        List<Integer> completed = new CopyOnWriteArrayList<>();
        List<Future> operations = new CopyOnWriteArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            Future<SqlConnection> operation = adbConnection.acquire()
                    .onComplete(event -> completed.add(finalI));
            operations.add(operation);
        }

        for (val operation : operations) {
            operation.onComplete(i -> adbConnection.release());
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(event -> testContext.verify(() -> {
                    for (Future operation : operations) {
                        if (operation.succeeded()) {
                            fail("Unexpected success");
                        }

                        assertEquals("ADB connection in failed state and can't be restored", operation.cause().getMessage());
                    }

                    verify(connectionFactory, times(5)).createPgConnection(any());
                }).completeNow());
    }

    @Test
    void shouldQueueAllTasksSequentiallyNotReconnectOnInit(VertxTestContext testContext, Vertx vertx) {
        // arrange
        reset(connectionFactory);
        when(connectionFactory.createPgConnection(any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));
        when(adbProperties.getMaxReconnections()).thenReturn(5);
        adbConnection = new AdbConnection(connectionFactory, vertx, adbProperties);
        List<Integer> completed = new CopyOnWriteArrayList<>();

        // act
        Promise<Void> promise = Promise.promise();
        vertx.setTimer(50L, t0 -> {
            adbConnection.acquire()
                    .onSuccess(i -> completed.add(0))
                    .onComplete(i0 -> {
                        adbConnection.release();
                        vertx.setTimer(50L, t1 -> {
                            adbConnection.acquire()
                                    .onSuccess(i -> completed.add(1))
                                    .onComplete(i1 -> {
                                        adbConnection.release();
                                        vertx.setTimer(50L, t2 -> {
                                            adbConnection.acquire()
                                                    .onSuccess(i -> completed.add(2))
                                                    .onComplete(i2 -> {
                                                        adbConnection.release();
                                                        promise.complete();
                                                    });
                                        });
                                    });
                        });
                    });
        });

        // assert
        promise.future().onComplete(ar -> testContext.verify(() -> {
            assertTrue(completed.isEmpty());
            verify(connectionFactory, times(5)).createPgConnection(any());
        }).completeNow());
    }

    @Test
    void shouldQueueAndCompleteThreeTasksAndFailOnReconnectWithInterval(VertxTestContext testContext, Vertx vertx) {
        // arrange
        reset(connectionFactory);
        when(connectionFactory.createPgConnection(any()))
                .thenReturn(Future.succeededFuture(pgConnection))
                .thenReturn(Future.failedFuture(new RuntimeException("Exception")));
        when(adbProperties.getMaxReconnections()).thenReturn(5);
        when(adbProperties.getQueriesByConnectLimit()).thenReturn(3);
        when(adbProperties.getReconnectionInterval()).thenReturn(10);
        adbConnection = new AdbConnection(connectionFactory, vertx, adbProperties);

        List<Integer> completed = new CopyOnWriteArrayList<>();
        List<Future> operations = new ArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            Future<SqlConnection> operation = adbConnection.acquire()
                    .onSuccess(event -> completed.add(finalI));
            operations.add(operation);
        }

        for (val operation : operations) {
            operation.onComplete(i -> adbConnection.release());
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(ar -> testContext.verify(() -> {

                    // should complete tasks before reconnect
                    for (int i = 0; i < operations.size(); i++) {
                        Future operation = operations.get(i);
                        if (i < 3) {
                            if (operation.failed()) {
                                fail(operation.cause());
                            }
                        } else {
                            if (operation.succeeded()) {
                                fail("Unexpected success");
                            }

                            assertEquals("ADB connection in failed state and can't be restored", operation.cause().getMessage());
                        }
                    }

                    verify(connectionFactory, times(6)).createPgConnection(any());
                    verify(pgConnection, times(1)).close();

                    Integer[] expectedOrder = {0, 1, 2};
                    assertThat(completed, Matchers.contains(expectedOrder)); // assert correct order
                }).completeNow());
    }

    @Test
    void shouldQueueAndCompleteThreeTasksAndFailOnReconnectWithoutInterval(VertxTestContext testContext, Vertx vertx) {
        // arrange
        reset(connectionFactory);
        when(connectionFactory.createPgConnection(any()))
                .thenReturn(Future.succeededFuture(pgConnection))
                .thenReturn(Future.failedFuture(new RuntimeException("Exception")));
        when(adbProperties.getMaxReconnections()).thenReturn(5);
        when(adbProperties.getQueriesByConnectLimit()).thenReturn(3);
        when(adbProperties.getReconnectionInterval()).thenReturn(0);
        adbConnection = new AdbConnection(connectionFactory, vertx, adbProperties);

        List<Integer> completed = new CopyOnWriteArrayList<>();
        List<Future> operations = new ArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            Future<SqlConnection> operation = adbConnection.acquire()
                    .onSuccess(event -> completed.add(finalI));
            operations.add(operation);
        }

        for (val operation : operations) {
            operation.onComplete(i -> adbConnection.release());
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(ar -> testContext.verify(() -> {

                    // should complete tasks before reconnect
                    for (int i = 0; i < operations.size(); i++) {
                        Future operation = operations.get(i);
                        if (i < 3) {
                            if (operation.failed()) {
                                fail(operation.cause());
                            }
                        } else {
                            if (operation.succeeded()) {
                                fail("Unexpected success");
                            }

                            assertEquals("ADB connection in failed state and can't be restored", operation.cause().getMessage());
                        }
                    }

                    verify(connectionFactory, times(6)).createPgConnection(any());
                    verify(pgConnection, times(1)).close();

                    Integer[] expectedOrder = {0, 1, 2};
                    assertThat(completed, Matchers.contains(expectedOrder)); // assert correct order
                }).completeNow());
    }
}