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

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import lombok.val;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdbConnectionPoolTest {
    @Mock
    private Vertx vertx;
    @Mock
    private AdbConnectionFactory connectionFactory;
    @Mock
    private SqlConnection sqlConnection;
    @Mock
    private Transaction tx;

    private AdbConnectionPool adbConnectionPool;

    private List<AdbConnection> poolMocks = new ArrayList<>();

    @BeforeEach
    void setUp() {
        lenient().when(connectionFactory.createAdbConnection(any())).thenAnswer(i -> getMock());
        lenient().when(sqlConnection.begin()).thenReturn(Future.succeededFuture(tx));
        lenient().when(tx.commit()).thenReturn(Future.succeededFuture());
        lenient().when(tx.rollback()).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldProcessWithConnectionWhenOperationSuccess(VertxTestContext testContext) {
        // arrange
        adbConnectionPool = new AdbConnectionPool(connectionFactory, vertx, 5);
        List<Future> operations = new ArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            operations.add(adbConnectionPool.withConnection(sqlConnection -> Future.succeededFuture(finalI)));
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    List<Integer> list = ar.result().list();
                    Integer[] expectedResult = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
                    assertThat(list, Matchers.contains(expectedResult));

                    for (AdbConnection poolMock : poolMocks) {
                        verify(poolMock, times(2)).acquire();
                        verify(poolMock, times(2)).release();
                    }
                }).completeNow());
    }

    @Test
    void shouldNotReleaseAfterAcquireFailed(VertxTestContext testContext) {
        // arrange
        adbConnectionPool = new AdbConnectionPool(connectionFactory, vertx, 5);

        for (AdbConnection poolMock : poolMocks) {
            reset(poolMock);
            when(poolMock.acquire()).thenReturn(Future.failedFuture("Failed"));
        }

        List<Future> operations = new ArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            operations.add(adbConnectionPool.withConnection(sqlConnection -> Future.succeededFuture(finalI)));
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    for (AdbConnection poolMock : poolMocks) {
                        verify(poolMock, times(2)).acquire();
                        verify(poolMock, times(0)).release();
                    }
                }).completeNow());
    }

    @Test
    void shouldProcessWithConnectionWhenOperationFailed(VertxTestContext testContext) {
        // arrange
        adbConnectionPool = new AdbConnectionPool(connectionFactory, vertx, 5);
        List<Future> operations = new ArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            operations.add(adbConnectionPool.withConnection(sqlConnection -> Future.failedFuture("Exception " + finalI)));
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(ar -> testContext.verify(() -> {
                    for (int i = 0; i < 10; i++) {
                        Future future = operations.get(i);
                        if (future.succeeded()) {
                            fail("Unexpected success");
                        }

                        assertEquals("Exception " + i, future.cause().getMessage());
                    }

                    for (AdbConnection poolMock : poolMocks) {
                        verify(poolMock, times(2)).acquire();
                        verify(poolMock, times(2)).release();
                    }
                }).completeNow());
    }

    @Test
    void shouldProcessWithTransactionLogic(VertxTestContext testContext) {
        // arrange
        adbConnectionPool = new AdbConnectionPool(connectionFactory, vertx, 5);
        List<Future> operations = new ArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            operations.add(adbConnectionPool.withTransaction(sqlConnection -> Future.succeededFuture(finalI)));
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    List<Integer> list = ar.result().list();
                    Integer[] expectedResult = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
                    assertThat(list, Matchers.contains(expectedResult));

                    verify(sqlConnection, times(10)).begin();
                    verify(tx, times(10)).commit();
                    verify(tx, times(0)).rollback();

                    for (AdbConnection poolMock : poolMocks) {
                        verify(poolMock, times(2)).acquire();
                        verify(poolMock, times(2)).release();
                    }
                }).completeNow());
    }

    @Test
    void shouldProcessWithTransactionLogicWhenCommitFailed(VertxTestContext testContext) {
        // arrange
        when(tx.commit()).thenReturn(Future.failedFuture("Failed to commit"));
        adbConnectionPool = new AdbConnectionPool(connectionFactory, vertx, 5);
        List<Future> operations = new ArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            operations.add(adbConnectionPool.withTransaction(sqlConnection -> Future.succeededFuture(finalI)));
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(ar -> testContext.verify(() -> {
                    for (int i = 0; i < 10; i++) {
                        Future future = operations.get(i);
                        if (future.succeeded()) {
                            fail("Unexpected success");
                        }

                        assertEquals("Failed to commit", future.cause().getMessage());
                    }

                    verify(sqlConnection, times(10)).begin();
                    verify(tx, times(10)).commit();
                    verify(tx, times(0)).rollback();

                    for (AdbConnection poolMock : poolMocks) {
                        verify(poolMock, times(2)).acquire();
                        verify(poolMock, times(2)).release();
                    }
                }).completeNow());
    }

    @Test
    void shouldProcessWithTransactionLogicWhenOperationFailed(VertxTestContext testContext) {
        // arrange
        adbConnectionPool = new AdbConnectionPool(connectionFactory, vertx, 5);
        List<Future> operations = new ArrayList<>();

        // act
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            operations.add(adbConnectionPool.withTransaction(sqlConnection -> Future.failedFuture("Exception " + finalI)));
        }

        // assert
        CompositeFuture.join(operations)
                .onComplete(ar -> testContext.verify(() -> {
                    for (int i = 0; i < 10; i++) {
                        Future future = operations.get(i);
                        if (future.succeeded()) {
                            fail("Unexpected success");
                        }

                        assertEquals("Exception " + i, future.cause().getMessage());
                    }

                    verify(sqlConnection, times(10)).begin();
                    verify(tx, times(0)).commit();
                    verify(tx, times(10)).rollback();

                    for (AdbConnection poolMock : poolMocks) {
                        verify(poolMock, times(2)).acquire();
                        verify(poolMock, times(2)).release();
                    }
                }).completeNow());
    }

    private AdbConnection getMock() {
        val adbConnection = mock(AdbConnection.class);
        when(adbConnection.acquire()).thenReturn(Future.succeededFuture(sqlConnection));
        poolMocks.add(adbConnection);
        return adbConnection;
    }
}