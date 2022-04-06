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
package ru.datamart.prostore.query.execution.core.delta.repository.executor;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.*;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class WriteDeltaHotSuccessExecutorTest {

    private static final String ENV = "test";
    private static final String DATAMART = "dm_test";
    private static final String DELTA_PATH = "/" + ENV + "/" + DATAMART + "/delta";
    private static final String DATE_PATH = String.format("/%s/%s/delta/date/2022-01-01/01:00:59", ENV, DATAMART);
    private static final String NUM_PATH = String.format("/%s/%s/delta/num/0", ENV, DATAMART);
    private static final Long DELTA_HOT_TEN = 10L;
    private static final Class<WriteDeltaHotSuccessExecutor> EXPECTED_EXECUTOR_IFACE = WriteDeltaHotSuccessExecutor.class;

    private static final LocalDateTime DELTA_DATE = LocalDateTime.of(2022, 1, 1, 1, 1, 1);
    private static final LocalDateTime DELTA_OK_DATE = LocalDateTime.of(2022, 1, 1, 1, 0, 59);

    private static final String CANT_WRITE_DELTA_HOT_MSG = "Can't write delta hot success";

    private final ZookeeperExecutor executor = mock(ZookeeperExecutor.class);
    private final WriteDeltaHotSuccessExecutor successExecutor = new WriteDeltaHotSuccessExecutor(executor, ENV);

    @Test
    void shouldSuccessWhenOkDeltaIsNull(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(getDeltaWithNullOkDelta())));
        when(executor.multi(any())).thenReturn(Future.succeededFuture());

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow()
                );
    }

    @Test
    void shouldFailWhenGetDataReturnNullBytes(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(null));
        when(executor.multi(any())).thenReturn(Future.succeededFuture());

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaIsAlreadyCommittedException.class, ar.cause().getClass());
                            assertEquals("Delta is already commited", ar.cause().getMessage());
                        }).completeNow()
                );
    }

    @Test
    void shouldFailWhenNotEmptyException(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.failedFuture(new KeeperException.NotEmptyException()));

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaNotFinishedException.class, ar.cause().getClass());
                            assertEquals("not finished write operation exist: KeeperErrorCode = Directory not empty", ar.cause().getMessage());
                        }).completeNow()
                );
    }


    @Test
    void shouldFailWhenBadVersion(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.failedFuture(new KeeperException.BadVersionException()));

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaAlreadyCommitedException.class, ar.cause().getClass());
                            assertEquals("Delta already commited: KeeperErrorCode = BadVersion", ar.cause().getMessage());
                        }).completeNow()
                );
    }

    @Test
    void shouldFailWhenOtherKeeperException(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.failedFuture(new KeeperException.DataInconsistencyException()));

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertTrue(ar.cause().getMessage().contains("Can't write delta hot \"success\" by datamart[dm_test], deltaDate[2022-01-01T01:01:01]:"));
                        }).completeNow()
                );
    }

    @Test
    void shouldFailWhenUnexpectedException(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.failedFuture(new RuntimeException()));

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertTrue(ar.cause().getMessage().contains("Can't write delta hot \"success\" by datamart[dm_test]"));
                        }).completeNow()
                );
    }

    @Test
    void shouldFailWhenDeltaHotDateBeforeOrEqualDeltaDate(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(getFullDeltaWithEqualDeltaDate())));

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaUnableSetDateTimeException.class, ar.cause().getClass());
                            assertEquals("Unable to set the date-time 2022-01-01 01:01:01 preceding or equal the actual delta 2022-01-01 01:01:01", ar.cause().getMessage());
                        }).completeNow()
                );
    }

    @Test
    void shouldSuccessWithFullDelta(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(getFullDeltaWithCorrectTime())));
        when(executor.createEmptyPersistentPath(any())).thenReturn(Future.succeededFuture(""));
        when(executor.createPersistentPath(any(), any())).thenReturn(Future.succeededFuture(""));
        when(executor.multi(any())).thenReturn(Future.succeededFuture());

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow()
                );
    }

    @Test
    void shouldSuccessWhenNodeExists(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(getFullDeltaWithCorrectTime())));
        when(executor.createEmptyPersistentPath(any())).thenReturn(Future.failedFuture(new KeeperException.NodeExistsException()));
        when(executor.createPersistentPath(any(), any())).thenReturn(Future.failedFuture(new KeeperException.NodeExistsException()));
        when(executor.multi(any())).thenReturn(Future.succeededFuture());

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow()
                );
    }

    @Test
    void shouldFailWhenExceptionInDeltaDatePathCreation(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(getFullDeltaWithCorrectTime())));

        when(executor.createEmptyPersistentPath(any())).thenReturn(Future.failedFuture(new RuntimeException()));

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertTrue(ar.cause().getMessage().contains(CANT_WRITE_DELTA_HOT_MSG));
                        }).completeNow()
                );
    }

    @Test
    void shouldFailWhenExceptionInDeltaDateTimePathCreation(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(getFullDeltaWithCorrectTime())));

        when(executor.createEmptyPersistentPath(any())).thenReturn(Future.succeededFuture(""));
        when(executor.createPersistentPath(eq(DATE_PATH), any())).thenReturn(Future.failedFuture(new RuntimeException()));

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertTrue(ar.cause().getMessage().contains(CANT_WRITE_DELTA_HOT_MSG));
                        }).completeNow()
                );
    }

    @Test
    void shouldFailWhenExceptionInDeltaDateNumPathCreation(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(getFullDeltaWithCorrectTime())));

        when(executor.createEmptyPersistentPath(any())).thenReturn(Future.succeededFuture(""));
        when(executor.createPersistentPath(eq(DATE_PATH), any())).thenReturn(Future.succeededFuture(""));
        when(executor.createPersistentPath(eq(NUM_PATH), any())).thenReturn(Future.failedFuture(new RuntimeException()));

        successExecutor.execute(DATAMART, DELTA_DATE)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertTrue(ar.cause().getMessage().contains(CANT_WRITE_DELTA_HOT_MSG));
                        }).completeNow()
                );
    }

    @Test
    void shouldReturnCorrectInterface() {
        assertEquals(EXPECTED_EXECUTOR_IFACE, successExecutor.getExecutorInterface());
    }

    private Delta getDeltaWithNullOkDelta() {
        val hotDelta = HotDelta.builder()
                .deltaNum(DELTA_HOT_TEN)
                .build();
        return new Delta(hotDelta, null);
    }

    private Delta getFullDeltaWithEqualDeltaDate() {
        val hotDelta = HotDelta.builder()
                .deltaNum(DELTA_HOT_TEN)
                .build();
        val okDelta = OkDelta.builder()
                .deltaDate(DELTA_DATE)
                .build();
        return new Delta(hotDelta, okDelta);
    }

    private Delta getFullDeltaWithCorrectTime() {
        val hotDelta = HotDelta.builder()
                .deltaNum(DELTA_HOT_TEN)
                .build();
        val okDelta = OkDelta.builder()
                .deltaDate(DELTA_OK_DATE)
                .build();
        return new Delta(hotDelta, okDelta);
    }

}