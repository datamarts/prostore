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
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaIsNotCommittedException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaNumIsNotNextToActualException;
import ru.datamart.prostore.serialization.CoreSerialization;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class WriteNewDeltaHotExecutorTest {

    private static final String ENV = "test";
    private static final String DATAMART = "dm_test";
    private static final String DELTA_PATH = "/" + ENV + "/" + DATAMART + "/delta";
    private static final Long DELTA_HOT_ZERO = 0L;
    private static final Long DELTA_HOT_TEN = 10L;
    private static final Class<WriteNewDeltaHotExecutor> EXPECTED_EXECUTOR_IFACE = WriteNewDeltaHotExecutor.class;

    private static final String NOT_COMMITTED = "The delta 10 is not committed.";
    private final ZookeeperExecutor executor = mock(ZookeeperExecutor.class);

    private final WriteNewDeltaHotExecutor deltaHotExecutor = new WriteNewDeltaHotExecutor(executor, ENV);

    @Test
    public void shouldSuccessWhenGetDataReturnNullBytes(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class))).thenReturn(Future.succeededFuture(null));
        when(executor.multi(any())).thenReturn(Future.succeededFuture());

        deltaHotExecutor.execute(DATAMART, DELTA_HOT_ZERO)
                .onComplete(
                        ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow()
                );
    }

    @Test
    public void shouldSuccessWhenGetDataReturnDelta(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(getOkDelta())));
        when(executor.multi(any())).thenReturn(Future.succeededFuture());

        deltaHotExecutor.execute(DATAMART, null)
                .onComplete(
                        ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow()
                );
    }

    @Test
    public void shouldFailWhenGetDataReturnNullBytesAndDeltaNumNotZero(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class))).thenReturn(Future.succeededFuture(null));
        when(executor.multi(any())).thenReturn(Future.succeededFuture());

        deltaHotExecutor.execute(DATAMART, DELTA_HOT_TEN)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaNumIsNotNextToActualException.class, ar.cause().getClass());
                            assertEquals("The delta number 10 is not next to an actual delta", ar.cause().getMessage());
                        }).completeNow()
                );
    }

    @Test
    public void shouldFailWhenDeltaIsNotCommitted(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(getHotDelta())));
        when(executor.multi(any())).thenReturn(Future.succeededFuture());

        deltaHotExecutor.execute(DATAMART, DELTA_HOT_TEN)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaIsNotCommittedException.class, ar.cause().getClass());
                            assertEquals(NOT_COMMITTED, ar.cause().getMessage());
                        }).completeNow()
                );
    }

    @Test
    public void shouldFailWhenNodeExists(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.failedFuture(new KeeperException.NodeExistsException()));

        deltaHotExecutor.execute(DATAMART, DELTA_HOT_TEN)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaIsNotCommittedException.class, ar.cause().getClass());
                            assertEquals(NOT_COMMITTED + ": KeeperErrorCode = NodeExists", ar.cause().getMessage());
                        }).completeNow()
                );
    }

    @Test
    public void shouldFailWhenUnexpectedException(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.failedFuture(new RuntimeException()));

        deltaHotExecutor.execute(DATAMART, DELTA_HOT_TEN)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertEquals("Can't write new delta hot on datamart[dm_test], deltaHotNumber[10]: null", ar.cause().getMessage());
                        }).completeNow()
                );
    }


    @Test
    void shouldReturnCorrectInterface() {
        assertEquals(EXPECTED_EXECUTOR_IFACE, deltaHotExecutor.getExecutorInterface());
    }

    private Delta getHotDelta() {
        val hotDelta = HotDelta.builder()
                .deltaNum(DELTA_HOT_TEN)
                .build();
        return new Delta(hotDelta, null);
    }

    private Delta getOkDelta() {
        val okDelta = OkDelta.builder().build();
        return new Delta(null, okDelta);
    }
}