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
import org.apache.zookeeper.OpResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOpRequest;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaClosedException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.delta.exception.TableBlockedException;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class WriteNewOperationExecutorTest {

    private static final String ENV = "test";
    private static final String DATAMART = "dm_test";
    private static final String DELTA_PATH = "/" + ENV + "/" + DATAMART + "/delta";
    private static final Long DELTA_HOT_TEN = 10L;
    private static final Class<WriteNewOperationExecutor> EXPECTED_EXECUTOR_IFACE = WriteNewOperationExecutor.class;

    private final ZookeeperExecutor executor = mock(ZookeeperExecutor.class);
    private DeltaWriteOpRequest deltaWriteOpRequest;

    private final WriteNewOperationExecutor operationExecutor = new WriteNewOperationExecutor(executor, ENV);

    @BeforeEach
    void setUp() {
        deltaWriteOpRequest = DeltaWriteOpRequest.builder()
                .datamart(DATAMART)
                .tableName("test_table")
                .build();
    }

    @Test
    void shouldSuccessWhenHotDeltaIsNotNull(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH))).thenReturn(Future.succeededFuture(CoreSerialization.serialize(getHotDelta())));
        when(executor.multi(any())).thenReturn(Future.succeededFuture(getCreateOpResult()));

        operationExecutor.execute(deltaWriteOpRequest)
                .onComplete(
                        ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow()
                );
    }

    @Test
    void shouldFailWhenDeltaHotIsNull(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH))).thenReturn(Future.succeededFuture(CoreSerialization.serialize(getOkDelta())));

        operationExecutor.execute(deltaWriteOpRequest)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaClosedException.class, ar.cause().getClass());
                            assertEquals("Delta closed", ar.cause().getMessage());
                        }).completeNow()
                );
    }

    @Test
    void shouldFailWhenGetDataReturnNullBytes(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH))).thenReturn(Future.succeededFuture(null));
        when(executor.multi(any())).thenReturn(Future.succeededFuture());

        operationExecutor.execute(deltaWriteOpRequest)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertTrue(ar.cause().getMessage().contains("Can't deserialize Delta: Can't deserialize bytes"));
                        }).completeNow()
                );
    }

    @Test
    void shouldFailWhenIncorrectCreateResultPath(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH))).thenReturn(Future.succeededFuture(CoreSerialization.serialize(getHotDelta())));
        when(executor.multi(any())).thenReturn(Future.succeededFuture(getCreateOpResultWithIncorrectPath()));

        operationExecutor.execute(deltaWriteOpRequest)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertTrue(ar.cause().getMessage().contains("Can't get op number"));
                        }).completeNow()
                );
    }

    @Test
    void shouldFailWhenExceptionInSequentialNodeCreation(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH))).thenReturn(Future.succeededFuture(CoreSerialization.serialize(getHotDelta())));
        when(executor.multi(any())).thenReturn(Future.succeededFuture(anyList()));

        operationExecutor.execute(deltaWriteOpRequest)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertTrue(ar.cause().getMessage().contains("Can't create sequential op node"));
                        }).completeNow()
                );
    }

    @Test
    void shouldFailWhenNodeExists(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH))).thenReturn(Future.failedFuture(new KeeperException.NodeExistsException()));

        operationExecutor.execute(deltaWriteOpRequest)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(TableBlockedException.class, ar.cause().getClass());
                            assertTrue(ar.cause().getMessage().contains("Table[test_table] blocked"));
                        }).completeNow()
                );
    }

    @Test
    void shouldFailWhenUnexpectedError(VertxTestContext testContext) {
        when(executor.getData(eq(DELTA_PATH))).thenReturn(Future.failedFuture(new RuntimeException()));

        operationExecutor.execute(deltaWriteOpRequest)
                .onComplete(
                        ar -> testContext.verify(() -> {
                            assertTrue(ar.failed());
                            assertEquals(DeltaException.class, ar.cause().getClass());
                            assertTrue(ar.cause().getMessage().contains("Can't write new operation on datamart"));
                        }).completeNow()
                );
    }

    @Test
    void shouldReturnCorrectInterface() {
        assertEquals(EXPECTED_EXECUTOR_IFACE, operationExecutor.getExecutorInterface());
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

    private List<OpResult> getCreateOpResult() {
        return Arrays.asList(new OpResult.CreateResult("/1"), new OpResult.CreateResult("/2"));
    }

    private List<OpResult> getCreateOpResultWithIncorrectPath() {
        return Arrays.asList(new OpResult.CreateResult("/error"), new OpResult.CreateResult("/error"));
    }

}