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
package ru.datamart.prostore.query.execution.core.delta.repository.executor;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import ru.datamart.prostore.serialization.CoreSerialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class WriteOperationErrorExecutorTest {
    private static final String ENV = "ENV";
    private static final String DATAMART = "DATAMART";

    @Mock
    private ZookeeperExecutor zookeeperExecutor;

    private WriteOperationErrorExecutor executor;

    @Captor
    private ArgumentCaptor<byte[]> byteCaptor;

    @BeforeEach
    void setUp() {
        executor = new WriteOperationErrorExecutor(zookeeperExecutor, ENV);
    }

    @Test
    void shouldSuccess(VertxTestContext testContext) {
        // arrange
        val deltaHot = HotDelta.builder()
                .build();
        val delta = new Delta(deltaHot, null);
        when(zookeeperExecutor.getData("/ENV/DATAMART/delta")).thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        val deltaWriteOp = DeltaWriteOp.builder()
                .status(WriteOperationStatus.EXECUTING.getValue())
                .tableName("table")
                .sysCn(0L)
                .cnFrom(0)
                .query("query")
                .build();
        when(zookeeperExecutor.getData("/ENV/DATAMART/run/0000000000")).thenReturn(Future.succeededFuture(CoreSerialization.serialize(deltaWriteOp)));

        when(zookeeperExecutor.setData(Mockito.any(), byteCaptor.capture(), Mockito.anyInt())).thenReturn(Future.succeededFuture());

        // act
        executor.execute(DATAMART, 0L)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val resultWriteOp = CoreSerialization.deserialize(byteCaptor.getValue(), DeltaWriteOp.class);
                    assertEquals(0, resultWriteOp.getCnFrom());
                    assertEquals(0L, resultWriteOp.getSysCn());
                    assertEquals("table", resultWriteOp.getTableName());
                    assertEquals("query", resultWriteOp.getQuery());
                    assertEquals(WriteOperationStatus.ERROR.getValue(), resultWriteOp.getStatus());
                }).completeNow());
    }

    @Test
    void shouldFailWhenNoDeltaHot(VertxTestContext testContext) {
        // arrange
        val delta = new Delta(null, null);
        when(zookeeperExecutor.getData("/ENV/DATAMART/delta")).thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        // act
        executor.execute(DATAMART, 0L)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Can't write operation \"error\" on datamart[DATAMART], sysCn[0]: Delta hot not exists", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenDeserializeException(VertxTestContext testContext) {
        // arrange
        when(zookeeperExecutor.getData("/ENV/DATAMART/delta")).thenReturn(Future.succeededFuture(new byte[0]));

        // act
        executor.execute(DATAMART, 0L)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertThat(ar.cause().getMessage()).contains("Can't deserialize Delta");
                }).completeNow());
    }

    @Test
    void shouldFailWhenNoNodeException(VertxTestContext testContext) {
        // arrange
        val deltaHot = HotDelta.builder()
                .build();
        val delta = new Delta(deltaHot, null);
        when(zookeeperExecutor.getData("/ENV/DATAMART/delta")).thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        when(zookeeperExecutor.getData("/ENV/DATAMART/run/0000000000")).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        // act
        executor.execute(DATAMART, 0L)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("write op not found: KeeperErrorCode = NoNode", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldPackUnexpectedException(VertxTestContext testContext) {
        // arrange
        when(zookeeperExecutor.getData("/ENV/DATAMART/delta")).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        // act
        executor.execute(DATAMART, 0L)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Can't write operation \"error\" on datamart[DATAMART], sysCn[0]: Exception", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldBeCorrectClass() {
        assertSame(executor.getExecutorInterface(), WriteOperationErrorExecutor.class);
    }
}