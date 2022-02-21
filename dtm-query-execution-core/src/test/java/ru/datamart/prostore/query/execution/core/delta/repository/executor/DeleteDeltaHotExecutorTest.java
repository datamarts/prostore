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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;


@ExtendWith({MockitoExtension.class, VertxExtension.class})
class DeleteDeltaHotExecutorTest {
    private static final LocalDateTime CURRENT_LOCAL_DATE_TIME = LocalDateTime.of(2020, 1, 10, 11, 11, 11);
    private static final String ENV = "ENV";
    private static final String DATAMART = "DATAMART";

    @Mock
    private ZookeeperExecutor zookeeperExecutor;

    @Captor
    private ArgumentCaptor<byte[]> byteCaptor;

    private DeleteDeltaHotExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new DeleteDeltaHotExecutor(zookeeperExecutor, ENV);
    }

    @Test
    void shouldDeleteDeltaHot(VertxTestContext testContext) {
        // arrange
        val okDelta = OkDelta.builder()
                .deltaNum(0)
                .cnFrom(0)
                .cnTo(1)
                .deltaDate(CURRENT_LOCAL_DATE_TIME)
                .build();
        val delta = new Delta(new HotDelta(), okDelta);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));
        when(zookeeperExecutor.setData(eq("/ENV/DATAMART/delta"), byteCaptor.capture(), anyInt()))
                .thenReturn(Future.succeededFuture());

        // act
        executor.execute(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val savedDelta = CoreSerialization.deserialize(byteCaptor.getValue(), Delta.class);
                    assertNull(savedDelta.getHot());
                }).completeNow());
    }

    @Test
    void shouldFailWhenDeltaException(VertxTestContext testContext) {
        // arrange
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta"), any(), any()))
                .thenReturn(Future.failedFuture(new DeltaException("Exception")));

        // act
        executor.execute(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Exception", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenUnexpectedException(VertxTestContext testContext) {
        // arrange
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta"), any(), any()))
                .thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        // act
        executor.execute(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Can't delete delta hot on datamart[DATAMART]: Exception", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldBeCorrectClass() {
        assertSame(executor.getExecutorInterface(), DeleteDeltaHotExecutor.class);
    }
}