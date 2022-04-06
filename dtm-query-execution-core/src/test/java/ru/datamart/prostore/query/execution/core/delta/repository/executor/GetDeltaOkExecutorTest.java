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
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaNotFoundException;
import ru.datamart.prostore.serialization.CoreSerialization;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class GetDeltaOkExecutorTest {

    private static final String ENV = "test";
    private static final String DATAMART = "dtm";
    public static final OkDelta DELTA_OK = OkDelta.builder()
            .deltaNum(1L)
            .cnFrom(2L)
            .cnTo(4L)
            .build();
    private static final Delta DELTA = Delta.builder()
            .ok(DELTA_OK)
            .build();

    private final ZookeeperExecutor zkExecutor = mock(ZookeeperExecutor.class);

    private final GetDeltaOkExecutor deltaExecutor = new GetDeltaOkExecutor(zkExecutor, ENV);

    @Test
    void shouldSucceed(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(DELTA)));

        //act
        deltaExecutor.execute(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(DELTA_OK, ar.result());
                }).completeNow());
    }
    @Test
    void shouldSucceedNullDeltaHot(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(Delta.builder().build())));

        //act
        deltaExecutor.execute(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertNull(ar.result());
                }).completeNow());
    }

    @Test
    void shouldFailedDeserialization(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(DELTA_OK)));

        //act
        deltaExecutor.execute(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DeltaException);
                }).completeNow());
    }

    @Test
    void shouldFailedNoNodeException(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString())).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        //act
        deltaExecutor.execute(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DeltaNotFoundException);
                }).completeNow());
    }

    @Test
    void shouldFailedOtherExceptions(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString())).thenReturn(Future.failedFuture("error"));

        //act
        deltaExecutor.execute(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DeltaException);
                }).completeNow());
    }
}
