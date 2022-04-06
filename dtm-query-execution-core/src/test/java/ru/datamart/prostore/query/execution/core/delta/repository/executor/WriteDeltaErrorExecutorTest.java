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
import org.apache.zookeeper.Op;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.*;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class WriteDeltaErrorExecutorTest {

    private static final String ENV = "test";
    private static final String DATAMART = "dtm";
    private static final Long DELTA_HOT_NUM = 1L;

    private static final Delta CORRECT_NUM_DELTA = Delta.builder()
            .hot(HotDelta.builder()
                    .deltaNum(DELTA_HOT_NUM)
                    .rollingBack(false)
                    .build())
            .build();
    private static final Delta INCORRECT_NUM_DELTA = Delta.builder()
            .hot(HotDelta.builder()
                    .deltaNum(DELTA_HOT_NUM + 1)
                    .rollingBack(false)
                    .build())
            .build();
    private static final Delta ROLLINGBACK_DELTA = Delta.builder()
            .hot(HotDelta.builder()
                    .deltaNum(DELTA_HOT_NUM)
                    .rollingBack(true)
                    .build())
            .build();
    private static final Delta NULL_HOT_DELTA = Delta.builder()
            .build();
    private static final Op SET_DATA_OP = Op.setData("/test/dtm/delta", CoreSerialization.serialize(Delta.builder()
            .hot(HotDelta.builder()
                    .deltaNum(DELTA_HOT_NUM)
                    .cnTo(-1L)
                    .rollingBack(true)
                    .build())
            .build()), -1);
    private final ZookeeperExecutor zkExecutor = mock(ZookeeperExecutor.class);
    private final WriteDeltaErrorExecutor deltaExecutor = new WriteDeltaErrorExecutor(zkExecutor, ENV);

    @Captor
    private ArgumentCaptor<List<Op>> zkOpsCaptor;

    @Test
    void shouldSucceed(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString(), isNull(), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(CORRECT_NUM_DELTA)));
        when(zkExecutor.multi(zkOpsCaptor.capture())).thenReturn(Future.succeededFuture());

        //act
        deltaExecutor.execute(DATAMART, DELTA_HOT_NUM)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());

                    val setDataOp = zkOpsCaptor.getValue().get(2);
                    assertThat(setDataOp).isEqualToIgnoringGivenFields(SET_DATA_OP, "version");
                    verify(zkExecutor).getData(anyString(), isNull(), any());
                    verify(zkExecutor).multi(anyCollection());
                }).completeNow());
    }

    @Test
    void shouldFailWithIncorrectDeltaHotNumber(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString(), isNull(), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(INCORRECT_NUM_DELTA)));

        //act
        deltaExecutor.execute(DATAMART, DELTA_HOT_NUM)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DeltaNumIsNotNextToActualException);

                    verify(zkExecutor).getData(anyString(), isNull(), any());
                    verifyNoMoreInteractions(zkExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWithNullDeltaHot(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString(), isNull(), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(NULL_HOT_DELTA)));

        //act
        deltaExecutor.execute(DATAMART, DELTA_HOT_NUM)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DeltaHotNotStartedException);

                    verify(zkExecutor).getData(anyString(), isNull(), any());
                    verifyNoMoreInteractions(zkExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWithRollingDeltaHot(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString(), isNull(), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(ROLLINGBACK_DELTA)));

        //act
        deltaExecutor.execute(DATAMART, DELTA_HOT_NUM)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DeltaAlreadyIsRollingBackException);

                    verify(zkExecutor).getData(anyString(), isNull(), any());
                    verifyNoMoreInteractions(zkExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWithNotEmptyException(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString(), isNull(), any())).thenReturn(Future.failedFuture(new KeeperException.NotEmptyException()));

        //act
        deltaExecutor.execute(DATAMART, DELTA_HOT_NUM)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DeltaNotFinishedException);
                }).completeNow());
    }

    @Test
    void shouldFailWithOtherKeeperException(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString(), isNull(), any())).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        //act
        deltaExecutor.execute(DATAMART, DELTA_HOT_NUM)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DeltaException);
                }).completeNow());
    }

    @Test
    void shouldFailWithOtherException(VertxTestContext testContext) {
        //arrange
        when(zkExecutor.getData(anyString(), isNull(), any())).thenReturn(Future.failedFuture("error"));

        //act
        deltaExecutor.execute(DATAMART, DELTA_HOT_NUM)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DeltaException);
                }).completeNow());
    }
}
