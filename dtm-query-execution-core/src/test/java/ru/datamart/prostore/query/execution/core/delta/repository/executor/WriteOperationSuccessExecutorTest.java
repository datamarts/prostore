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
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.operation.WriteOpFinish;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.ArrayList;
import java.util.Arrays;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class WriteOperationSuccessExecutorTest {
    private static final String ENV = "ENV";
    private static final String DATAMART = "DATAMART";

    @Mock
    private ZookeeperExecutor zookeeperExecutor;

    private WriteOperationSuccessExecutor executor;

    @Captor
    private ArgumentCaptor<Iterable<Op>> opsCaptor;

    @BeforeEach
    void setUp() {
        executor = new WriteOperationSuccessExecutor(zookeeperExecutor, ENV);
    }

    @Test
    void shouldSuccessWhenOneOperation(VertxTestContext testContext) {
        // arrange
        val hotDelta = HotDelta.builder()
                .deltaNum(0)
                .cnFrom(0L)
                .cnTo(null)
                .cnMax(-1)
                .rollingBack(false)
                .writeOperationsFinished(null)
                .build();
        val delta = new Delta(hotDelta, null);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        val writeOp = DeltaWriteOp.builder()
                .cnFrom(0)
                .sysCn(0L)
                .status(WriteOperationStatus.EXECUTING.getValue())
                .query("query")
                .tableName("table")
                .build();
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/run/0000000000"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(writeOp)));

        when(zookeeperExecutor.getChildren("/ENV/DATAMART/run"))
                .thenReturn(Future.succeededFuture(Arrays.asList("0000000000")));

        when(zookeeperExecutor.multi(any()))
                .thenReturn(Future.succeededFuture());

        // act
        executor.execute(DATAMART, 0)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(zookeeperExecutor).multi(opsCaptor.capture());

                    ArrayList<Op> ops = new ArrayList<>();
                    opsCaptor.getValue().forEach(ops::add);
                    assertThat(ops, contains(
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/run/0000000000")),
                                    hasProperty("type", is(ZooDefs.OpCode.delete))
                            ),
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/block/table")),
                                    hasProperty("type", is(ZooDefs.OpCode.delete))
                            ),
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/delta")),
                                    hasProperty("type", is(ZooDefs.OpCode.setData))
                            )
                    ));

                    byte[] data = TestUtils.getFieldValue(ops.get(2), "data");
                    Delta updatedDelta = CoreSerialization.deserialize(data, Delta.class);
                    assertNotNull(updatedDelta.getHot());
                    assertEquals(0, updatedDelta.getHot().getDeltaNum());
                    assertEquals(0, updatedDelta.getHot().getCnFrom());
                    assertEquals(0, updatedDelta.getHot().getCnTo());
                    assertEquals(0, updatedDelta.getHot().getCnMax());
                    assertFalse(updatedDelta.getHot().isRollingBack());
                    assertThat(updatedDelta.getHot().getWriteOperationsFinished(), containsInAnyOrder(
                            allOf(
                                    hasProperty("tableName", is("table")),
                                    hasProperty("cnList", containsInAnyOrder(
                                            is(0L)
                                    ))
                            )
                    ));
                    verifyNoMoreInteractions(zookeeperExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWhenDeltaHotNotExist(VertxTestContext testContext) {
        // arrange
        val delta = new Delta(null, null);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        // act
        executor.execute(DATAMART, 0)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Can't write operation \"success\" by datamart[DATAMART], sysCn[0]: Delta hot not exists", ar.cause().getMessage());
                    verifyNoMoreInteractions(zookeeperExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenTwoOperationsAndCompletingLast(VertxTestContext testContext) {
        // arrange
        val hotDelta = HotDelta.builder()
                .deltaNum(0)
                .cnFrom(0L)
                .cnTo(null)
                .cnMax(-1)
                .rollingBack(false)
                .writeOperationsFinished(null)
                .build();
        val delta = new Delta(hotDelta, null);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        val writeOp = DeltaWriteOp.builder()
                .cnFrom(0)
                .sysCn(1L)
                .status(WriteOperationStatus.EXECUTING.getValue())
                .query("query")
                .tableName("table")
                .build();
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/run/0000000001"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(writeOp)));

        when(zookeeperExecutor.getChildren("/ENV/DATAMART/run"))
                .thenReturn(Future.succeededFuture(Arrays.asList("0000000000", "0000000001")));

        when(zookeeperExecutor.multi(any()))
                .thenReturn(Future.succeededFuture());

        // act
        executor.execute(DATAMART, 1)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(zookeeperExecutor).multi(opsCaptor.capture());

                    ArrayList<Op> ops = new ArrayList<>();
                    opsCaptor.getValue().forEach(ops::add);
                    assertThat(ops, contains(
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/run/0000000001")),
                                    hasProperty("type", is(ZooDefs.OpCode.delete))
                            ),
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/block/table")),
                                    hasProperty("type", is(ZooDefs.OpCode.delete))
                            ),
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/delta")),
                                    hasProperty("type", is(ZooDefs.OpCode.setData))
                            )
                    ));

                    byte[] data = TestUtils.getFieldValue(ops.get(2), "data");
                    Delta updatedDelta = CoreSerialization.deserialize(data, Delta.class);
                    assertNotNull(updatedDelta.getHot());
                    assertEquals(0, updatedDelta.getHot().getDeltaNum());
                    assertEquals(0, updatedDelta.getHot().getCnFrom());
                    assertNull(updatedDelta.getHot().getCnTo());
                    assertEquals(1, updatedDelta.getHot().getCnMax());
                    assertFalse(updatedDelta.getHot().isRollingBack());
                    assertThat(updatedDelta.getHot().getWriteOperationsFinished(), containsInAnyOrder(
                            allOf(
                                    hasProperty("tableName", is("table")),
                                    hasProperty("cnList", containsInAnyOrder(
                                            is(1L)
                                    ))
                            )
                    ));
                    verifyNoMoreInteractions(zookeeperExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenTwoOperationsAndCompletingFirst(VertxTestContext testContext) {
        // arrange
        val hotDelta = HotDelta.builder()
                .deltaNum(0)
                .cnFrom(0L)
                .cnTo(null)
                .cnMax(-1)
                .rollingBack(false)
                .writeOperationsFinished(null)
                .build();
        val delta = new Delta(hotDelta, null);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        val writeOp = DeltaWriteOp.builder()
                .cnFrom(0)
                .sysCn(1L)
                .status(WriteOperationStatus.EXECUTING.getValue())
                .query("query")
                .tableName("table")
                .build();
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/run/0000000000"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(writeOp)));

        when(zookeeperExecutor.getChildren("/ENV/DATAMART/run"))
                .thenReturn(Future.succeededFuture(Arrays.asList("0000000000", "0000000001")));

        when(zookeeperExecutor.multi(any()))
                .thenReturn(Future.succeededFuture());

        // act
        executor.execute(DATAMART, 0)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(zookeeperExecutor).multi(opsCaptor.capture());

                    ArrayList<Op> ops = new ArrayList<>();
                    opsCaptor.getValue().forEach(ops::add);
                    assertThat(ops, contains(
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/run/0000000000")),
                                    hasProperty("type", is(ZooDefs.OpCode.delete))
                            ),
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/block/table")),
                                    hasProperty("type", is(ZooDefs.OpCode.delete))
                            ),
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/delta")),
                                    hasProperty("type", is(ZooDefs.OpCode.setData))
                            )
                    ));

                    byte[] data = TestUtils.getFieldValue(ops.get(2), "data");
                    Delta updatedDelta = CoreSerialization.deserialize(data, Delta.class);
                    assertNotNull(updatedDelta.getHot());
                    assertEquals(0, updatedDelta.getHot().getDeltaNum());
                    assertEquals(0, updatedDelta.getHot().getCnFrom());
                    assertEquals(0, updatedDelta.getHot().getCnTo());
                    assertEquals(0, updatedDelta.getHot().getCnMax());
                    assertFalse(updatedDelta.getHot().isRollingBack());
                    assertThat(updatedDelta.getHot().getWriteOperationsFinished(), containsInAnyOrder(
                            allOf(
                                    hasProperty("tableName", is("table")),
                                    hasProperty("cnList", containsInAnyOrder(
                                            is(0L)
                                    ))
                            )
                    ));
                    verifyNoMoreInteractions(zookeeperExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenTwoOperationsAndFirstCompleted(VertxTestContext testContext) {
        // arrange
        val writeOpFinishes = new ArrayList<WriteOpFinish>();
        writeOpFinishes.add(new WriteOpFinish("table", newArrayList(0L)));

        val hotDelta = HotDelta.builder()
                .deltaNum(0)
                .cnFrom(0L)
                .cnTo(null)
                .cnMax(-1)
                .rollingBack(false)
                .writeOperationsFinished(writeOpFinishes)
                .build();
        val delta = new Delta(hotDelta, null);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        val writeOp = DeltaWriteOp.builder()
                .cnFrom(0)
                .sysCn(1L)
                .status(WriteOperationStatus.EXECUTING.getValue())
                .query("query")
                .tableName("table")
                .build();
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/run/0000000001"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(writeOp)));

        when(zookeeperExecutor.getChildren("/ENV/DATAMART/run"))
                .thenReturn(Future.succeededFuture(Arrays.asList("0000000001")));

        when(zookeeperExecutor.multi(any()))
                .thenReturn(Future.succeededFuture());

        // act
        executor.execute(DATAMART, 1)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(zookeeperExecutor).multi(opsCaptor.capture());

                    ArrayList<Op> ops = new ArrayList<>();
                    opsCaptor.getValue().forEach(ops::add);
                    assertThat(ops, contains(
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/run/0000000001")),
                                    hasProperty("type", is(ZooDefs.OpCode.delete))
                            ),
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/block/table")),
                                    hasProperty("type", is(ZooDefs.OpCode.delete))
                            ),
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/delta")),
                                    hasProperty("type", is(ZooDefs.OpCode.setData))
                            )
                    ));

                    byte[] data = TestUtils.getFieldValue(ops.get(2), "data");
                    Delta updatedDelta = CoreSerialization.deserialize(data, Delta.class);
                    assertNotNull(updatedDelta.getHot());
                    assertEquals(0, updatedDelta.getHot().getDeltaNum());
                    assertEquals(0, updatedDelta.getHot().getCnFrom());
                    assertEquals(1, updatedDelta.getHot().getCnTo());
                    assertEquals(1, updatedDelta.getHot().getCnMax());
                    assertFalse(updatedDelta.getHot().isRollingBack());
                    assertThat(updatedDelta.getHot().getWriteOperationsFinished(), containsInAnyOrder(
                            allOf(
                                    hasProperty("tableName", is("table")),
                                    hasProperty("cnList", containsInAnyOrder(
                                            is(0L), is(1L)
                                    ))
                            )
                    ));
                    verifyNoMoreInteractions(zookeeperExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenTwoOperationsAndFirstCompletedInAnotherTable(VertxTestContext testContext) {
        // arrange
        val writeOpFinishes = new ArrayList<WriteOpFinish>();
        writeOpFinishes.add(new WriteOpFinish("anothertable", newArrayList(0L)));

        val hotDelta = HotDelta.builder()
                .deltaNum(0)
                .cnFrom(0L)
                .cnTo(null)
                .cnMax(-1)
                .rollingBack(false)
                .writeOperationsFinished(writeOpFinishes)
                .build();
        val delta = new Delta(hotDelta, null);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        val writeOp = DeltaWriteOp.builder()
                .cnFrom(0)
                .sysCn(1L)
                .status(WriteOperationStatus.EXECUTING.getValue())
                .query("query")
                .tableName("table")
                .build();
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/run/0000000001"), any(), any()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(writeOp)));

        when(zookeeperExecutor.getChildren("/ENV/DATAMART/run"))
                .thenReturn(Future.succeededFuture(Arrays.asList("0000000001")));

        when(zookeeperExecutor.multi(any()))
                .thenReturn(Future.succeededFuture());

        // act
        executor.execute(DATAMART, 1)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(zookeeperExecutor).multi(opsCaptor.capture());

                    ArrayList<Op> ops = new ArrayList<>();
                    opsCaptor.getValue().forEach(ops::add);
                    assertThat(ops, contains(
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/run/0000000001")),
                                    hasProperty("type", is(ZooDefs.OpCode.delete))
                            ),
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/block/table")),
                                    hasProperty("type", is(ZooDefs.OpCode.delete))
                            ),
                            allOf(
                                    hasProperty("path", is("/ENV/DATAMART/delta")),
                                    hasProperty("type", is(ZooDefs.OpCode.setData))
                            )
                    ));

                    byte[] data = TestUtils.getFieldValue(ops.get(2), "data");
                    Delta updatedDelta = CoreSerialization.deserialize(data, Delta.class);
                    assertNotNull(updatedDelta.getHot());
                    assertEquals(0, updatedDelta.getHot().getDeltaNum());
                    assertEquals(0, updatedDelta.getHot().getCnFrom());
                    assertEquals(1, updatedDelta.getHot().getCnTo());
                    assertEquals(1, updatedDelta.getHot().getCnMax());
                    assertFalse(updatedDelta.getHot().isRollingBack());
                    assertThat(updatedDelta.getHot().getWriteOperationsFinished(), containsInAnyOrder(
                            allOf(
                                    hasProperty("tableName", is("table")),
                                    hasProperty("cnList", containsInAnyOrder(
                                            is(1L)
                                    ))
                            ),
                            allOf(
                                    hasProperty("tableName", is("anothertable")),
                                    hasProperty("cnList", containsInAnyOrder(
                                            is(0L)
                                    ))
                            )
                    ));
                    verifyNoMoreInteractions(zookeeperExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWhenDeserializeException(VertxTestContext testContext) {
        // arrange
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta"), any(), any()))
                .thenReturn(Future.succeededFuture(new byte[0]));

        // act
        executor.execute(DATAMART, 1)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertThat(ar.cause().getMessage(), containsString("Can't deserialize Delta"));
                    verifyNoMoreInteractions(zookeeperExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWithPackedError(VertxTestContext testContext) {
        // arrange
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta"), any(), any()))
                .thenReturn(Future.failedFuture(new KeeperException.BadVersionException()));

        // act
        executor.execute(DATAMART, 1)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertSame(ar.cause().getClass(), DeltaException.class);
                    assertSame(ar.cause().getCause().getClass(), KeeperException.BadVersionException.class);
                    verifyNoMoreInteractions(zookeeperExecutor);
                }).completeNow());
    }

    @Test
    void shouldBeCorrectClass() {
        assertSame(executor.getExecutorInterface(), WriteOperationSuccessExecutor.class);
    }
}