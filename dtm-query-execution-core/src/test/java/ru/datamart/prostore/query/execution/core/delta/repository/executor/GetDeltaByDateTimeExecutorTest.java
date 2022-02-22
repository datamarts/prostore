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
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;


@ExtendWith({MockitoExtension.class, VertxExtension.class})
class GetDeltaByDateTimeExecutorTest {
    private static final LocalDateTime CURRENT_LOCAL_DATE_TIME = LocalDateTime.of(2020, 1, 10, 11, 11, 11);
    private static final LocalDateTime AFTER_LOCAL_DATE_TIME = LocalDateTime.of(2020, 1, 15, 11, 11, 11);
    private static final LocalDateTime BEFORE_LOCAL_DATE_TIME = LocalDateTime.of(2020, 1, 5, 11, 11, 11);
    private static final String ENV = "ENV";
    private static final String DATAMART = "DATAMART";

    @Mock
    private ZookeeperExecutor zookeeperExecutor;

    private GetDeltaByDateTimeExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new GetDeltaByDateTimeExecutor(zookeeperExecutor, ENV);
    }

    @Test
    void shouldReturnDeltaWhenDeltaDateEqualToRequested(VertxTestContext testContext) {
        // arrange
        val okDelta = OkDelta.builder()
                .deltaNum(1)
                .cnFrom(0)
                .cnTo(1)
                .deltaDate(CURRENT_LOCAL_DATE_TIME)
                .build();
        val delta = new Delta(null, okDelta);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta")))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        // act
        executor.execute(DATAMART, CURRENT_LOCAL_DATE_TIME)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertThat(ar.result(), Matchers.allOf(
                            Matchers.hasProperty("deltaNum", Matchers.is(1L)),
                            Matchers.hasProperty("cnFrom", Matchers.is(0L)),
                            Matchers.hasProperty("cnTo", Matchers.is(1L)),
                            Matchers.hasProperty("deltaDate", Matchers.is(CURRENT_LOCAL_DATE_TIME))
                    ));
                }).completeNow());
    }

    @Test
    void shouldFailWhenNoDeltaOk(VertxTestContext testContext) {
        // arrange
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta")))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(new Delta())));

        // act
        executor.execute(DATAMART, CURRENT_LOCAL_DATE_TIME)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Delta not exist", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldReturnDeltaWhenDeltaStartedBeforeRequested(VertxTestContext testContext) {
        // arrange
        val okDelta = OkDelta.builder()
                .deltaNum(1)
                .cnFrom(0)
                .cnTo(1)
                .deltaDate(BEFORE_LOCAL_DATE_TIME)
                .build();
        val delta = new Delta(null, okDelta);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta")))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        // act
        executor.execute(DATAMART, CURRENT_LOCAL_DATE_TIME)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertThat(ar.result(), Matchers.allOf(
                            Matchers.hasProperty("deltaNum", Matchers.is(1L)),
                            Matchers.hasProperty("cnFrom", Matchers.is(0L)),
                            Matchers.hasProperty("cnTo", Matchers.is(1L)),
                            Matchers.hasProperty("deltaDate", Matchers.is(BEFORE_LOCAL_DATE_TIME))
                    ));
                }).completeNow());
    }

    @Test
    void shouldReturnClosestDeltaWhenMultipleDeltasByDate(VertxTestContext testContext) {
        // arrange
        val okDelta = OkDelta.builder()
                .deltaNum(2)
                .cnFrom(1)
                .cnTo(2)
                .deltaDate(AFTER_LOCAL_DATE_TIME)
                .build();
        val delta1 = new Delta(null, okDelta);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta")))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta1)));
        when(zookeeperExecutor.getChildren(eq("/ENV/DATAMART/delta/date")))
                .thenReturn(Future.succeededFuture(Arrays.asList("2020-01-01", "2020-01-05")));
        when(zookeeperExecutor.getChildren(eq("/ENV/DATAMART/delta/date/2020-01-05")))
                .thenReturn(Future.succeededFuture(Arrays.asList("01:01:01", "11:11:11")));
        val delta = OkDelta.builder()
                .deltaNum(1)
                .cnFrom(0)
                .cnTo(1)
                .deltaDate(BEFORE_LOCAL_DATE_TIME)
                .build();
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta/date/2020-01-05/11:11:11")))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        // act
        executor.execute(DATAMART, CURRENT_LOCAL_DATE_TIME)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertThat(ar.result(), Matchers.allOf(
                            Matchers.hasProperty("deltaNum", Matchers.is(1L)),
                            Matchers.hasProperty("cnFrom", Matchers.is(0L)),
                            Matchers.hasProperty("cnTo", Matchers.is(1L)),
                            Matchers.hasProperty("deltaDate", Matchers.is(BEFORE_LOCAL_DATE_TIME))
                    ));
                }).completeNow());
    }

    @Test
    void shouldReturnClosestDeltaWhenMultipleDeltasByDateAndTimeOfFirstDateIsAfterRequested(VertxTestContext testContext) {
        // arrange
        val okDelta = OkDelta.builder()
                .deltaNum(2)
                .cnFrom(1)
                .cnTo(2)
                .deltaDate(AFTER_LOCAL_DATE_TIME)
                .build();
        val delta1 = new Delta(null, okDelta);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta")))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta1)));
        when(zookeeperExecutor.getChildren(eq("/ENV/DATAMART/delta/date")))
                .thenReturn(Future.succeededFuture(Arrays.asList("2020-01-10", "2020-01-05")));
        when(zookeeperExecutor.getChildren(eq("/ENV/DATAMART/delta/date/2020-01-10")))
                .thenReturn(Future.succeededFuture(Arrays.asList("22:22:22")));
        when(zookeeperExecutor.getChildren(eq("/ENV/DATAMART/delta/date/2020-01-05")))
                .thenReturn(Future.succeededFuture(Arrays.asList("01:01:01", "11:11:11")));
        val delta = OkDelta.builder()
                .deltaNum(1)
                .cnFrom(0)
                .cnTo(1)
                .deltaDate(BEFORE_LOCAL_DATE_TIME)
                .build();
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta/date/2020-01-05/11:11:11")))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));

        // act
        executor.execute(DATAMART, CURRENT_LOCAL_DATE_TIME)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertThat(ar.result(), Matchers.allOf(
                            Matchers.hasProperty("deltaNum", Matchers.is(1L)),
                            Matchers.hasProperty("cnFrom", Matchers.is(0L)),
                            Matchers.hasProperty("cnTo", Matchers.is(1L)),
                            Matchers.hasProperty("deltaDate", Matchers.is(BEFORE_LOCAL_DATE_TIME))
                    ));
                }).completeNow());
    }

    @Test
    void shouldFailWhenOlderDateOnNextDateHaveNoTimes(VertxTestContext testContext) {
        // arrange
        val okDelta = OkDelta.builder()
                .deltaNum(2)
                .cnFrom(1)
                .cnTo(2)
                .deltaDate(AFTER_LOCAL_DATE_TIME)
                .build();
        val delta = new Delta(null, okDelta);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta")))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));
        when(zookeeperExecutor.getChildren(eq("/ENV/DATAMART/delta/date")))
                .thenReturn(Future.succeededFuture(Arrays.asList("2020-01-10", "2020-01-05")));
        when(zookeeperExecutor.getChildren(eq("/ENV/DATAMART/delta/date/2020-01-10")))
                .thenReturn(Future.succeededFuture(Arrays.asList("22:22:22")));
        when(zookeeperExecutor.getChildren(eq("/ENV/DATAMART/delta/date/2020-01-05")))
                .thenReturn(Future.succeededFuture(Collections.emptyList()));

        // act
        executor.execute(DATAMART, CURRENT_LOCAL_DATE_TIME)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Delta not found", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailClosestHistoryDeltaWhenOlderDateAndNextDateNotExist(VertxTestContext testContext) {
        // arrange
        val okDelta = OkDelta.builder()
                .deltaNum(2)
                .cnFrom(1)
                .cnTo(2)
                .deltaDate(AFTER_LOCAL_DATE_TIME)
                .build();
        val delta = new Delta(null, okDelta);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta")))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));
        when(zookeeperExecutor.getChildren(eq("/ENV/DATAMART/delta/date")))
                .thenReturn(Future.succeededFuture(Arrays.asList("2020-01-10")));
        when(zookeeperExecutor.getChildren(eq("/ENV/DATAMART/delta/date/2020-01-10")))
                .thenReturn(Future.succeededFuture(Arrays.asList("22:22:22")));

        // act
        executor.execute(DATAMART, CURRENT_LOCAL_DATE_TIME)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Delta not found", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailClosestHistoryDeltaWhenOlderDateNotExist(VertxTestContext testContext) {
        // arrange
        val okDelta = OkDelta.builder()
                .deltaNum(2)
                .cnFrom(1)
                .cnTo(2)
                .deltaDate(AFTER_LOCAL_DATE_TIME)
                .build();
        val delta = new Delta(null, okDelta);
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta")))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(delta)));
        when(zookeeperExecutor.getChildren(eq("/ENV/DATAMART/delta/date")))
                .thenReturn(Future.succeededFuture(Arrays.asList("2020-01-11")));

        // act
        executor.execute(DATAMART, CURRENT_LOCAL_DATE_TIME)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Delta not found", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenNoNodeException(VertxTestContext testContext) {
        // arrange
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta")))
                .thenReturn(Future.failedFuture(new KeeperException.NoNodeException("exception")));

        // act
        executor.execute(DATAMART, CURRENT_LOCAL_DATE_TIME)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Database DATAMART does not exist", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenDeltaException(VertxTestContext testContext) {
        // arrange
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta")))
                .thenReturn(Future.failedFuture(new DeltaException("exception")));

        // act
        executor.execute(DATAMART, CURRENT_LOCAL_DATE_TIME)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("exception", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenUnexpectedException(VertxTestContext testContext) {
        // arrange
        when(zookeeperExecutor.getData(eq("/ENV/DATAMART/delta")))
                .thenReturn(Future.failedFuture(new RuntimeException("exception")));

        // act
        executor.execute(DATAMART, CURRENT_LOCAL_DATE_TIME)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("can't get delta ok on datamart[DATAMART], dateTime[2020-01-10T11:11:11]: exception", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldBeCorrectClass() {
        assertSame(executor.getExecutorInterface(), GetDeltaByDateTimeExecutor.class);
    }
}