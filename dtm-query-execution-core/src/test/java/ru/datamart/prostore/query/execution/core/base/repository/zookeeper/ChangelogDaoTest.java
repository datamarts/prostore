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
package ru.datamart.prostore.query.execution.core.base.repository.zookeeper;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Changelog;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class ChangelogDaoTest {

    private static final String DATAMART = "dtm";
    private static final String ENV = "test";
    private static final String ENTITY = "test_entity";

    private static final String EXPECTED_CHANGE_QUERY = "expected change query";

    private static final String EXPECTED_PREVIOUS_NOT_COMPLETED_ERROR_MSG = "Previous change operation is not completed in datamart [dtm]";
    private static final String EXPECTED_CHANGE_OPERATIONS_FORBIDDEN_ERROR_MSG = "Change operations are forbidden";
    private static final String EXPECTED_COULD_NOT_CREATE_CHANGELOG_ERROR_MSG = "Changelog node already exist";
    private static final String EXPECTED_ERROR_MSG = "Unexpected exception during writeNewRecord";
    private static final String ERROR_MSG = "error";

    private static final ZookeeperExecutor EXECUTOR = mock(ZookeeperExecutor.class);
    private static final Changelog CHANGELOG_ZERO = Changelog.builder()
            .operationNumber(0L)
            .entityName("tbl")
            .changeQuery("query")
            .deltaNum(0L)
            .build();
    private static final Changelog CHANGELOG_ONE = Changelog.builder()
            .operationNumber(1L)
            .entityName("tbl")
            .changeQuery("query")
            .deltaNum(0L)
            .build();
    private static final Changelog PARENT_CHANGELOG = Changelog.builder()
            .entityName("tbl")
            .changeQuery("query")
            .deltaNum(0L)
            .build();

    private static ChangelogDao changelogDao;

    @BeforeAll
    static void setUp() {
        changelogDao = new ChangelogDao(EXECUTOR, ENV);
    }

    @Test
    void getChildrenWithParentNodeSuccess(VertxTestContext testContext) {
        when(EXECUTOR.getChildren(contains(DATAMART))).thenReturn(Future.succeededFuture(Arrays.asList("000", "001")));
        when(EXECUTOR.getData(anyString()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(CHANGELOG_ZERO)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(CHANGELOG_ONE)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(PARENT_CHANGELOG)));

        changelogDao.getChanges(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    assertEquals(3, ar.result().size());
                    assertThat(ar.result()).contains(CHANGELOG_ZERO, CHANGELOG_ONE);
                    assertEquals(2, ar.result().get(2).getOperationNumber());
                }).completeNow());
    }

    @Test
    void getChildrenWithoutParentNodeSuccess(VertxTestContext testContext) {
        when(EXECUTOR.getChildren(contains(DATAMART))).thenReturn(Future.succeededFuture(Arrays.asList("000", "001")));
        when(EXECUTOR.getData(anyString()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(CHANGELOG_ZERO)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(CHANGELOG_ONE)))
                .thenReturn(Future.succeededFuture(null));

        changelogDao.getChanges(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    assertEquals(2, ar.result().size());
                    assertThat(ar.result()).contains(CHANGELOG_ZERO, CHANGELOG_ONE);
                }).completeNow());
    }

    @Test
    void getChildrenWithoutChildrenSuccess(VertxTestContext testContext) {
        when(EXECUTOR.getChildren(contains(DATAMART))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(EXECUTOR.getData(anyString()))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(PARENT_CHANGELOG)));

        changelogDao.getChanges(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    assertEquals(1, ar.result().size());
                    assertEquals(0, ar.result().get(0).getOperationNumber());
                }).completeNow());
    }

    @Test
    void getChildrenWithoEmptyChangelogNodeSuccess(VertxTestContext testContext) {
        when(EXECUTOR.getChildren(contains(DATAMART))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(EXECUTOR.getData(anyString()))
                .thenReturn(Future.succeededFuture(null));

        changelogDao.getChanges(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    assertEquals(0, ar.result().size());
                }).completeNow());
    }

    @Test
    void getChildrenNoNodeExceptionSuccess(VertxTestContext testContext) {
        when(EXECUTOR.getChildren(contains(DATAMART))).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));
        when(EXECUTOR.getData(anyString()))
                .thenReturn(Future.succeededFuture(null));

        changelogDao.getChanges(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    assertTrue(ar.result().isEmpty());
                }).completeNow());
    }

    @Test
    void getChildrenGetDataUnexpectedExceptionFail(VertxTestContext testContext) {
        when(EXECUTOR.getChildren(contains(DATAMART))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(EXECUTOR.getData(anyString()))
                .thenReturn(Future.failedFuture("unexpected exception"));

        changelogDao.getChanges(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertThat(ar.cause().getMessage()).containsIgnoringCase("unexpected exception");
                }).completeNow());
    }

    @Test
    void shouldFailWhenDifferentChangeQueries(VertxTestContext testContext) {
        val changelog = Changelog.builder()
                .entityName(ENTITY)
                .changeQuery(EXPECTED_CHANGE_QUERY)
                .build();

        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));

        changelogDao.writeNewRecord(DATAMART, ENTITY, "failed change query", null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(DtmException.class, ar.cause().getClass());
                    assertEquals(EXPECTED_PREVIOUS_NOT_COMPLETED_ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenDifferentDeltaNum(VertxTestContext testContext) {
        val changelog = Changelog.builder()
                .entityName(ENTITY)
                .changeQuery(EXPECTED_CHANGE_QUERY)
                .deltaNum(5L)
                .build();

        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));

        changelogDao.writeNewRecord(DATAMART, ENTITY, EXPECTED_CHANGE_QUERY, null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(DtmException.class, ar.cause().getClass());
                    assertEquals(EXPECTED_PREVIOUS_NOT_COMPLETED_ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldSuccessOnSetDataWhenNullChangelog(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(null));

        when(EXECUTOR.multi(any())).thenReturn(Future.succeededFuture());

        changelogDao.writeNewRecord(DATAMART, ENTITY, EXPECTED_CHANGE_QUERY, null)
                .onComplete(ar -> testContext.verify(() ->
                        assertTrue(ar.succeeded()))
                        .completeNow());
    }

    @Test
    void shouldFailOnSetDataWhenNodeExists(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(null));

        when(EXECUTOR.multi(any())).thenReturn(Future.failedFuture(new KeeperException.NodeExistsException()));

        changelogDao.writeNewRecord(DATAMART, ENTITY, EXPECTED_CHANGE_QUERY, null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(DtmException.class, ar.cause().getClass());
                    assertEquals(EXPECTED_CHANGE_OPERATIONS_FORBIDDEN_ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailOnSetDataWhenBadVersion(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(null));

        when(EXECUTOR.multi(any())).thenReturn(Future.failedFuture(new KeeperException.BadVersionException()));

        changelogDao.writeNewRecord(DATAMART, ENTITY, EXPECTED_CHANGE_QUERY, null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(DtmException.class, ar.cause().getClass());
                    assertEquals(EXPECTED_PREVIOUS_NOT_COMPLETED_ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailOnSetDataWhenUnexpectedException(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(null));

        when(EXECUTOR.multi(any())).thenReturn(Future.failedFuture(new RuntimeException()));

        changelogDao.writeNewRecord(DATAMART, ENTITY, EXPECTED_CHANGE_QUERY, null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(DtmException.class, ar.cause().getClass());
                    assertEquals(EXPECTED_ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldSuccessOnCreateNewChangelogIfNotPresent(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.failedFuture(new DtmException("Failed exception")));

        when(EXECUTOR.create(anyString(), any(), anyList(), any()))
                .thenReturn(Future.succeededFuture("path"));

        when(EXECUTOR.multi(any())).thenReturn(Future.succeededFuture());

        changelogDao.writeNewRecord(DATAMART, ENTITY, EXPECTED_CHANGE_QUERY, null)
                .onComplete(ar -> testContext.verify(() ->
                        assertTrue(ar.succeeded()))
                        .completeNow());
    }

    @Test
    void shouldFailWhenNewChangelogExists(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.failedFuture(new DtmException("Failed exception")));

        when(EXECUTOR.create(anyString(), any(), anyList(), any()))
                .thenReturn(Future.failedFuture(new KeeperException.NodeExistsException()));

        changelogDao.writeNewRecord(DATAMART, ENTITY, EXPECTED_CHANGE_QUERY, null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(DtmException.class, ar.cause().getClass());
                    assertEquals(EXPECTED_COULD_NOT_CREATE_CHANGELOG_ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailOnCreateChangelogWhenUnexpectedError(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.failedFuture(new DtmException("Failed exception")));

        when(EXECUTOR.create(anyString(), any(), anyList(), any()))
                .thenReturn(Future.failedFuture(new RuntimeException()));

        changelogDao.writeNewRecord(DATAMART, ENTITY, EXPECTED_CHANGE_QUERY, null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(DtmException.class, ar.cause().getClass());
                    assertEquals(EXPECTED_ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldSuccessEraseChangeOperationWhenChangelogEmpty(VertxTestContext testContext) {
        val changelog = Changelog.builder()
                .entityName(ENTITY)
                .changeQuery(EXPECTED_CHANGE_QUERY)
                .deltaNum(0L)
                .build();
        val operationNumber = 0L;

        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));

        when(EXECUTOR.getChildren(anyString()))
                .thenReturn(Future.succeededFuture(Collections.emptyList()));

        when(EXECUTOR.multi(any()))
                .thenReturn(Future.succeededFuture());
        changelogDao.eraseChangeOperation(DATAMART, operationNumber)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    changelog.setOperationNumber(operationNumber);
                    assertEquals(changelog, ar.result());
                }).completeNow());
    }

    @Test
    void shouldSuccessEraseChangeOperationWhenChangelogNotEmpty(VertxTestContext testContext) {
        val changelog = Changelog.builder()
                .entityName(ENTITY)
                .changeQuery(EXPECTED_CHANGE_QUERY)
                .deltaNum(0L)
                .build();
        val operationNumber = 2L;

        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));

        when(EXECUTOR.getChildren(anyString()))
                .thenReturn(Future.succeededFuture(Arrays.asList("000", "001")));

        when(EXECUTOR.multi(any()))
                .thenReturn(Future.succeededFuture());
        changelogDao.eraseChangeOperation(DATAMART, operationNumber)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    changelog.setOperationNumber(operationNumber);
                    assertEquals(changelog, ar.result());
                }).completeNow());
    }

    @Test
    void shouldFailEraseChangeOperationWhenNoActiveOperation(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(null));

        changelogDao.eraseChangeOperation(DATAMART, 2L)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals("Active operation does not exist", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailEraseChangeOperationWhenIncorrectOperationNumber(VertxTestContext testContext) {
        val changelog = Changelog.builder()
                .entityName(ENTITY)
                .changeQuery(EXPECTED_CHANGE_QUERY)
                .deltaNum(0L)
                .build();
        val operationNumber = 1L;

        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));

        when(EXECUTOR.getChildren(anyString()))
                .thenReturn(Future.succeededFuture(Arrays.asList("000", "001")));

        changelogDao.eraseChangeOperation(DATAMART, operationNumber)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals("Not found active change operation with change_num = 1", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailEraseChangeOperationWhenExecuteMultiFailed(VertxTestContext testContext) {
        val changelog = Changelog.builder()
                .entityName(ENTITY)
                .changeQuery(EXPECTED_CHANGE_QUERY)
                .deltaNum(0L)
                .build();
        val operationNumber = 2L;

        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any(Stat.class)))
                .thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));

        when(EXECUTOR.getChildren(anyString()))
                .thenReturn(Future.succeededFuture(Arrays.asList("000", "001")));

        when(EXECUTOR.multi(any()))
                .thenReturn(Future.failedFuture(ERROR_MSG));
        changelogDao.eraseChangeOperation(DATAMART, operationNumber)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }
}
