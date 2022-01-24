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
package io.arenadata.dtm.query.execution.core.base.repository.zookeeper;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Changelog;
import io.arenadata.dtm.query.execution.core.base.repository.DaoUtils;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class ChangelogDaoTest {

    private static final String DATAMART = "dtm";
    private static final String ENV = "test";
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
                .thenReturn(Future.succeededFuture(DaoUtils.serialize(CHANGELOG_ZERO)))
                .thenReturn(Future.succeededFuture(DaoUtils.serialize(CHANGELOG_ONE)))
                .thenReturn(Future.succeededFuture(DaoUtils.serialize(PARENT_CHANGELOG)));

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
                .thenReturn(Future.succeededFuture(DaoUtils.serialize(CHANGELOG_ZERO)))
                .thenReturn(Future.succeededFuture(DaoUtils.serialize(CHANGELOG_ONE)))
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
                .thenReturn(Future.succeededFuture(DaoUtils.serialize(PARENT_CHANGELOG)));

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
    void getChildrenNoNodeExceptionFail(VertxTestContext testContext) {
        when(EXECUTOR.getChildren(contains(DATAMART))).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));
        when(EXECUTOR.getData(anyString()))
                .thenReturn(Future.succeededFuture(null));

        changelogDao.getChanges(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertThat(ar.cause().getMessage()).containsIgnoringCase("not found");
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

}
