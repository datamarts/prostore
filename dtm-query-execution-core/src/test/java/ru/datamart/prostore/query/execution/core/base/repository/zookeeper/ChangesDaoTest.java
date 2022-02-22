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
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.DenyChanges;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.serialization.CoreSerialization;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class ChangesDaoTest {

    private static final String DATAMART = "dtm";
    private static final String ENV = "test";
    private static final String EMPTY_CODE = "";
    private static final String DENY_CODE = "1234";
    private static final ZookeeperExecutor EXECUTOR = mock(ZookeeperExecutor.class);
    private static final DenyChanges DENY_CHANGES = DenyChanges.builder()
            .denyCode(DENY_CODE)
            .denyTime("2021-12-03 09:56:09")
            .build();
    private static final DenyChanges DENY_CHANGES_NULL_CODE = DenyChanges.builder()
            .denyCode(null)
            .denyTime("2021-12-03 09:56:09")
            .build();

    private static ChangesDao changesDao;

    @BeforeAll
    static void setUp() {
        changesDao = new ChangesDao(EXECUTOR, ENV);
    }

    @Test
    void denyChangesSuccess(VertxTestContext testContext) {
        when(EXECUTOR.create(anyString(), any(), anyList(), any())).thenReturn(Future.succeededFuture(""));

        changesDao.denyChanges(DATAMART, EMPTY_CODE)
                .onComplete(ar -> testContext.verify(() ->
                        assertTrue(ar.succeeded()))
                        .completeNow());
    }

    @Test
    void denyChangesNodeExistsException(VertxTestContext testContext) {
        when(EXECUTOR.create(anyString(), any(), anyList(), any())).thenReturn(Future.failedFuture(new KeeperException.NodeExistsException()));

        changesDao.denyChanges(DATAMART, EMPTY_CODE)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Changes are blocked by another process"));
                }).completeNow());
    }

    @Test
    void denyChangesNoNodeException(VertxTestContext testContext) {
        when(EXECUTOR.create(anyString(), any(), anyList(), any())).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        changesDao.denyChanges(DATAMART, EMPTY_CODE)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DatamartNotExistsException);
                }).completeNow());
    }

    @Test
    void denyChangesUnexpectedException(VertxTestContext testContext) {
        when(EXECUTOR.create(anyString(), any(), anyList(), any())).thenReturn(Future.failedFuture("unexpected keeper error"));

        changesDao.denyChanges(DATAMART, EMPTY_CODE)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Can't execute denyChanges"));
                }).completeNow());
    }

    @Test
    void allowChangesSuccess(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(DENY_CHANGES)));
        when(EXECUTOR.delete(anyString(), anyInt())).thenReturn(Future.succeededFuture());

        changesDao.allowChanges(DATAMART, DENY_CODE)
                .onComplete(ar -> testContext.verify(() ->
                        assertTrue(ar.succeeded()))
                        .completeNow());
    }

    @Test
    void allowChangesImmutableNodeNoNodeException(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString())).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        changesDao.allowChanges(DATAMART, DENY_CODE)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Changes are not blocked now within"));
                }).completeNow());
    }

    @Test
    void allowChangesImmutableNodeUnexpectedException(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString())).thenReturn(Future.failedFuture("Unexpected exception"));

        changesDao.allowChanges(DATAMART, DENY_CODE)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Can't execute allowChanges"));
                }).completeNow());
    }

    @Test
    void allowChangesWrongCodeException(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(DENY_CHANGES)));
        when(EXECUTOR.delete(anyString(), anyInt())).thenReturn(Future.succeededFuture());

        changesDao.allowChanges(DATAMART, EMPTY_CODE)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Wrong code"));
                }).completeNow());
    }

    @Test
    void allowChangesNullCodeException(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(DENY_CHANGES_NULL_CODE)));
        when(EXECUTOR.delete(anyString(), anyInt())).thenReturn(Future.succeededFuture());

        changesDao.allowChanges(DATAMART, DENY_CODE)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Changes are blocked by another process within"));
                }).completeNow());
    }

    @Test
    void allowChangesEmptyNode(VertxTestContext testContext) {
        when(EXECUTOR.getData(anyString())).thenReturn(Future.succeededFuture(null));

        changesDao.allowChanges(DATAMART, DENY_CODE)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Unexpected null"));
                }).completeNow());
    }

}
