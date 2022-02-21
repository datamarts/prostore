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
package ru.datamart.prostore.query.execution.core.base.repository.zookeeper;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.core.base.dto.metadata.DatamartInfo;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartAlreadyExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class DatamartDaoTest {
    private static final String TARGET_PATH = "/SYSNAME/test_dtm";
    private static final String ENV_PATH = "/SYSNAME";
    private static final String SYSTEM_NAME = "SYSNAME";
    private static final String DATAMART = "test_dtm";

    private static final String ERROR_CREATE_DATAMART = "Can't create datamart [test_dtm]";
    private static final String ERROR_DATABASE_DOES_NOT_EXIST = "Database test_dtm does not exist";


    private final ZookeeperExecutor executor = mock(ZookeeperExecutor.class);
    private final DatamartDao datamartDao = new DatamartDao(executor, SYSTEM_NAME);

    @Test
    void shouldSuccess(VertxTestContext testContext) {
        when(executor.createEmptyPersistentPath(anyString())).thenReturn(Future.succeededFuture());
        when(executor.multi(anyList())).thenReturn(Future.succeededFuture());

        datamartDao.createDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow());
    }

    @Test
    void shouldSuccessWhenDatamartNodeExists(VertxTestContext testContext) {
        when(executor.createEmptyPersistentPath(anyString())).thenReturn(Future.failedFuture(new KeeperException.NodeExistsException()));
        when(executor.multi(anyList())).thenReturn(Future.succeededFuture());

        datamartDao.createDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow());
    }

    @Test
    void shouldFailWhenUnexpectedErrorOccursInCreateEmptyPath(VertxTestContext testContext) {
        when(executor.createEmptyPersistentPath(anyString())).thenReturn(Future.failedFuture(new RuntimeException("Failed")));

        datamartDao.createDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> {
            assertTrue(ar.failed());
            assertEquals(DtmException.class, ar.cause().getClass());
            assertEquals(ERROR_CREATE_DATAMART, ar.cause().getMessage());
        }).completeNow());
    }

    @Test
    void shouldFailWhenDatabaseHasAlreadyExisted(VertxTestContext testContext) {
        when(executor.createEmptyPersistentPath(anyString())).thenReturn(Future.succeededFuture());

        KeeperException.NodeExistsException exception = mock(KeeperException.NodeExistsException.class);
        when(exception.getResults()).thenReturn(getErrorOpResult());

        when(executor.multi(anyList())).thenReturn(Future.failedFuture(exception));

        datamartDao.createDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> {
            assertTrue(ar.failed());
            assertEquals(DatamartAlreadyExistsException.class, ar.cause().getClass());
            assertEquals("Database test_dtm already exists", ar.cause().getMessage());
        }).completeNow());
    }

    @Test
    void shouldFailWhenUnexpectedErrorOccursInMulti(VertxTestContext testContext) {
        when(executor.createEmptyPersistentPath(anyString())).thenReturn(Future.succeededFuture());
        when(executor.multi(anyList())).thenReturn(Future.failedFuture(new RuntimeException()));

        datamartDao.createDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> {
            assertTrue(ar.failed());
            assertEquals(DtmException.class, ar.cause().getClass());
            assertEquals(ERROR_CREATE_DATAMART, ar.cause().getMessage());
        }).completeNow());
    }

    @Test
    void shouldSuccessWhenGetDatamarts(VertxTestContext testContext) {
        when(executor.getChildren(anyString())).thenReturn(Future.succeededFuture());

        datamartDao.getDatamarts().onComplete(ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow());
    }

    @Test
    void shouldFailWhenGetChildrenEnvDoesNotExist(VertxTestContext testContext) {
        when(executor.getChildren(anyString())).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        datamartDao.getDatamarts().onComplete(ar -> testContext.verify(() -> {
            assertTrue(ar.failed());
            assertEquals(DtmException.class, ar.cause().getClass());
            assertEquals("Env [/SYSNAME] not exists", ar.cause().getMessage());
        }).completeNow());
    }

    @Test
    void shouldFailWhenUnexpectedErrorOccursInGetChildren(VertxTestContext testContext) {
        when(executor.getChildren(anyString())).thenReturn(Future.failedFuture(new RuntimeException()));

        datamartDao.getDatamarts().onComplete(ar -> testContext.verify(() -> {
            assertTrue(ar.failed());
            assertEquals(DtmException.class, ar.cause().getClass());
            assertEquals("Can't get datamarts", ar.cause().getMessage());
        }).completeNow());
    }

    @Test
    void shouldReturnTrueWhenDatamartExists(VertxTestContext testContext) {
        when(executor.exists(TARGET_PATH)).thenReturn(Future.succeededFuture(true));

        datamartDao.existsDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> assertEquals(true, ar.result())).completeNow());
    }

    @Test
    void shouldReturnCorrectDatamartMeta(VertxTestContext testContext) {
        when(executor.getChildren(ENV_PATH)).thenReturn(Future.succeededFuture(getChildrenList()));

        datamartDao.getDatamartMeta().onComplete(ar -> testContext.verify(() -> assertEquals(getExpectedDatamartInfoList(), ar.result())).completeNow());
    }

    @Test
    void shouldSuccessWhenGetDatamart(VertxTestContext testContext) {
        when(executor.getData(TARGET_PATH)).thenReturn(Future.succeededFuture());

        datamartDao.getDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow());
    }

    @Test
    void shouldFailWhenGetDataDatabaseDoesNotExist(VertxTestContext testContext) {
        when(executor.getData(TARGET_PATH)).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        datamartDao.getDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> {
            assertTrue(ar.failed());
            assertEquals(DatamartNotExistsException.class, ar.cause().getClass());
            assertEquals(ERROR_DATABASE_DOES_NOT_EXIST, ar.cause().getMessage());
        }).completeNow());
    }

    @Test
    void shouldFailWhenUnexpectedErrorOccursInGetData(VertxTestContext testContext) {
        when(executor.getData(TARGET_PATH)).thenReturn(Future.failedFuture(new RuntimeException()));

        datamartDao.getDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> {
            assertTrue(ar.failed());
            assertEquals(DtmException.class, ar.cause().getClass());
            assertEquals("Can't get datamarts [test_dtm]", ar.cause().getMessage());
        }).completeNow());
    }

    @Test
    void shouldSuccessDeleteDatamart(VertxTestContext testContext) {
        when(executor.deleteRecursive(TARGET_PATH)).thenReturn(Future.succeededFuture());

        datamartDao.deleteDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> assertTrue(ar.succeeded())).completeNow());
    }

    @Test
    void shouldFailWhenDeleteDatamartWithIllegalArgument(VertxTestContext testContext) {
        when(executor.deleteRecursive(TARGET_PATH)).thenReturn(Future.failedFuture(new IllegalArgumentException()));

        datamartDao.deleteDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> {
            assertTrue(ar.failed());
            assertEquals(DatamartNotExistsException.class, ar.cause().getClass());
            assertEquals(ERROR_DATABASE_DOES_NOT_EXIST, ar.cause().getMessage());
        }).completeNow());
    }

    @Test
    void shouldFailWhenUnexpectedErrorOccirsInDeleteDatamart(VertxTestContext testContext) {
        when(executor.deleteRecursive(TARGET_PATH)).thenReturn(Future.failedFuture(new RuntimeException()));

        datamartDao.deleteDatamart(DATAMART).onComplete(ar -> testContext.verify(() -> {
            assertTrue(ar.failed());
            assertEquals(DtmException.class, ar.cause().getClass());
            assertEquals("Can't delete datamarts [test_dtm]", ar.cause().getMessage());
        }).completeNow());
    }

    private List<OpResult> getErrorOpResult() {
        return Arrays.asList(new OpResult.ErrorResult(1), new OpResult.ErrorResult(2));
    }

    private List<String> getChildrenList() {
        return Arrays.asList("child_a", "child_b", "child_c");
    }

    private List<DatamartInfo> getExpectedDatamartInfoList() {
        return Arrays.asList(new DatamartInfo("child_a"),
                new DatamartInfo("child_b"),
                new DatamartInfo("child_c"));
    }
}