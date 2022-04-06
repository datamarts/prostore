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
package ru.datamart.prostore.query.execution.core.ddl;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Changelog;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.ddl.EraseChangeOperation;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ChangelogDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.EraseChangeOperationExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class EraseChangeOperationExecutorTest {

    private static final String OPERATION_NUM_COLUMN = "change_num";
    private static final String ERROR_MSG = "error";

    private static final String DATAMART = "dtm";
    private static final String CONTEXT_DATAMART = "ctx_dtm";
    private static final Long OPERATION_NUM = 1L;
    private static final Changelog CHANGELOG = Changelog.builder()
            .operationNumber(OPERATION_NUM)
            .entityName("accounts")
            .changeQuery("changeQuery")
            .dateTimeStart("2021-12-01 11:55:53")
            .dateTimeEnd("2021-12-01 11:55:54")
            .deltaNum(null)
            .build();

    @Mock
    private EraseChangeOperation eraseChangeOperation;

    @Mock
    private DatamartDao datamartDao;

    @Mock
    private ChangelogDao changelogDao;

    @InjectMocks
    private EraseChangeOperationExecutor executor;

    @Test
    void shouldSuccess(VertxTestContext testContext) {
        val context = new DdlRequestContext(null, null, null, null, null);
        context.setSqlNode(eraseChangeOperation);

        when(eraseChangeOperation.getDatamart()).thenReturn(DATAMART);
        when(eraseChangeOperation.getChangeOperationNumber()).thenReturn(OPERATION_NUM);

        when(datamartDao.existsDatamart(DATAMART)).thenReturn(Future.succeededFuture(true));
        when(changelogDao.eraseChangeOperation(DATAMART, OPERATION_NUM)).thenReturn(Future.succeededFuture(CHANGELOG));

        executor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    val result = ar.result().getResult();
                    assertEquals(1, result.size());
                    assertEquals(OPERATION_NUM, result.get(0).get(OPERATION_NUM_COLUMN));
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenContextDatamart(VertxTestContext testContext) {
        val context = new DdlRequestContext(null, new DatamartRequest(QueryRequest.builder()
                .datamartMnemonic(CONTEXT_DATAMART)
                .build()), null, null, null);
        context.setSqlNode(eraseChangeOperation);

        when(eraseChangeOperation.getDatamart()).thenReturn(null);
        when(eraseChangeOperation.getChangeOperationNumber()).thenReturn(OPERATION_NUM);

        when(datamartDao.existsDatamart(CONTEXT_DATAMART)).thenReturn(Future.succeededFuture(true));
        when(changelogDao.eraseChangeOperation(CONTEXT_DATAMART, OPERATION_NUM)).thenReturn(Future.succeededFuture(CHANGELOG));

        executor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    val result = ar.result().getResult();
                    assertEquals(1, result.size());
                    assertEquals(OPERATION_NUM, result.get(0).get(OPERATION_NUM_COLUMN));
                }).completeNow());
    }

    @Test
    void shouldFailWhenChangelogDaoFailed(VertxTestContext testContext) {
        val context = new DdlRequestContext(null, null, null, null, null);
        context.setSqlNode(eraseChangeOperation);

        when(eraseChangeOperation.getDatamart()).thenReturn(DATAMART);
        when(eraseChangeOperation.getChangeOperationNumber()).thenReturn(OPERATION_NUM);

        when(datamartDao.existsDatamart(DATAMART)).thenReturn(Future.succeededFuture(true));
        when(changelogDao.eraseChangeOperation(DATAMART, OPERATION_NUM)).thenReturn(Future.failedFuture(ERROR_MSG));

        executor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenExistsDatamartFailed(VertxTestContext testContext) {
        val context = new DdlRequestContext(null, null, null, null, null);
        context.setSqlNode(eraseChangeOperation);

        when(eraseChangeOperation.getDatamart()).thenReturn(DATAMART);
        when(eraseChangeOperation.getChangeOperationNumber()).thenReturn(OPERATION_NUM);

        when(datamartDao.existsDatamart(DATAMART)).thenReturn(Future.failedFuture(ERROR_MSG));

        executor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenDatamartNotExistFailed(VertxTestContext testContext) {
        val context = new DdlRequestContext(null, null, null, null, null);
        context.setSqlNode(eraseChangeOperation);

        when(eraseChangeOperation.getDatamart()).thenReturn(DATAMART);
        when(eraseChangeOperation.getChangeOperationNumber()).thenReturn(OPERATION_NUM);

        when(datamartDao.existsDatamart(DATAMART)).thenReturn(Future.succeededFuture(false));

        executor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals("Database dtm does not exist", ar.cause().getMessage());
                }).completeNow());
    }
}
