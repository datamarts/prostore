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
package ru.datamart.prostore.query.execution.core.ddl.changes;

import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlAllowChanges;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ChangesDao;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.changes.AllowChangesExecutor;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.NlsString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class ChangesExecutorTest {

    private static final String CONTEXT_DATAMART = "context_datamart";
    private static final String QUERY_DATAMART = "query_datamart";
    private final DatamartRequest datamartRequest = new DatamartRequest(QueryRequest.builder()
            .datamartMnemonic(CONTEXT_DATAMART)
            .build());

    @Mock
    private ChangesDao changesDao;

    @InjectMocks
    private AllowChangesExecutor changesExecutor;

    @Test
    void executeSuccess(VertxTestContext testContext) {
        //prepare
        val sqlAllowChanges = new SqlAllowChanges(SqlParserPos.ZERO, null, null);
        val context = new DdlRequestContext(null, datamartRequest, null, null, null);
        context.setSqlCall(sqlAllowChanges);

        when(changesDao.allowChanges(CONTEXT_DATAMART, "")).thenReturn(Future.succeededFuture());
        //act
        changesExecutor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void executeSuccessWithOnlyDatamart(VertxTestContext testContext) {
        //prepare
        val sqlAllowChanges = new SqlAllowChanges(SqlParserPos.ZERO, new SqlIdentifier(QUERY_DATAMART, SqlParserPos.ZERO), null);
        val context = new DdlRequestContext(null, datamartRequest, null, null, null);
        context.setSqlCall(sqlAllowChanges);

        when(changesDao.allowChanges(QUERY_DATAMART, "")).thenReturn(Future.succeededFuture());
        //act
        changesExecutor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void executeSuccessWithOnlyDenyCode(VertxTestContext testContext) {
        //prepare
        val denyCode = "aBc_123";
        val sqlCharStringLiteral = mock(SqlCharStringLiteral.class);
        val nlsString = mock(NlsString.class);
        when(sqlCharStringLiteral.getNlsString()).thenReturn(nlsString);
        when(nlsString.getValue()).thenReturn(denyCode);
        val sqlAllowChanges = new SqlAllowChanges(SqlParserPos.ZERO, null, sqlCharStringLiteral);
        val context = new DdlRequestContext(null, datamartRequest, null, null, null);
        context.setSqlCall(sqlAllowChanges);

        when(changesDao.allowChanges(CONTEXT_DATAMART, denyCode)).thenReturn(Future.succeededFuture());
        //act
        changesExecutor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void executeSuccessWithDatamartAndDenyCode(VertxTestContext testContext) {
        //prepare
        val denyCode = "aBc_123";
        val sqlCharStringLiteral = mock(SqlCharStringLiteral.class);
        val nlsString = mock(NlsString.class);
        when(sqlCharStringLiteral.getNlsString()).thenReturn(nlsString);
        when(nlsString.getValue()).thenReturn(denyCode);
        val sqlAllowChanges = new SqlAllowChanges(SqlParserPos.ZERO, new SqlIdentifier(QUERY_DATAMART, SqlParserPos.ZERO), sqlCharStringLiteral);
        val context = new DdlRequestContext(null, datamartRequest, null, null, null);
        context.setSqlCall(sqlAllowChanges);

        when(changesDao.allowChanges(QUERY_DATAMART, denyCode)).thenReturn(Future.succeededFuture());
        //act
        changesExecutor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void executeDaoError(VertxTestContext testContext) {
        //prepare
        val sqlAllowChanges = new SqlAllowChanges(SqlParserPos.ZERO, null, null);
        val context = new DdlRequestContext(null, datamartRequest, null, null, null);
        context.setSqlCall(sqlAllowChanges);

        when(changesDao.allowChanges(CONTEXT_DATAMART, "")).thenReturn(Future.failedFuture("error"));
        //act
        changesExecutor.execute(context, null)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                }).completeNow());
    }
}
