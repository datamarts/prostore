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
package ru.datamart.prostore.query.execution.core.check;

import ru.datamart.prostore.common.model.ddl.Changelog;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlGetChanges;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ChangelogDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.service.impl.GetChangesExecutor;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class GetChangesExecutorTest {

    private static final String OPERATION_NUM = "change_num";
    private static final String ENTITY_NAME = "entity_name";
    private static final String CHANGE_QUERY = "change_query";
    private static final String DATE_TIME_START = "date_time_start";
    private static final String DATE_TIME_END = "date_time_end";
    private static final String DELTA_NUM = "delta_num";

    private static final String CONTEXT_DATAMART = "context_datamart";
    private static final String QUERY_DATAMART = "query_datamart";

    private static final Changelog CHANGELOG = Changelog.builder()
            .operationNumber(0L)
            .entityName("accounts")
            .changeQuery("changeQuery")
            .dateTimeStart("2021-12-01 11:55:53")
            .dateTimeEnd("2021-12-01 11:55:54")
            .deltaNum(null)
            .build();

    private static final Changelog PARENT_CHANGELOG = Changelog.builder()
            .operationNumber(0L)
            .entityName("accounts")
            .changeQuery("changeQuery")
            .dateTimeStart("2021-12-01 11:55:53")
            .dateTimeEnd(null)
            .deltaNum(null)
            .build();

    private final DatamartRequest datamartRequest = new DatamartRequest(QueryRequest.builder()
            .datamartMnemonic(CONTEXT_DATAMART)
            .build());

    @Mock
    private DatamartDao datamartDao;
    @Mock
    private ChangelogDao changelogDao;
    @InjectMocks
    private GetChangesExecutor getChangesExecutor;

    @Test
    void executeSuccessWithQueryDatamart(VertxTestContext testContext) {
        //prepare
        val sqlGetChanges = new SqlGetChanges(SqlParserPos.ZERO, new SqlIdentifier(QUERY_DATAMART, SqlParserPos.ZERO));
        val context = new CheckContext(null, null, datamartRequest, CheckType.CHANGES, null);
        context.setSqlNode(sqlGetChanges);

        when(datamartDao.existsDatamart(QUERY_DATAMART)).thenReturn(Future.succeededFuture(true));
        when(changelogDao.getChanges(QUERY_DATAMART)).thenReturn(Future.succeededFuture(Collections.singletonList(CHANGELOG)));

        getChangesExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    val result = ar.result().getResult();
                    assertEquals(1, result.size());
                    assertEquals(CHANGELOG.getOperationNumber(), result.get(0).get(OPERATION_NUM));
                    assertEquals(CHANGELOG.getEntityName(), result.get(0).get(ENTITY_NAME));
                    assertEquals(CHANGELOG.getChangeQuery(), result.get(0).get(CHANGE_QUERY));
                    assertEquals(1638359753000000L, result.get(0).get(DATE_TIME_START));
                    assertEquals(1638359754000000L, result.get(0).get(DATE_TIME_END));
                    assertEquals(CHANGELOG.getDeltaNum(), result.get(0).get(DELTA_NUM));
                }).completeNow());
    }

    @Test
    void executeSuccessWithContextDatamart(VertxTestContext testContext) {
        //prepare
        val sqlGetChanges = new SqlGetChanges(SqlParserPos.ZERO, null);
        val context = new CheckContext(null, null, datamartRequest, CheckType.CHANGES, null);
        context.setSqlNode(sqlGetChanges);

        when(datamartDao.existsDatamart(CONTEXT_DATAMART)).thenReturn(Future.succeededFuture(true));
        when(changelogDao.getChanges(CONTEXT_DATAMART)).thenReturn(Future.succeededFuture(Collections.singletonList(CHANGELOG)));

        getChangesExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    val result = ar.result().getResult();
                    assertEquals(1, result.size());
                    assertEquals(CHANGELOG.getOperationNumber(), result.get(0).get(OPERATION_NUM));
                    assertEquals(CHANGELOG.getEntityName(), result.get(0).get(ENTITY_NAME));
                    assertEquals(CHANGELOG.getChangeQuery(), result.get(0).get(CHANGE_QUERY));
                    assertEquals(1638359753000000L, result.get(0).get(DATE_TIME_START));
                    assertEquals(1638359754000000L, result.get(0).get(DATE_TIME_END));
                    assertEquals(CHANGELOG.getDeltaNum(), result.get(0).get(DELTA_NUM));
                }).completeNow());
    }

    @Test
    void executeSuccessWithNullEndTime(VertxTestContext testContext) {
        //prepare
        val sqlGetChanges = new SqlGetChanges(SqlParserPos.ZERO, null);
        val context = new CheckContext(null, null, datamartRequest, CheckType.CHANGES, null);
        context.setSqlNode(sqlGetChanges);

        when(datamartDao.existsDatamart(CONTEXT_DATAMART)).thenReturn(Future.succeededFuture(true));
        when(changelogDao.getChanges(CONTEXT_DATAMART)).thenReturn(Future.succeededFuture(Collections.singletonList(PARENT_CHANGELOG)));

        getChangesExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    val result = ar.result().getResult();
                    assertEquals(1, result.size());
                    assertEquals(CHANGELOG.getOperationNumber(), result.get(0).get(OPERATION_NUM));
                    assertEquals(CHANGELOG.getEntityName(), result.get(0).get(ENTITY_NAME));
                    assertEquals(CHANGELOG.getChangeQuery(), result.get(0).get(CHANGE_QUERY));
                    assertEquals(1638359753000000L, result.get(0).get(DATE_TIME_START));
                    assertNull(result.get(0).get(DATE_TIME_END));
                    assertEquals(CHANGELOG.getDeltaNum(), result.get(0).get(DELTA_NUM));
                }).completeNow());
    }

    @Test
    void executeErrorDatamartNotExists(VertxTestContext testContext) {
        //prepare
        val sqlGetChanges = new SqlGetChanges(SqlParserPos.ZERO, null);
        val context = new CheckContext(null, null, datamartRequest, CheckType.CHANGES, null);
        context.setSqlNode(sqlGetChanges);

        when(datamartDao.existsDatamart(CONTEXT_DATAMART)).thenReturn(Future.succeededFuture(false));

        getChangesExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DatamartNotExistsException);
                }).completeNow());
    }

    @Test
    void executeGetChangesError(VertxTestContext testContext) {
        //prepare
        val sqlGetChanges = new SqlGetChanges(SqlParserPos.ZERO, new SqlIdentifier(QUERY_DATAMART, SqlParserPos.ZERO));
        val context = new CheckContext(null, null, datamartRequest, CheckType.CHANGES, null);
        context.setSqlNode(sqlGetChanges);

        when(datamartDao.existsDatamart(QUERY_DATAMART)).thenReturn(Future.succeededFuture(true));
        when(changelogDao.getChanges(QUERY_DATAMART)).thenReturn(Future.failedFuture("error"));

        getChangesExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals("error", ar.cause().getMessage());
                }).completeNow());
    }
}
