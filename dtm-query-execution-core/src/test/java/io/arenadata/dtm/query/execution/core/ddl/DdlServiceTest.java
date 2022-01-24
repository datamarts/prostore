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
package io.arenadata.dtm.query.execution.core.ddl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.OperationNames;
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.DdlExecutor;
import io.arenadata.dtm.query.execution.core.ddl.service.DdlService;
import io.arenadata.dtm.query.execution.core.ddl.utils.ParseQueryUtils;
import io.arenadata.dtm.query.execution.plugin.api.service.PostExecutor;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DdlServiceTest {

    private static final String ENV = "test";
    private static final String INFORMATION_SCHEMA = "information_schema";
    private static final String INFORMATION_SCHEMA_ENTITIES = "information_schema.entities";
    private static final String DTM_TBL = "dtm.tbl";
    private static final String ERROR_MESSAGE = "ERROR";

    private final ParseQueryUtils parseQueryUtils = mock(ParseQueryUtils.class);
    private final PostExecutor<DdlRequestContext> postExecutor = mock(PostExecutor.class);
    private final DdlExecutor ddlExecutor = mock(DdlExecutor.class);
    private final SqlCreateDatabase sqlCreateDatabase = mock(SqlCreateDatabase.class);
    private final SqlCreateTable sqlCreateTable = mock(SqlCreateTable.class);
    private final SqlOperator sqlOperator = mock(SqlOperator.class);
    private final QueryResult result = QueryResult.builder()
            .requestId(UUID.randomUUID())
            .build();

    private DdlRequestContext context;
    private DdlService ddlService;

    @BeforeEach
    void setUp() {
        when(ddlExecutor.getOperationKind()).thenReturn(OperationNames.CREATE_TABLE);
        when(postExecutor.getPostActionType()).thenReturn(PostSqlActionType.UPDATE_INFORMATION_SCHEMA);
        when(sqlCreateTable.getOperator()).thenReturn(sqlOperator);
        when(sqlOperator.getName()).thenReturn(OperationNames.CREATE_TABLE);
        when(sqlCreateDatabase.getOperator()).thenReturn(sqlOperator);
        when(sqlOperator.getName()).thenReturn(OperationNames.CREATE_TABLE);

        context = new DdlRequestContext(null,
                new DatamartRequest(QueryRequest.builder()
                        .datamartMnemonic("dtm")
                        .build()),
                sqlCreateDatabase, SourceType.ADB, ENV);
        ddlService = new DdlService(parseQueryUtils,
                Collections.singletonList(postExecutor),
                Collections.singletonList(ddlExecutor));
    }

    @Test
    void shouldSucceed() {
        //arrange
        context.setSqlNode(sqlCreateTable);
        when(parseQueryUtils.getDatamartName(anyList())).thenReturn(DTM_TBL);
        when(ddlExecutor.execute(any(), anyString())).thenReturn(Future.succeededFuture(result));
        when(postExecutor.execute(any())).thenReturn(Future.succeededFuture());

        //act
        ddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(result.getRequestId(), ar.result().getRequestId());
                });
    }

    @Test
    void shouldFailWhenDdlExecutorError() {
        //arrange
        context.setSqlNode(sqlCreateTable);
        when(parseQueryUtils.getDatamartName(anyList())).thenReturn(DTM_TBL);
        when(ddlExecutor.execute(any(), anyString())).thenReturn(Future.failedFuture(new DtmException(ERROR_MESSAGE)));

        //act
        ddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(DtmException.class, ar.cause().getClass());
                    assertEquals(ERROR_MESSAGE, ar.cause().getMessage());
                });
    }

    @Test
    void shouldSucceedWhenPostExecutorError() {
        //arrange
        context.setSqlNode(sqlCreateTable);
        when(parseQueryUtils.getDatamartName(anyList())).thenReturn(DTM_TBL);
        when(ddlExecutor.execute(any(), anyString())).thenReturn(Future.succeededFuture(result));
        when(postExecutor.execute(any())).thenReturn(Future.failedFuture(new DtmException(ERROR_MESSAGE)));

        //act
        ddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(result.getRequestId(), ar.result().getRequestId());
                });
    }

    @Test
    void shouldFailWhenCreateDatabaseInformationSchema() {
        //arrange
        context.getRequest().getQueryRequest().setDatamartMnemonic(null);
        when(parseQueryUtils.getDatamartName(anyList())).thenReturn(INFORMATION_SCHEMA);
        SqlIdentifier identifier = mock(SqlIdentifier.class);
        when(sqlCreateDatabase.getName()).thenReturn(identifier);
        when(identifier.getSimple()).thenReturn(INFORMATION_SCHEMA);

        //act
        ddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(ValidationDtmException.class, ar.cause().getClass());
                });
    }

    @Test
    void shouldFailWhenCreateTableInInformationSchema() {
        //arrange
        context.setSqlNode(sqlCreateTable);
        context.getRequest().getQueryRequest().setDatamartMnemonic(INFORMATION_SCHEMA);
        when(parseQueryUtils.getDatamartName(anyList())).thenReturn(INFORMATION_SCHEMA_ENTITIES);

        //act
        ddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(ValidationDtmException.class, ar.cause().getClass());
                });
    }

    @Test
    void shouldFailWhenCreateDatabaseInvalidNameWithDot() {
        //arrange
        when(parseQueryUtils.getDatamartName(anyList())).thenReturn("datamart.datamart");

        //act
        ddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(ValidationDtmException.class, ar.cause().getClass());
                });
    }

    @Test
    void shouldFailWhenCreateDatabaseInvalidNameStartWithUnderscore() {
        //arrange
        when(parseQueryUtils.getDatamartName(anyList())).thenReturn("_datamart");

        //act
        ddlService.execute(context)
                .onComplete(ar -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(ValidationDtmException.class, ar.cause().getClass());
                });
    }
}
