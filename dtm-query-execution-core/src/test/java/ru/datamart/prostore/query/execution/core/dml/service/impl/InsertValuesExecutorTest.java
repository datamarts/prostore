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
package ru.datamart.prostore.query.execution.core.dml.service.impl;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.dml.DmlType;
import ru.datamart.prostore.query.execution.core.base.exception.table.ValidationDtmException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOpRequest;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.delta.exception.TableBlockedException;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequest;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.dml.service.impl.validate.BasicUpdateColumnsValidator;
import ru.datamart.prostore.query.execution.core.dml.service.impl.validate.WithNullableCheckUpdateColumnsValidator;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
class InsertValuesExecutorTest {

    private static final String INSERT_INTO_SQL = "INSERT INTO users(id, name) values(1, 'Name')";
    private static final String RETRY_INSERT_INTO_SQL = "RETRY INSERT INTO users(id, name) values(1, 'Name')";

    @Mock
    private DataSourcePluginService pluginService;
    @Mock
    private EntityDao entityDao;
    @Mock
    private DeltaServiceDao deltaServiceDao;
    @Mock
    private RestoreStateService restoreStateService;
    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private ServiceDbDao serviceDbDao;

    private InsertValuesExecutor executor;
    private Entity logicalEntity;
    private Entity writeableExternalEntity;
    private DmlRequestContext requestContext;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);

        val updateColumnsValidator = new WithNullableCheckUpdateColumnsValidator(new BasicUpdateColumnsValidator());
        executor = new InsertValuesExecutor(pluginService, serviceDbFacade, restoreStateService, updateColumnsValidator);
        logicalEntity = Entity.builder()
                .schema("datamart")
                .name("users")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .type(ColumnType.INT)
                                .primaryOrder(1)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("name")
                                .type(ColumnType.VARCHAR)
                                .nullable(true)
                                .build()))
                .destination(Collections.singleton(SourceType.ADB))
                .entityType(EntityType.TABLE)
                .build();
        writeableExternalEntity = Entity.builder()
                .schema("datamart")
                .name("users")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .type(ColumnType.INT)
                                .primaryOrder(1)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("name")
                                .type(ColumnType.VARCHAR)
                                .nullable(true)
                                .build()))
                .destination(Collections.singleton(SourceType.ADB))
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .build();

        String sql = INSERT_INTO_SQL;
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .sql(sql)
                .build();
        requestContext = DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();
    }

    @Test
    void insertSuccess(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void insertSuccessForWriteableExternalTable(VertxTestContext testContext) {
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(writeableExternalEntity));
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.verify(() -> {
                    verifyNoInteractions(deltaServiceDao, restoreStateService);
                }).completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void insertSuccessWhenRetryAndResumableOperation(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name) VALUES ca37a04282d9b18bf2a47122618b1594")
                .tableName("users")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));
        requestContext.setSqlNode(TestUtils.DEFINITION_SERVICE.processingQuery(RETRY_INSERT_INTO_SQL));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).getDeltaWriteOperations("datamart");
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void insertSuccessWhenNotRetryAndResumableOperation(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException("Exception"))));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name) VALUES ca37a04282d9b18bf2a47122618b1594")
                .tableName("users")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        executor.execute(requestContext)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals("Table[tbl] blocked: Exception", ar.cause().getMessage());
                    verify(deltaServiceDao, never()).getDeltaWriteOperations("datamart");
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void insertFailWhenNotTableBlockedException(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new RuntimeException("Exception")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name) VALUES ca37a04282d9b18bf2a47122618b1594")
                .tableName("unknown")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Unexpected success"))
                .onFailure(t -> testContext.verify(() -> {
                    assertEquals("Exception", t.getMessage());
                    verify(deltaServiceDao, never()).getDeltaWriteOperations("datamart");
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                    verify(pluginService).hasSourceType(any());
                    verifyNoMoreInteractions(pluginService);
                }).completeNow());
    }

    @Test
    void insertFailWhenRetryAndNotEqualExistWriteOpTable(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name) VALUES ca37a04282d9b18bf2a47122618b1594")
                .tableName("unknown")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));
        requestContext.setSqlNode(TestUtils.DEFINITION_SERVICE.processingQuery(RETRY_INSERT_INTO_SQL));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Unexpected success"))
                .onFailure(t -> testContext.verify(() -> {
                    assertEquals("Table blocked and could not find equal writeOp for resume", t.getMessage());
                    verify(deltaServiceDao).getDeltaWriteOperations("datamart");
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                    verify(pluginService).hasSourceType(any());
                    verifyNoMoreInteractions(pluginService);
                }).completeNow());
    }

    @Test
    void insertFailWhenNotRetryAndNotEqualExistWriteOpTable(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException("Exception"))));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name) VALUES ca37a04282d9b18bf2a47122618b1594")
                .tableName("unknown")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Unexpected success"))
                .onFailure(t -> testContext.verify(() -> {
                    assertEquals("Table[tbl] blocked: Exception", t.getMessage());
                    verify(deltaServiceDao, never()).getDeltaWriteOperations("datamart");
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                    verify(pluginService).hasSourceType(any());
                    verifyNoMoreInteractions(pluginService);
                }).completeNow());
    }

    @Test
    void insertFailWhenRetryAndNotEqualExistWriteOpStatus(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name) VALUES ca37a04282d9b18bf2a47122618b1594")
                .tableName("users")
                .status(WriteOperationStatus.ERROR.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));
        requestContext.setSqlNode(TestUtils.DEFINITION_SERVICE.processingQuery(RETRY_INSERT_INTO_SQL));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Unexpected success"))
                .onFailure(t -> testContext.verify(() -> {
                    assertEquals("Table blocked and could not find equal writeOp for resume", t.getMessage());
                    verify(deltaServiceDao).getDeltaWriteOperations("datamart");
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                    verify(pluginService).hasSourceType(any());
                    verifyNoMoreInteractions(pluginService);
                }).completeNow());
    }

    @Test
    void insertFailWhenNotRetryAndNotEqualExistWriteOpStatus(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException("Exception"))));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name) VALUES ca37a04282d9b18bf2a47122618b1594")
                .tableName("users")
                .status(WriteOperationStatus.ERROR.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Unexpected success"))
                .onFailure(t -> testContext.verify(() -> {
                    assertEquals("Table[tbl] blocked: Exception", t.getMessage());
                    verify(deltaServiceDao, never()).getDeltaWriteOperations("datamart");
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                    verify(pluginService).hasSourceType(any());
                    verifyNoMoreInteractions(pluginService);
                }).completeNow());
    }

    @Test
    void insertFailWhenRetryAndNotEqualExistWriteOpQuery(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name) VALUES NOT_RIGHT")
                .tableName("users")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));
        requestContext.setSqlNode(TestUtils.DEFINITION_SERVICE.processingQuery(RETRY_INSERT_INTO_SQL));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Unexpected success"))
                .onFailure(t -> testContext.verify(() -> {
                    assertEquals("Table blocked and could not find equal writeOp for resume", t.getMessage());
                    verify(deltaServiceDao).getDeltaWriteOperations("datamart");
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                    verify(pluginService).hasSourceType(any());
                    verifyNoMoreInteractions(pluginService);
                }).completeNow());
    }

    @Test
    void insertFailWhenNotRetryAndNotEqualExistWriteOpQuery(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException("Exception"))));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name) VALUES NOT_RIGHT")
                .tableName("users")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Unexpected success"))
                .onFailure(t -> testContext.verify(() -> {
                    assertEquals("Table[tbl] blocked: Exception", t.getMessage());
                    verify(deltaServiceDao, never()).getDeltaWriteOperations("datamart");
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                    verify(pluginService).hasSourceType(any());
                    verifyNoMoreInteractions(pluginService);
                }).completeNow());
    }

    @Test
    void insertIntoMatView(VertxTestContext testContext) {
        logicalEntity.setEntityType(EntityType.MATERIALIZED_VIEW);

        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed to insert into mat view"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Forbidden. Write operations allowed for logical and writeable external tables only.", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void insertSuccessWithoutColumns(VertxTestContext testContext) {
        String sql = "INSERT INTO users values(1, 'Name')";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .sql(sql)
                .build();
        DmlRequestContext context = DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();

        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(context)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void insertSuccessWithoutColumnsForWriteableExternalTable(VertxTestContext testContext) {
        String sql = "INSERT INTO users values(1, 'Name')";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .sql(sql)
                .build();
        DmlRequestContext context = DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();

        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(writeableExternalEntity));
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(context)
                .onSuccess(ar -> testContext.verify(() -> {
                    verifyNoInteractions(deltaServiceDao, restoreStateService);
                }).completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void insertWithNonExistedColumn(VertxTestContext testContext) {
        String sql = "INSERT INTO users(id, name, non_existed_column) values(1, 'Name')";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .sql(sql)
                .build();
        DmlRequestContext context = DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();

        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query contains non existed column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void insertWithSystemColumn(VertxTestContext testContext) {
        String sql = "INSERT INTO users(id, name, sys_from) values(1, 'Name')";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .sql(sql)
                .build();
        DmlRequestContext context = DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();

        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query contains non existed column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void insertWithoutNotNullColumn(VertxTestContext testContext) {
        String sql = "INSERT INTO users(name) values('Name')";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .sql(sql)
                .build();
        DmlRequestContext context = DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();

        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query doesn't contains non nullable column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void testWithNoDeltaHotFound(VertxTestContext testContext) {
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.failedFuture(new DeltaException("Delta hot not found")));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because delta hot is not found"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Delta hot not found", error.getMessage())).completeNow());
    }

    @Test
    void testSourceTypeNotConfigured(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(false);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because entity's source type is not configured"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Plugins: [ADB] for the table [users] datamart [datamart] are not configured", error.getMessage())).completeNow());
    }

    @Test
    void testNotValuesSource(VertxTestContext testContext) {
        String sql = "INSERT INTO users(id, name) SELECT 1";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        requestContext.setSqlNode(sqlNode);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because not insert sql node"))
                .onFailure(ar -> testContext.verify(() -> assertEquals("Invalid source for [INSERT_VALUES]", ar.getMessage())).completeNow());
    }

    @Test
    void testNotInsert(VertxTestContext testContext) {
        String sql = "SELECT 1";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        requestContext.setSqlNode(sqlNode);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because not insert sql node"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Unsupported sql node", error.getMessage())).completeNow());
    }

    @Test
    void testPluginLlwFailedWithDtmException(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.failedFuture(new DtmException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Llw failed", error.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testPluginLlwFailedWithDtmExceptionForWriteableExternalTable(VertxTestContext testContext) {
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(writeableExternalEntity));
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.failedFuture(new DtmException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Llw failed", error.getMessage());
                    verifyNoInteractions(deltaServiceDao, restoreStateService);
                }).completeNow());
    }

    @Test
    void testPluginLlwFailedWithUnexpectedExceptionWithMessage(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.failedFuture(new RuntimeException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Llw failed", error.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testPluginLlwFailedWithUnexpectedExceptionWithMessageForWriteableExternalTable(VertxTestContext testContext) {
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(writeableExternalEntity));
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.failedFuture(new RuntimeException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Llw failed", error.getMessage());
                    verifyNoInteractions(deltaServiceDao, restoreStateService);
                }).completeNow());
    }

    @Test
    void testPluginLlwFailedWithUnexpectedExceptionWithoutMessage(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(logicalEntity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.failedFuture(new RuntimeException()));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertSame(RuntimeException.class, error.getClass());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testPluginLlwFailedWithUnexpectedExceptionWithoutMessageForWriteableExternalTable(VertxTestContext testContext) {
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(writeableExternalEntity));
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertValuesRequest.class))).thenReturn(Future.failedFuture(new RuntimeException()));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertSame(RuntimeException.class, error.getClass());
                    verifyNoInteractions(deltaServiceDao, restoreStateService);
                }).completeNow());
    }

    @Test
    void testGetType() {
        assertEquals(DmlType.INSERT_VALUES, executor.getType());
    }

}
