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
package ru.datamart.prostore.query.execution.core.dml.service.impl;

import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryTemplateResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.dml.DmlType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOpRequest;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.delta.exception.TableBlockedException;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequest;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.dml.service.SqlParametersTypeExtractor;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
class DeleteExecutorTest {

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
    @Mock
    private LogicalSchemaProvider logicalSchemaProvider;
    @Mock
    private QueryTemplateExtractor templateExtractor;
    @Mock
    private QueryParserService queryParserService;
    @Mock
    private SqlParametersTypeExtractor parametersTypeExtractor;
    @Mock
    private QueryTemplateResult templateResult;
    @Mock
    private RelRoot relRoot;
    @Mock
    private QueryParserResponse parserResponse;

    private DeleteExecutor executor;
    private Entity entity;
    private DmlRequestContext requestContext;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);

        executor = new DeleteExecutor(pluginService, serviceDbFacade, restoreStateService, logicalSchemaProvider, templateExtractor, queryParserService, parametersTypeExtractor);

        entity = Entity.builder()
                .schema("datamart")
                .name("users")
                .destination(Collections.singleton(SourceType.ADB))
                .entityType(EntityType.TABLE)
                .build();

        String sql = "DELETE FROM users u where u.id = 1";
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
    void deleteSuccess(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(templateExtractor.extract(any(SqlNode.class))).thenReturn(templateResult);
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(parserResponse.getRelNode()).thenReturn(relRoot);
        when(parametersTypeExtractor.extract(any())).thenReturn(Collections.emptyList());

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(ar -> testContext.failNow(ar.getCause()));
    }

    @Test
    void deleteSuccessWhenResumableOperation(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(templateExtractor.extract(any(SqlNode.class))).thenReturn(templateResult);
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(parserResponse.getRelNode()).thenReturn(relRoot);
        when(parametersTypeExtractor.extract(any())).thenReturn(Collections.emptyList());

        val existWriteOp = DeltaWriteOp.builder()
                .query("DELETE FROM users AS u WHERE u.id = 1")
                .tableName("users")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

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
    void deleteFailWhenNotTableBlockedException(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new RuntimeException("Exception")));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(templateExtractor.extract(any(SqlNode.class))).thenReturn(templateResult);
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(parserResponse.getRelNode()).thenReturn(relRoot);
        when(parametersTypeExtractor.extract(any())).thenReturn(Collections.emptyList());

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
    void deleteFailWhenNotEqualExistWriteOpTable(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(templateExtractor.extract(any(SqlNode.class))).thenReturn(templateResult);
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(parserResponse.getRelNode()).thenReturn(relRoot);
        when(parametersTypeExtractor.extract(any())).thenReturn(Collections.emptyList());

        val existWriteOp = DeltaWriteOp.builder()
                .query("DELETE FROM users AS u WHERE u.id = 1")
                .tableName("unknown")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

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
    void deleteFailWhenNotEqualExistWriteOpStatus(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(templateExtractor.extract(any(SqlNode.class))).thenReturn(templateResult);
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(parserResponse.getRelNode()).thenReturn(relRoot);
        when(parametersTypeExtractor.extract(any())).thenReturn(Collections.emptyList());

        val existWriteOp = DeltaWriteOp.builder()
                .query("DELETE FROM users AS u WHERE u.id = 1")
                .tableName("users")
                .status(WriteOperationStatus.ERROR.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

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
    void deleteFailWhenNotEqualExistWriteOpQuery(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(templateExtractor.extract(any(SqlNode.class))).thenReturn(templateResult);
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(parserResponse.getRelNode()).thenReturn(relRoot);
        when(parametersTypeExtractor.extract(any())).thenReturn(Collections.emptyList());

        val existWriteOp = DeltaWriteOp.builder()
                .query("WRONG")
                .tableName("users")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

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
    void testDeleteFromMatView(VertxTestContext testContext) {
        entity.setEntityType(EntityType.MATERIALIZED_VIEW);

        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed to delete from mat view"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Forbidden. Write operations allowed for logical tables only.", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testWithNoDeltaHotFound(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.failedFuture(new DeltaException("Delta hot not found")));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because delta hot is not found"))
                .onFailure(ar -> testContext.verify(() -> assertEquals("Delta hot not found", ar.getMessage())).completeNow());
    }

    @Test
    void testWhenDatamartHasNoData(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(null));

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(deltaServiceDao).getDeltaHot("datamart");
                    verify(deltaServiceDao).getDeltaOk("datamart");
                    verify(entityDao).getEntity("datamart", "users");
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(ar -> testContext.failNow("Should have been failed because delta ok is not found"));
    }

    @Test
    void testWithNotDeleteNode(VertxTestContext testContext) {
        String sql = "SELECT 1";
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        requestContext.setSqlNode(sqlNode);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because delta hot is not found"))
                .onFailure(ar -> testContext.verify(() -> assertEquals("Unsupported sql node", ar.getMessage())).completeNow());
    }

    @Test
    void testSourceTypeNotConfigured(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(false);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because entity's source type is not configured"))
                .onFailure(ar -> testContext.verify(() -> assertEquals("Plugins: [ADB] for the table [users] datamart [datamart] are not configured", ar.getMessage())).completeNow());
    }

    @Test
    void testPluginLlwFailedWithDtmException(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.failedFuture(new DtmException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(templateExtractor.extract(any(SqlNode.class))).thenReturn(templateResult);
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(parserResponse.getRelNode()).thenReturn(relRoot);
        when(parametersTypeExtractor.extract(any())).thenReturn(Collections.emptyList());

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Llw failed", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testPluginLlwFailedWithUnexpectedExceptionWithMessage(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(templateExtractor.extract(any(SqlNode.class))).thenReturn(templateResult);
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(parserResponse.getRelNode()).thenReturn(relRoot);
        when(parametersTypeExtractor.extract(any())).thenReturn(Collections.emptyList());

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Llw failed", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testPluginLlwFailedWithUnexpectedExceptionWithoutMessage(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
        when(pluginService.delete(eq(SourceType.ADB), any(), any())).thenReturn(Future.failedFuture(new RuntimeException()));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(templateExtractor.extract(any(SqlNode.class))).thenReturn(templateResult);
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(parserResponse.getRelNode()).thenReturn(relRoot);
        when(parametersTypeExtractor.extract(any())).thenReturn(Collections.emptyList());

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertSame(RuntimeException.class, ar.getClass());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testDeltaOkNotFound(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.failedFuture(new DeltaException("Delta ok not found")));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Delta ok not found", ar.getMessage());
                    verify(deltaServiceDao, never()).writeNewOperation(any());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testDatamartsNotFound(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(1L, null, 1L, 1L)));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(logicalSchemaProvider.getSchemaFromQuery(any(), eq("datamart"))).thenReturn(Future.failedFuture(new DtmException("Failed to get schema")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        executor.execute(requestContext)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Failed to get schema", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testDmlType() {
        assertEquals(DmlType.DELETE, executor.getType());
    }

}
