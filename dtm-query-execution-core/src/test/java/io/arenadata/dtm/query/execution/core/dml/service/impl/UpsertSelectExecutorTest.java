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
package io.arenadata.dtm.query.execution.core.dml.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationExtractor;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationService;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteContextProvider;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreQueryTemplateExtractor;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaException;
import io.arenadata.dtm.query.execution.core.delta.exception.TableBlockedException;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequest;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dml.dto.PluginDeterminationRequest;
import io.arenadata.dtm.query.execution.core.dml.dto.PluginDeterminationResult;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.dml.service.PluginDeterminationService;
import io.arenadata.dtm.query.execution.core.dml.service.SqlParametersTypeExtractor;
import io.arenadata.dtm.query.execution.core.dml.service.view.LogicViewReplacer;
import io.arenadata.dtm.query.execution.core.dml.service.view.MaterializedViewReplacer;
import io.arenadata.dtm.query.execution.core.dml.service.view.ViewReplacerService;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.arenadata.dtm.query.execution.core.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.UpsertSelectRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class UpsertSelectExecutorTest {

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
    private PluginDeterminationService pluginDeterminationService;

    private UpsertSelectExecutor upsertExecutor;
    private Entity entity;
    private Entity badUsersView;

    @BeforeEach
    public void setUp(Vertx vertx) {
        MockitoAnnotations.initMocks(this);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);

        val deltaInformationExtractor = new DeltaInformationExtractor();
        val deltaQueryPreprocessor = new DeltaQueryPreprocessor(new DeltaInformationService(serviceDbFacade), deltaInformationExtractor, entityDao);
        val calciteConfiguration = new CalciteConfiguration();
        val sqlParserFactory = calciteConfiguration.getSqlParserFactory();
        val configParser = calciteConfiguration.configEddlParser(sqlParserFactory);
        val schemaFactory = new CoreSchemaFactory();
        val calciteSchemaFactory = new CoreCalciteSchemaFactory(schemaFactory);
        val contextProvider = new CoreCalciteContextProvider(configParser, calciteSchemaFactory);
        val queryParserService = new CoreCalciteDMLQueryParserService(contextProvider, vertx);
        val columnMetadataService = new ColumnMetadataService(queryParserService);
        val definitionService = new CoreCalciteDefinitionService(configParser);
        val logicalSchemaProvider = new LogicalSchemaProvider(new LogicalSchemaService(serviceDbFacade, deltaInformationExtractor));
        val viewReplacerService = new ViewReplacerService(entityDao, new LogicViewReplacer(definitionService), Mockito.mock(MaterializedViewReplacer.class));
        val templateExtractor = new CoreQueryTemplateExtractor(definitionService, calciteConfiguration.coreSqlDialect());
        val parametersTypeExtractor = new SqlParametersTypeExtractor();
        upsertExecutor = new UpsertSelectExecutor(pluginService, serviceDbFacade, restoreStateService, logicalSchemaProvider, deltaQueryPreprocessor, queryParserService, columnMetadataService, viewReplacerService, pluginDeterminationService, templateExtractor, parametersTypeExtractor);

        val fields = Arrays.asList(
                EntityField.builder()
                        .name("id")
                        .ordinalPosition(0)
                        .type(ColumnType.INT)
                        .primaryOrder(0)
                        .nullable(false)
                        .build(),
                EntityField.builder()
                        .name("name")
                        .ordinalPosition(1)
                        .type(ColumnType.VARCHAR)
                        .nullable(true)
                        .size(10)
                        .build(),
                EntityField.builder()
                        .name("time_col")
                        .ordinalPosition(2)
                        .type(ColumnType.TIME)
                        .nullable(true)
                        .accuracy(6)
                        .build(),
                EntityField.builder()
                        .name("timestamp_col")
                        .ordinalPosition(3)
                        .type(ColumnType.TIMESTAMP)
                        .nullable(true)
                        .accuracy(6)
                        .build(),
                EntityField.builder()
                        .name("uuid_col")
                        .ordinalPosition(4)
                        .type(ColumnType.UUID)
                        .nullable(true)
                        .build());

        entity = Entity.builder()
                .schema("datamart")
                .name("users")
                .fields(fields)
                .destination(Collections.singleton(SourceType.ADB))
                .entityType(EntityType.TABLE)
                .build();

        Entity badUsersEntity = Entity.builder()
                .name("badusers")
                .schema("datamart2")
                .destination(new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG, SourceType.ADP, SourceType.ADQM)))
                .entityType(EntityType.TABLE)
                .fields(fields)
                .build();

        badUsersView = Entity.builder()
                .name("badusers_view")
                .schema("datamart2")
                .destination(new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG, SourceType.ADP, SourceType.ADQM)))
                .entityType(EntityType.VIEW)
                .viewQuery("SELECT * FROM datamart2.badusers")
                .fields(fields)
                .build();

        Entity banned = Entity.builder()
                .name("banned")
                .schema("datamart")
                .destination(new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG, SourceType.ADP, SourceType.ADQM)))
                .entityType(EntityType.TABLE)
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .type(ColumnType.INT)
                                .primaryOrder(0)
                                .ordinalPosition(0)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("userid")
                                .type(ColumnType.INT)
                                .ordinalPosition(1)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("rating")
                                .type(ColumnType.INT32)
                                .ordinalPosition(2)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("uuid_col")
                                .type(ColumnType.UUID)
                                .size(16)
                                .ordinalPosition(2)
                                .nullable(true)
                                .build()
                ))
                .build();

        lenient().when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        lenient().when(entityDao.getEntity(eq("datamart2"), eq("badusers"))).thenReturn(Future.succeededFuture(badUsersEntity));
        lenient().when(entityDao.getEntity(eq("datamart"), eq("banned"))).thenReturn(Future.succeededFuture(banned));
        lenient().when(entityDao.getEntity("datamart2", "badusers_view")).thenReturn(Future.succeededFuture(badUsersView));
        lenient().when(deltaServiceDao.getDeltaOk("datamart2")).thenReturn(Future.succeededFuture(new OkDelta(0, LocalDateTime.now(), 0, 1)));
        lenient().when(pluginDeterminationService.determine(any())).thenReturn(Future.succeededFuture(new PluginDeterminationResult(Collections.emptySet(), SourceType.ADB, SourceType.ADB)));
        lenient().when(deltaServiceDao.writeOperationError("datamart", 1L)).thenReturn(Future.succeededFuture());
        lenient().when(deltaServiceDao.deleteWriteOperation("datamart", 1L)).thenReturn(Future.succeededFuture());
        lenient().when(restoreStateService.restoreErase("datamart")).thenReturn(Future.succeededFuture());
    }

    @Test
    void upsertSuccess(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void upsertSuccessWhenResumableOperation(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("UPSERT INTO users (id, name, time_col, timestamp_col, uuid_col) (SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers)")
                .tableName("users")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        upsertExecutor.execute(context)
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
    void upsertFailWhenNotTableBlockedException(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new RuntimeException("Exception")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        upsertExecutor.execute(context)
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
    void upsertFailWhenNotEqualExistWriteOpTable(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("UPSERT INTO users (id, name, time_col, timestamp_col, uuid_col) (SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers)")
                .tableName("unknown")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        upsertExecutor.execute(context)
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
    void upsertFailWhenNotEqualExistWriteOpStatus(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("UPSERT INTO users (id, name, time_col, timestamp_col, uuid_col) (SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers)")
                .tableName("users")
                .status(WriteOperationStatus.ERROR.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        upsertExecutor.execute(context)
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
    void upsertFailWhenNotEqualExistWriteOpQuery(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("UPSERT INTO users (id, name) VALUES ROW(2, 'Name')")
                .tableName("users")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        upsertExecutor.execute(context)
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
    void upsertSuccessWhenDatasourceSet(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADG), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(pluginService.hasSourceType(SourceType.ADG)).thenReturn(true);
        reset(pluginDeterminationService);
        when(pluginDeterminationService.determine(any())).thenAnswer(invocation -> {
            val arg = invocation.getArgument(0, PluginDeterminationRequest.class);
            return Future.succeededFuture(new PluginDeterminationResult(EnumSet.allOf(SourceType.class), arg.getPreferredSourceType(), arg.getPreferredSourceType()));
        });
        val context = prepareContext("UPSERT INTO users(id, name, time_col, timestamp_col, uuid_col) SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers DATASOURCE_TYPE='adg'");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void upsertFailWhenDatasourceSetAndDisabled(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(pluginService.hasSourceType(SourceType.ADG)).thenReturn(false);
        reset(pluginDeterminationService);
        when(pluginDeterminationService.determine(any())).thenAnswer(invocation -> {
            val arg = invocation.getArgument(0, PluginDeterminationRequest.class);
            return Future.succeededFuture(new PluginDeterminationResult(EnumSet.allOf(SourceType.class), arg.getPreferredSourceType(), arg.getPreferredSourceType()));
        });
        val context = prepareContext("UPSERT INTO users(id, name, time_col, timestamp_col, uuid_col) SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers DATASOURCE_TYPE='adg'");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of not compatible types"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Plugin [ADG] is not enabled to run Upsert operation", error.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void upsertSuccessWhenNotCompatibleTypes(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("UPSERT INTO users(id, name) SELECT id, 1 FROM datamart2.badusers");

        upsertExecutor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {

                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void upsertSuccessWhenNotCompatibleWithDoubleType(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("UPSERT INTO users(id, name) SELECT id, CAST(1.1 as double precision) FROM datamart2.badusers");

        upsertExecutor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void upsertSuccessWhenWrongSize(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("UPSERT INTO users(id, name) SELECT id, CAST('a' as varchar) FROM datamart2.badusers");

        upsertExecutor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void upsertSuccessWhenWrongTimeAccuracy(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("UPSERT INTO users(id, time_col) SELECT id, time '11:11:11.123' FROM datamart2.badusers");

        upsertExecutor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void upsertSuccessWhenWrongTimestampAccuracy(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("UPSERT INTO users(id, timestamp_col) SELECT id, timestamp '2021-01-01 11:11:11.123' FROM datamart2.badusers");

        upsertExecutor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void upsertSuccessWhenWrongUuidSize(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(0, LocalDateTime.now(), 0, 1)));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("UPSERT INTO users(id, uuid_col) SELECT id, uuid_col FROM datamart.banned");

        upsertExecutor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void upsertFailWhenWrongQueryColumnsSize(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(0, LocalDateTime.now(), 0, 1)));
        val context = prepareContext("UPSERT INTO users(name, id) SELECT name, id, time_col FROM datamart.users");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of query columns size"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Upsert select into users has conflict with query columns wrong count, entity: 2, query: 3", error.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void upsertIntoMatView(VertxTestContext testContext) {
        entity.setEntityType(EntityType.MATERIALIZED_VIEW);
        val context = prepareBasicContext();

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed to upsert into mat view"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Forbidden. Write operations allowed for logical tables only.", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void upsertFromView(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        val context = prepareContext("UPSERT INTO users(id, name, time_col, timestamp_col, uuid_col) SELECT * FROM datamart2.badusers_view");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void upsertSuccessWithoutColumns(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("UPSERT INTO users SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void upsertSuccessWithoutColumnsWithStarQuery(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("UPSERT INTO users SELECT * FROM datamart2.badusers");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void upsertWithNonExistedColumn(VertxTestContext testContext) {
        String sql = "UPSERT INTO users(id, name, non_existed_column) SELECT id, name FROM datamart2.badusers";
        val context = prepareContext(sql);

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query contains non existed column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void upsertWithSystemColumn(VertxTestContext testContext) {
        val context = prepareContext("UPSERT INTO users(id, name, sys_from) SELECT id, name FROM datamart2.badusers");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query contains non system column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void upsertWithoutNotNullColumn(VertxTestContext testContext) {
        val context = prepareContext("UPSERT INTO users(name) SELECT userid FROM banned");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query doesn't contains non nullable column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void testWithNoDeltaHotFound(VertxTestContext testContext) {
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.failedFuture(new DeltaException("Delta hot not found")));
        val context = prepareBasicContext();

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because delta hot is not found"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Delta hot not found", error.getMessage())).completeNow());
    }

    @Test
    void testSourceTypeNotConfigured(VertxTestContext testContext) {
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(false);
        val context = prepareBasicContext();

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because entity's source type is not configured"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Plugins: [ADB] for the table [users] datamart [datamart] are not configured", error.getMessage())).completeNow());
    }

    @Test
    void testNotUpsertNode(VertxTestContext testContext) {
        val context = prepareContext("INSERT INTO users(id, name) SELECT id, name FROM datamart2.badusers");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because not upsert sql node"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Not upsert operation.", error.getMessage())).completeNow());
    }

    @Test
    void testNotSelectSource(VertxTestContext testContext) {
        val context = prepareContext("UPSERT INTO users(id, name) VALUES (1,'1')");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because not select source sql node"))
                .onFailure(ar -> testContext.verify(() -> assertEquals("Invalid source for [UPSERT_SELECT]", ar.getMessage())).completeNow());
    }

    @Test
    void testNotInsert(VertxTestContext testContext) {
        val context = prepareContext("SELECT 1");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because not upsert sql node"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Unsupported sql node", error.getMessage())).completeNow());
    }

    @Test
    void testEstimate(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("UPSERT INTO users(id) SELECT id FROM datamart2.badusers ESTIMATE_ONLY");

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because ESTIMATE_ONLY sql node"))
                .onFailure(error -> testContext.verify(() -> assertEquals("ESTIMATE_ONLY is not allowed in upsert select", error.getMessage())).completeNow());
    }

    @Test
    void testPluginLlwFailedWithDtmException(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.failedFuture(new DtmException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Llw failed", error.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testPluginLlwFailedWithUnexpectedExceptionWithMessage(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.failedFuture(new RuntimeException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Llw failed", error.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testPluginLlwFailedWithUnexpectedExceptionWithoutMessage(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(pluginService.upsert(eq(SourceType.ADB), any(), any(UpsertSelectRequest.class))).thenReturn(Future.failedFuture(new RuntimeException()));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        upsertExecutor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of llw fail"))
                .onFailure(error -> testContext.verify(() -> {
                    assertSame(RuntimeException.class, error.getClass());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void testGetType() {
        assertEquals(DmlType.UPSERT_SELECT, upsertExecutor.getType());
    }

    private DmlRequestContext prepareBasicContext() {
        return prepareContext("UPSERT INTO users(id, name, time_col, timestamp_col, uuid_col) SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers");
    }

    private DmlRequestContext prepareContext(String sql) {
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        QueryRequest queryRequest = QueryRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("datamart")
                .sql(sql)
                .build();
        return DmlRequestContext.builder()
                .envName("dev")
                .request(new DmlRequest(queryRequest))
                .sourceType(SourceType.ADB)
                .sqlNode(sqlNode)
                .build();
    }
}