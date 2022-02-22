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
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationExtractor;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationService;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaQueryPreprocessor;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaService;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.core.calcite.factory.CoreSchemaFactory;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteContextProvider;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreQueryTemplateExtractor;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOpRequest;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.delta.exception.TableBlockedException;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequest;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.dml.dto.PluginDeterminationRequest;
import ru.datamart.prostore.query.execution.core.dml.dto.PluginDeterminationResult;
import ru.datamart.prostore.query.execution.core.dml.service.ColumnMetadataService;
import ru.datamart.prostore.query.execution.core.dml.service.PluginDeterminationService;
import ru.datamart.prostore.query.execution.core.dml.service.SqlParametersTypeExtractor;
import ru.datamart.prostore.query.execution.core.dml.service.impl.validate.BasicUpdateColumnsValidator;
import ru.datamart.prostore.query.execution.core.dml.service.impl.validate.WithNullableCheckUpdateColumnsValidator;
import ru.datamart.prostore.query.execution.core.dml.service.view.LogicViewReplacer;
import ru.datamart.prostore.query.execution.core.dml.service.view.MaterializedViewReplacer;
import ru.datamart.prostore.query.execution.core.dml.service.view.ViewReplacerService;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class InsertSelectExecutorTest {

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

    private InsertSelectExecutor executor;
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
        val updateColumnsValidator = new WithNullableCheckUpdateColumnsValidator(new BasicUpdateColumnsValidator());
        executor = new InsertSelectExecutor(pluginService, serviceDbFacade, restoreStateService, logicalSchemaProvider, deltaQueryPreprocessor, queryParserService, columnMetadataService, viewReplacerService, pluginDeterminationService, templateExtractor, parametersTypeExtractor, updateColumnsValidator);

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
    void insertSuccess(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

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
    void insertSuccessWhenResumableOperation(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name, time_col, timestamp_col, uuid_col) (SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers)")
                .tableName("users")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        executor.execute(context)
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
    void insertFailWhenNotTableBlockedException(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new RuntimeException("Exception")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        executor.execute(context)
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
    void insertFailWhenNotEqualExistWriteOpTable(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name, time_col, timestamp_col, uuid_col) (SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers)")
                .tableName("unknown")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        executor.execute(context)
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
    void insertFailWhenNotEqualExistWriteOpStatus(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name, time_col, timestamp_col, uuid_col) (SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers)")
                .tableName("users")
                .status(WriteOperationStatus.ERROR.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        executor.execute(context)
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
    void insertFailWhenNotEqualExistWriteOpQuery(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(entityDao.getEntity("datamart", "users")).thenReturn(Future.succeededFuture(entity));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.failedFuture(new TableBlockedException("tbl", new RuntimeException())));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        DeltaWriteOp existWriteOp = DeltaWriteOp.builder()
                .query("INSERT INTO users (id, name) VALUES ROW(2, 'Name')")
                .tableName("users")
                .status(WriteOperationStatus.EXECUTING.getValue())
                .sysCn(1L)
                .build();
        when(deltaServiceDao.getDeltaWriteOperations("datamart")).thenReturn(Future.succeededFuture(Arrays.asList(existWriteOp)));

        executor.execute(context)
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
    void insertSuccessWhenDatasourceSet(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADG), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(pluginService.hasSourceType(SourceType.ADG)).thenReturn(true);
        reset(pluginDeterminationService);
        when(pluginDeterminationService.determine(any())).thenAnswer(invocation -> {
            val arg = invocation.getArgument(0, PluginDeterminationRequest.class);
            return Future.succeededFuture(new PluginDeterminationResult(EnumSet.allOf(SourceType.class), arg.getPreferredSourceType(), arg.getPreferredSourceType()));
        });
        val context = prepareContext("INSERT INTO users(id, name, time_col, timestamp_col, uuid_col) SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers DATASOURCE_TYPE='adg'");

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
    void insertFailWhenDatasourceSetAndDisabled(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(pluginService.hasSourceType(SourceType.ADG)).thenReturn(false);
        reset(pluginDeterminationService);
        when(pluginDeterminationService.determine(any())).thenAnswer(invocation -> {
            val arg = invocation.getArgument(0, PluginDeterminationRequest.class);
            return Future.succeededFuture(new PluginDeterminationResult(EnumSet.allOf(SourceType.class), arg.getPreferredSourceType(), arg.getPreferredSourceType()));
        });
        val context = prepareContext("INSERT INTO users(id, name, time_col, timestamp_col, uuid_col) SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers DATASOURCE_TYPE='adg'");

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of not compatible types"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Plugin [ADG] is not enabled to run Insert operation", error.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void insertSuccessWhenNotCompatibleTypes(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("INSERT INTO users(id, name) SELECT id, 1 FROM datamart2.badusers");

        executor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {

                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void insertSuccessWhenNotCompatibleWithDoubleType(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("INSERT INTO users(id, name) SELECT id, CAST(1.1 as double precision) FROM datamart2.badusers");

        executor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void insertSuccessWhenWrongSize(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("INSERT INTO users(id, name) SELECT id, CAST('a' as varchar) FROM datamart2.badusers");

        executor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void insertSuccessWhenWrongTimeAccuracy(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("INSERT INTO users(id, time_col) SELECT id, time '11:11:11.123' FROM datamart2.badusers");

        executor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void insertSuccessWhenWrongTimestampAccuracy(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("INSERT INTO users(id, timestamp_col) SELECT id, timestamp '2021-01-01 11:11:11.123' FROM datamart2.badusers");

        executor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void insertSuccessWhenWrongUuidSize(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(0, LocalDateTime.now(), 0, 1)));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("INSERT INTO users(id, uuid_col) SELECT id, uuid_col FROM datamart.banned");

        executor.execute(context)
                .onFailure(testContext::failNow)
                .onSuccess(r -> testContext.verify(() -> {
                    verify(deltaServiceDao).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void insertFailWhenWrongQueryColumnsSize(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(deltaServiceDao.getDeltaOk("datamart")).thenReturn(Future.succeededFuture(new OkDelta(0, LocalDateTime.now(), 0, 1)));
        val context = prepareContext("INSERT INTO users(name, id) SELECT name, id, time_col FROM datamart.users");

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because of query columns size"))
                .onFailure(error -> testContext.verify(() -> {
                    assertEquals("Insert select into users has conflict with query columns wrong count, entity: 2, query: 3", error.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao).writeOperationError("datamart", 1L);
                    verify(restoreStateService).restoreErase(any());
                }).completeNow());
    }

    @Test
    void insertIntoMatView(VertxTestContext testContext) {
        entity.setEntityType(EntityType.MATERIALIZED_VIEW);
        val context = prepareBasicContext();

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed to insert into mat view"))
                .onFailure(ar -> testContext.verify(() -> {
                    assertEquals("Forbidden. Write operations allowed for logical tables only.", ar.getMessage());
                    verify(deltaServiceDao, never()).writeOperationSuccess("datamart", 1L);
                    verify(deltaServiceDao, never()).deleteWriteOperation("datamart", 1L);
                    verify(deltaServiceDao, never()).writeOperationError("datamart", 1L);
                    verify(restoreStateService, never()).restoreErase(any());
                }).completeNow());
    }

    @Test
    void insertFromView(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);

        val context = prepareContext("INSERT INTO users(id, name, time_col, timestamp_col, uuid_col) SELECT * FROM datamart2.badusers_view");

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
    void insertSuccessWithoutColumns(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("INSERT INTO users SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers");

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
    void insertSuccessWithoutColumnsWithStarQuery(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(deltaServiceDao.writeOperationSuccess("datamart", 1L)).thenReturn(Future.succeededFuture());
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.succeededFuture());
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("INSERT INTO users SELECT * FROM datamart2.badusers");

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
    void insertWithNonExistedColumn(VertxTestContext testContext) {
        String sql = "INSERT INTO users(id, name, non_existed_column) SELECT id, name FROM datamart2.badusers";
        val context = prepareContext(sql);

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query contains non existed column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void insertWithSystemColumn(VertxTestContext testContext) {
        val context = prepareContext("INSERT INTO users(id, name, sys_from) SELECT id, name FROM datamart2.badusers");

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query contains non system column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void insertWithoutNotNullColumn(VertxTestContext testContext) {
        val context = prepareContext("INSERT INTO users(name) SELECT userid FROM banned");

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because query doesn't contains non nullable column"))
                .onFailure(error -> testContext.verify(() -> assertEquals(ValidationDtmException.class, error.getClass())).completeNow());
    }

    @Test
    void testWithNoDeltaHotFound(VertxTestContext testContext) {
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.failedFuture(new DeltaException("Delta hot not found")));
        val context = prepareBasicContext();

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because delta hot is not found"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Delta hot not found", error.getMessage())).completeNow());
    }

    @Test
    void testSourceTypeNotConfigured(VertxTestContext testContext) {
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(false);
        val context = prepareBasicContext();

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because entity's source type is not configured"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Plugins: [ADB] for the table [users] datamart [datamart] are not configured", error.getMessage())).completeNow());
    }

    @Test
    void testNotSelectSource(VertxTestContext testContext) {
        val context = prepareContext("INSERT INTO users(id, name) VALUES (1,'1')");

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because not select source sql node"))
                .onFailure(ar -> testContext.verify(() -> assertEquals("Invalid source for [INSERT_SELECT]", ar.getMessage())).completeNow());
    }

    @Test
    void testNotInsert(VertxTestContext testContext) {
        val context = prepareContext("SELECT 1");

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because not insert sql node"))
                .onFailure(error -> testContext.verify(() -> assertEquals("Unsupported sql node", error.getMessage())).completeNow());
    }

    @Test
    void testEstimate(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareContext("INSERT INTO users(id) SELECT id FROM datamart2.badusers ESTIMATE_ONLY");

        executor.execute(context)
                .onSuccess(ar -> testContext.failNow("Should have been failed because ESTIMATE_ONLY sql node"))
                .onFailure(error -> testContext.verify(() -> assertEquals("ESTIMATE_ONLY is not allowed in insert select", error.getMessage())).completeNow());
    }

    @Test
    void testPluginLlwFailedWithDtmException(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaHot("datamart")).thenReturn(Future.succeededFuture(new HotDelta()));
        when(deltaServiceDao.writeNewOperation(any(DeltaWriteOpRequest.class))).thenReturn(Future.succeededFuture(1L));
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.failedFuture(new DtmException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        executor.execute(context)
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
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.failedFuture(new RuntimeException("Llw failed")));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        executor.execute(context)
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
        when(pluginService.insert(eq(SourceType.ADB), any(), any(InsertSelectRequest.class))).thenReturn(Future.failedFuture(new RuntimeException()));
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        val context = prepareBasicContext();

        executor.execute(context)
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
        assertEquals(DmlType.INSERT_SELECT, executor.getType());
    }

    private DmlRequestContext prepareBasicContext() {
        return prepareContext("INSERT INTO users(id, name, time_col, timestamp_col, uuid_col) SELECT id, name, time_col, timestamp_col, uuid_col FROM datamart2.badusers");
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