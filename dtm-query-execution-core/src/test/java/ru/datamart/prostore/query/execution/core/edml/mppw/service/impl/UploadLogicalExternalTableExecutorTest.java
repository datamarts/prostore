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
package ru.datamart.prostore.query.execution.core.edml.mppw.service.impl;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOpRequest;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.UploadFailedExecutor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class UploadLogicalExternalTableExecutorTest {

    @Mock
    private DeltaServiceDao deltaServiceDao;
    @Mock
    private UploadKafkaExecutor uploadKafkaExecutor;
    @Mock
    private UploadFailedExecutor uploadFailedExecutor;
    @Mock
    private DataSourcePluginService pluginService;
    @Mock
    private LogicalSchemaProvider logicalSchemaProvider;
    @Mock
    private EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService = new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private QueryRequest queryRequest;
    private Set<SourceType> sourceTypes = new HashSet<>();
    private Entity sourceEntity;
    private Entity destEntity;
    private UploadLogicalExternalTableExecutor uploadLogicalExternalTableExecutor;

    @Captor
    private ArgumentCaptor<DeltaWriteOpRequest> deltaWriteOpRequestArgumentCaptor;

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        sourceTypes.addAll(Arrays.asList(SourceType.ADB, SourceType.ADG));
        sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .name("upload_table")
                .schema("test")
                .externalTableSchema("")
                .build();
        destEntity = Entity.builder()
                .entityType(EntityType.TABLE)
                .name("pso")
                .schema("test")
                .destination(sourceTypes)
                .build();
        lenient().when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
        lenient().when(pluginService.hasSourceType(Mockito.any(SourceType.class))).thenAnswer(invocationOnMock -> sourceTypes.contains(invocationOnMock.getArgument(0, SourceType.class)));
        lenient().when(logicalSchemaProvider.getSchemaFromQuery(any(), any())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        lenient().doNothing().when(evictQueryTemplateCacheService).evictByDatamartName(anyString());
        lenient().doNothing().when(evictQueryTemplateCacheService).evictByEntityName(anyString(), anyString());
        lenient().when(uploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());
        lenient().when(uploadKafkaExecutor.getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);
        uploadLogicalExternalTableExecutor = new UploadLogicalExternalTableExecutor(deltaServiceDao, uploadFailedExecutor,
                Collections.singletonList(uploadKafkaExecutor), pluginService, logicalSchemaProvider, evictQueryTemplateCacheService);
    }

    @Test
    void executeKafkaSuccessWithSysCnExists(VertxTestContext testContext) {
        // arrange
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(sysCn);

        when(uploadKafkaExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(queryResult));

        when(deltaServiceDao.writeOperationSuccess(eq(queryRequest.getDatamartMnemonic()),
                eq(sysCn))).thenReturn(Future.succeededFuture());

        // act
        uploadLogicalExternalTableExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(queryResult, ar.result());
                    verify(evictQueryTemplateCacheService, times(2))
                            .evictByDatamartName(destEntity.getSchema());
                }).completeNow());
    }

    @Test
    void executeKafkaSuccessWithoutSysCn(VertxTestContext testContext) {
        // arrange
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 2L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(deltaWriteOpRequestArgumentCaptor.capture()))
                .thenReturn(Future.succeededFuture(sysCn));

        when(uploadKafkaExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(queryResult));

        when(deltaServiceDao.writeOperationSuccess(eq(queryRequest.getDatamartMnemonic()),
                eq(sysCn))).thenReturn(Future.succeededFuture());

        // act
        uploadLogicalExternalTableExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(queryResult, ar.result());
                    assertEquals(context.getSysCn(), sysCn);
                    verify(evictQueryTemplateCacheService, times(2))
                            .evictByDatamartName(destEntity.getSchema());

                    val deltaWriteOpRequest = deltaWriteOpRequestArgumentCaptor.getValue();
                    assertEquals("pso", deltaWriteOpRequest.getTableName());
                    assertEquals("upload_table", deltaWriteOpRequest.getTableNameExt());
                }).completeNow());
    }

    @Test
    void executeWriteNewOpError(VertxTestContext testContext) {
        // arrange
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        // act
        uploadLogicalExternalTableExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    verifyEvictCacheExecuted();
                }).completeNow());

    }

    @Test
    void executeWriteOpSuccessError() {
        // arrange
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final QueryResult queryResult = QueryResult.emptyResult();
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        when(uploadKafkaExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(queryResult));

        when(deltaServiceDao.writeOperationSuccess(eq(queryRequest.getDatamartMnemonic()),
                eq(sysCn))).thenReturn(Future.failedFuture(new DtmException("")));

        // act
        uploadLogicalExternalTableExecutor.execute(context)
                .onComplete(ar -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    verifyEvictCacheExecuted();
                });
    }

    @Test
    void executeKafkaError() {
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        when(uploadKafkaExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        when(uploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn)))
                .thenReturn(Future.succeededFuture());

        when(uploadFailedExecutor.execute(any())).thenReturn(Future.succeededFuture());

        // act
        uploadLogicalExternalTableExecutor.execute(context)
                .onComplete(ar -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    verifyEvictCacheExecuted();
                });
    }

    @Test
    void executeWriteOpError() {
        // arrange
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        when(uploadKafkaExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn)))
                .thenReturn(Future.failedFuture(new DtmException("")));

        // act
        uploadLogicalExternalTableExecutor.execute(context)
                .onComplete(ar -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    verifyEvictCacheExecuted();
                });
    }

    @Test
    void executeUploadFailedError() {
        // arrange
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        final Long sysCn = 1L;

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(deltaServiceDao.writeNewOperation(any()))
                .thenReturn(Future.succeededFuture(sysCn));

        when(uploadKafkaExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        when(deltaServiceDao.writeOperationError(eq("test"), eq(sysCn)))
                .thenReturn(Future.succeededFuture());

        when(uploadFailedExecutor.execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        // act
        uploadLogicalExternalTableExecutor.execute(context)
                .onComplete(ar -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    verifyEvictCacheExecuted();
                });
    }

    @Test
    void executeWithNonexistingDestSource() {
        // arrange
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.getDestinationEntity().setDestination(new HashSet<>(Arrays.asList(SourceType.ADB,
                SourceType.ADG, SourceType.ADQM)));

        // act
        uploadLogicalExternalTableExecutor.execute(context)
                .onComplete(ar -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    verify(evictQueryTemplateCacheService, times(0)).evictByDatamartName(anyString());
                });
    }

    private void verifyEvictCacheExecuted() {
        verify(evictQueryTemplateCacheService, times(1))
                .evictByDatamartName(destEntity.getSchema());
    }
}
