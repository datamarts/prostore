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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.EdmlUploadExecutor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class UploadStandaloneExternalTableExecutorTest {

    private static final String ENV = "env";
    public static final String DATAMART = "test";

    @Mock
    private EdmlUploadExecutor edmlUploadExecutorKafka;
    @Mock
    private DataSourcePluginService pluginService;
    @Mock
    private LogicalSchemaProvider logicalSchemaProvider;

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService = new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));

    private UploadStandaloneExternalTableExecutor uploadStandaloneExternalTableExecutor;

    @BeforeEach
    void setUp() {

        lenient().when(edmlUploadExecutorKafka.getUploadType()).thenReturn(ExternalTableLocationType.KAFKA);

        val executors = Arrays.asList(edmlUploadExecutorKafka);
        uploadStandaloneExternalTableExecutor = new UploadStandaloneExternalTableExecutor(executors, pluginService, logicalSchemaProvider);
    }

    @Test
    void shouldSuccess(VertxTestContext testContext) {
        // arrange
        val sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .name("upload_table")
                .schema("test")
                .externalTableSchema("")
                .build();
        val destEntity = Entity.builder()
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .name("pso")
                .schema("test")
                .destination(EnumSet.of(SourceType.ADB))
                .build();

        val context = getEdmlRequestContext(sourceEntity, destEntity);
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), any())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(edmlUploadExecutorKafka.execute(any())).thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        // act
        uploadStandaloneExternalTableExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        Assertions.fail(ar.cause());
                    }
                }).completeNow());
    }

    @Test
    void shouldFailWhenNoSourceType(VertxTestContext testContext) {
        // arrange
        val sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .name("upload_table")
                .schema("test")
                .externalTableSchema("")
                .build();
        val destEntity = Entity.builder()
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .name("pso")
                .schema("test")
                .destination(EnumSet.of(SourceType.ADB))
                .build();

        val context = getEdmlRequestContext(sourceEntity, destEntity);
        List<Datamart> schema = new ArrayList<>();

        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(false);

        // act
        uploadStandaloneExternalTableExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        Assertions.fail("Unexpected success");
                    }

                    Assertions.assertEquals("Plugins: [ADB] for the table [pso] datamart [test] are not configured", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenFailedToInitSchema(VertxTestContext testContext) {
        // arrange
        val sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .name("upload_table")
                .schema("test")
                .externalTableSchema("")
                .build();
        val destEntity = Entity.builder()
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .name("pso")
                .schema("test")
                .destination(EnumSet.of(SourceType.ADB))
                .build();

        val context = getEdmlRequestContext(sourceEntity, destEntity);
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), any())).thenReturn(Future.failedFuture("Failed"));

        // act
        uploadStandaloneExternalTableExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        Assertions.fail("Unexpected success");
                    }

                    Assertions.assertEquals("Failed", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenNotKafkaType(VertxTestContext testContext) {
        // arrange
        val sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.HDFS)
                .externalTableUploadMessageLimit(1000)
                .name("upload_table")
                .schema("test")
                .externalTableSchema("")
                .build();
        val destEntity = Entity.builder()
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .name("pso")
                .schema("test")
                .destination(EnumSet.of(SourceType.ADB))
                .build();

        val context = getEdmlRequestContext(sourceEntity, destEntity);
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), any())).thenReturn(Future.succeededFuture(Collections.emptyList()));

        // act
        uploadStandaloneExternalTableExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        Assertions.fail("Unexpected success");
                    }

                    Assertions.assertEquals("Other download types are not yet implemented", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenExecuteFailed(VertxTestContext testContext) {
        // arrange
        val sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .name("upload_table")
                .schema("test")
                .externalTableSchema("")
                .build();
        val destEntity = Entity.builder()
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .name("pso")
                .schema("test")
                .destination(EnumSet.of(SourceType.ADB))
                .build();

        val context = getEdmlRequestContext(sourceEntity, destEntity);
        when(pluginService.hasSourceType(SourceType.ADB)).thenReturn(true);
        when(logicalSchemaProvider.getSchemaFromQuery(any(), any())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(edmlUploadExecutorKafka.execute(any())).thenReturn(Future.failedFuture("Failed"));

        // act
        uploadStandaloneExternalTableExecutor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        Assertions.fail("Unexpected success");
                    }

                    Assertions.assertEquals("Failed", ar.cause().getMessage());
                }).completeNow());
    }

    private EdmlRequestContext getEdmlRequestContext(Entity sourceEntity, Entity destEntity) {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART);
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        String insertSql = "insert into test.pso (select id, lst_nam FROM test.upload_table)";
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, ENV);
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        return context;
    }
}