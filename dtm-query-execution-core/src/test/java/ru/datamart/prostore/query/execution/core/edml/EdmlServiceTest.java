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
package ru.datamart.prostore.query.execution.core.edml;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlAction;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.mppr.service.impl.DownloadExternalTableExecutor;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.impl.RollbackCrashedWriteOpExecutor;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.impl.UploadLogicalExternalTableExecutor;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.impl.UploadStandaloneExternalTableExecutor;
import ru.datamart.prostore.query.execution.core.edml.service.EdmlService;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class EdmlServiceTest {

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));

    @Mock
    private DownloadExternalTableExecutor downloadExternalTableExecutor;
    @Mock
    private UploadLogicalExternalTableExecutor uploadLogicalExternalTableExecutor;
    @Mock
    private UploadStandaloneExternalTableExecutor uploadStandaloneExternalTableExecutor;
    @Mock
    private RollbackCrashedWriteOpExecutor rollbackCrashedWriteOpExecutor;
    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private ServiceDbDao serviceDbDao;
    @Mock
    private EntityDao entityDao;

    private EdmlService edmlService;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(downloadExternalTableExecutor.getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(uploadLogicalExternalTableExecutor.getAction()).thenReturn(EdmlAction.UPLOAD_LOGICAL);
        when(uploadStandaloneExternalTableExecutor.getAction()).thenReturn(EdmlAction.UPLOAD_STANDALONE);
        when(rollbackCrashedWriteOpExecutor.getAction()).thenReturn(EdmlAction.ROLLBACK);
        edmlService = new EdmlService(serviceDbFacade, Arrays.asList(downloadExternalTableExecutor, uploadLogicalExternalTableExecutor,
                uploadStandaloneExternalTableExecutor, rollbackCrashedWriteOpExecutor));
    }

    @Test
    void executeDownloadExtTableSuccess(VertxTestContext vertxTestContext) {
        // arrange
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("download_table")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.TABLE)
                .name("pso")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "download_table")).thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "pso")).thenReturn(Future.succeededFuture(sourceEntity));

        when(downloadExternalTableExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        // act
        edmlService.execute(context)
                .onComplete(ar -> vertxTestContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(context.getSourceEntity(), sourceEntity);
                    assertEquals(context.getDestinationEntity(), destinationEntity);
                }).completeNow());
    }

    @Test
    void executeUploadExtTableSuccess(VertxTestContext testContext) {
        // arrange
        queryRequest.setSql("INSERT INTO test.pso SELECT id, name FROM test.upload_table");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.TABLE)
                .name("pso")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("upload_table")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "pso"))
                .thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "upload_table"))
                .thenReturn(Future.succeededFuture(sourceEntity));

        when(uploadLogicalExternalTableExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        // act
        edmlService.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(context.getSourceEntity(), sourceEntity);
                    assertEquals(context.getDestinationEntity(), destinationEntity);
                }).completeNow());
    }

    @Test
    void executeUploadIntoStandaloneSuccess(VertxTestContext testContext) {
        // arrange
        queryRequest.setSql("INSERT INTO test.pso SELECT id, name FROM test.upload_table");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .name("pso")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("upload_table")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "pso"))
                .thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "upload_table"))
                .thenReturn(Future.succeededFuture(sourceEntity));

        when(uploadStandaloneExternalTableExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        // act
        edmlService.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(context.getSourceEntity(), sourceEntity);
                    assertEquals(context.getDestinationEntity(), destinationEntity);
                }).completeNow());
    }

    @Test
    void executeDownloadExtTableAsSource(VertxTestContext testContext) {
        // arrange
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("download_table")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("pso")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "download_table"))
                .thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "pso"))
                .thenReturn(Future.succeededFuture(sourceEntity));

        // act
        edmlService.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    @Test
    void executeUploadExtTableAsDestination(VertxTestContext testContext) {
        // arrange
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("download_table")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("pso")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "download_table"))
                .thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "pso"))
                .thenReturn(Future.succeededFuture(sourceEntity));

        // act
        edmlService.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    @Test
    void executeUploadExtTableToMaterializedView(VertxTestContext testContext) {
        // arrange
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.MATERIALIZED_VIEW)
                .name("download_table")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.UPLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("pso")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "download_table"))
                .thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "pso"))
                .thenReturn(Future.succeededFuture(sourceEntity));

        // act
        edmlService.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    TestUtils.assertException(DtmException.class, "MPPW operation doesn't support materialized views", ar.cause());
                }).completeNow());
    }

    @Test
    void executeDownloadFromMaterializedViewSuccess(VertxTestContext testContext) {
        // arrange
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("download_table")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.MATERIALIZED_VIEW)
                .name("pso")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "download_table")).thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "pso")).thenReturn(Future.succeededFuture(sourceEntity));

        when(downloadExternalTableExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        // act
        edmlService.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(context.getSourceEntity(), sourceEntity);
                    assertEquals(context.getDestinationEntity(), destinationEntity);
                }).completeNow());
    }

    @Test
    void executeDownloadFromStandaloneTableSuccess(VertxTestContext testContext) {
        // arrange
        queryRequest.setSql("INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("download_table")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.READABLE_EXTERNAL_TABLE)
                .name("pso")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "download_table")).thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "pso")).thenReturn(Future.succeededFuture(sourceEntity));

        when(downloadExternalTableExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        // act
        edmlService.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(context.getSourceEntity(), sourceEntity);
                    assertEquals(context.getDestinationEntity(), destinationEntity);
                }).completeNow());
    }

    @Test
    void executeRollbackSuccess(VertxTestContext testContext) {
        // arrange
        SqlNode sqlNode = mock(SqlNode.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.ROLLBACK);
        queryRequest.setSql("ROLLBACK DELTA");
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        when(rollbackCrashedWriteOpExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        // act
        edmlService.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                }).completeNow());
    }

    @Test
    void executeWithoutExtTables(VertxTestContext testContext) {
        // arrange
        String sql = "INSERT INTO test.download_table SELECT id, lst_nam FROM test.pso";
        queryRequest.setSql(sql);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.TABLE)
                .name("download_table")
                .schema("test")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.TABLE)
                .name("pso")
                .schema("test")
                .build();

        when(entityDao.getEntity("test", "download_table")).thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test", "pso")).thenReturn(Future.succeededFuture(sourceEntity));

        // act
        edmlService.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    TestUtils.assertException(DtmException.class,
                            "Can't determine external table from query",
                            ar.cause());
                }).completeNow());
    }

    @Test
    void executeWithDifferentDatamarts(VertxTestContext testContext) {
        // arrange
        queryRequest.setSql("INSERT INTO test1.download_table SELECT id, lst_nam FROM test2.pso");
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        Entity destinationEntity = Entity.builder()
                .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
                .externalTableFormat(ExternalTableFormat.AVRO)
                .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
                .externalTableLocationType(ExternalTableLocationType.KAFKA)
                .externalTableUploadMessageLimit(1000)
                .externalTableSchema("{\"schema\"}")
                .name("download_table")
                .schema("test1")
                .build();

        Entity sourceEntity = Entity.builder()
                .entityType(EntityType.TABLE)
                .name("pso")
                .schema("test2")
                .build();

        // act
        edmlService.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    TestUtils.assertException(DtmException.class,
                            "Unsupported operation for tables in different datamarts",
                            ar.cause());
                }).completeNow());
    }
}
