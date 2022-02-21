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
package ru.datamart.prostore.query.execution.core.edml;

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
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlAction;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.mppr.service.impl.DownloadExternalTableExecutor;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.impl.RollbackCrashedWriteOpExecutor;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.impl.UploadExternalTableExecutor;
import ru.datamart.prostore.query.execution.core.edml.service.EdmlExecutor;
import ru.datamart.prostore.query.execution.core.edml.service.EdmlService;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EdmlServiceTest {

    private static final int DOWNLOAD_EXECUTOR = 0;
    private static final int UPLOAD_EXECUTOR = 1;
    private static final int ROLLBACK_EXECUTOR = 2;
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final List<EdmlExecutor> edmlExecutors = Arrays.asList(mock(DownloadExternalTableExecutor.class),
            mock(UploadExternalTableExecutor.class),
            mock(RollbackCrashedWriteOpExecutor.class));
    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private EdmlService edmlService;
    private QueryRequest queryRequest;

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(edmlExecutors.get(DOWNLOAD_EXECUTOR).getAction()).thenReturn(EdmlAction.DOWNLOAD);
        when(edmlExecutors.get(UPLOAD_EXECUTOR).getAction()).thenReturn(EdmlAction.UPLOAD);
        when(edmlExecutors.get(ROLLBACK_EXECUTOR).getAction()).thenReturn(EdmlAction.ROLLBACK);
        edmlService = new EdmlService(serviceDbFacade, edmlExecutors);
    }

    @Test
    void executeDownloadExtTableSuccess() {
        Promise<QueryResult> promise = Promise.promise();

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

        when(edmlExecutors.get(DOWNLOAD_EXECUTOR).execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        edmlService.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(context.getSourceEntity(), sourceEntity);
        assertEquals(context.getDestinationEntity(), destinationEntity);
    }

    @Test
    void executeUploadExtTableSuccess() {
        Promise<QueryResult> promise = Promise.promise();
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

        when(edmlExecutors.get(UPLOAD_EXECUTOR).execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        edmlService.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals(context.getSourceEntity(), sourceEntity);
        assertEquals(context.getDestinationEntity(), destinationEntity);
    }

    @Test
    void executeDownloadExtTableAsSource() {
        Promise<QueryResult> promise = Promise.promise();
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

        edmlService.execute(context)
                .onComplete(promise);
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeUploadExtTableAsDestination() {
        Promise<QueryResult> promise = Promise.promise();
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

        edmlService.execute(context)
                .onComplete(promise);
        assertNotNull(promise.future().cause());
    }

    @Test
    void executeUploadExtTableToMaterializedView() {
        // arrange
        Promise<QueryResult> promise = Promise.promise();
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
                .onComplete(promise);

        // assert
        assertTrue(promise.future().failed());
        TestUtils.assertException(DtmException.class, "MPPW operation doesn't support materialized views", promise.future().cause());
    }

    @Test
    void executeDownloadFromMaterializedViewSuccess() {
        Promise<QueryResult> promise = Promise.promise();

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

        when(edmlExecutors.get(DOWNLOAD_EXECUTOR).execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        edmlService.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(context.getSourceEntity(), sourceEntity);
        assertEquals(context.getDestinationEntity(), destinationEntity);
    }

    @Test
    void executeRollbackSuccess() {
        Promise<QueryResult> promise = Promise.promise();

        SqlNode sqlNode = mock(SqlNode.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.ROLLBACK);
        queryRequest.setSql("ROLLBACK DELTA");
        DatamartRequest request = new DatamartRequest(queryRequest);
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");

        when(edmlExecutors.get(ROLLBACK_EXECUTOR).execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        edmlService.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeWithoutExtTables() {
        Promise<QueryResult> promise = Promise.promise();

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

        edmlService.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        TestUtils.assertException(DtmException.class,
                "Can't determine external table from query",
                promise.future().cause());
    }

    @Test
    void executeWithDifferentDatamarts() {
        Promise<QueryResult> promise = Promise.promise();

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

        when(entityDao.getEntity("test1", "download_table")).thenReturn(Future.succeededFuture(destinationEntity));

        when(entityDao.getEntity("test2", "pso")).thenReturn(Future.succeededFuture(sourceEntity));

        edmlService.execute(context)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        TestUtils.assertException(DtmException.class,
                "Unsupported operation for tables in different datamarts",
                promise.future().cause());
    }
}
