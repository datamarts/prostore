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
package io.arenadata.dtm.query.execution.core.edml;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dml.service.view.ViewReplacerService;
import io.arenadata.dtm.query.execution.core.edml.mppr.service.impl.DownloadExternalTableExecutor;
import io.arenadata.dtm.query.execution.core.edml.mppr.service.impl.DownloadKafkaExecutor;
import io.arenadata.dtm.query.execution.core.edml.mppr.service.EdmlDownloadExecutor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DownloadExternalTableExecutorTest {
    public static final String SELECT_SQL = "select id, lst_nam FROM test.pso";
    private final LogicalSchemaProvider logicalSchemaProvider = mock(LogicalSchemaProvider.class);
    private final DeltaQueryPreprocessor deltaQueryPreprocessor = mock(DeltaQueryPreprocessor.class);
    private final List<EdmlDownloadExecutor> downloadExecutors = Arrays.asList(mock(DownloadKafkaExecutor.class));
    private DownloadExternalTableExecutor downloadExternalTableExecutor;
    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
        new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private QueryRequest queryRequest;
    private Entity destEntity;
    private Entity sourceEntity;
    private final List<Datamart> schema = Collections.emptyList();
    private final ViewReplacerService viewReplacerService = mock(ViewReplacerService.class);

    @BeforeEach
    void setUp() {
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));

        destEntity = Entity.builder()
            .entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE)
            .externalTableFormat(ExternalTableFormat.AVRO)
            .externalTableLocationPath("kafka://kafka-1.dtm.local:9092/topic")
            .externalTableLocationType(ExternalTableLocationType.KAFKA)
            .externalTableUploadMessageLimit(1000)
            .name("download_table")
            .schema("test")
            .externalTableSchema("")
            .build();

        sourceEntity = Entity.builder()
            .schema("test")
            .name("pso")
            .entityType(EntityType.TABLE)
            .build();

        DeltaQueryPreprocessorResponse deltaQueryPreprocessorResponse = mock(DeltaQueryPreprocessorResponse.class);
        when(deltaQueryPreprocessor.process(any()))
                .thenReturn(Future.succeededFuture(deltaQueryPreprocessorResponse));
    }

    @Test
    void executeKafkaExecutorSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
            deltaQueryPreprocessor, downloadExecutors, viewReplacerService);
        String insertSql = "insert into test.download_table " + SELECT_SQL;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        when(viewReplacerService.replace(any(SqlNode.class), any())).thenReturn(Future.succeededFuture(sqlNode));
        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);


        when(logicalSchemaProvider.getSchemaFromQuery(any(), any()))
                .thenReturn(Future.succeededFuture(schema));

        when(logicalSchemaProvider.getSchemaFromDeltaInformations(any(), any()))
                .thenReturn(Future.succeededFuture(schema));

        DeltaQueryPreprocessorResponse deltaQueryPreprocessorResponse = mock(DeltaQueryPreprocessorResponse.class);
        when(deltaQueryPreprocessor.process(any()))
            .thenReturn(Future.succeededFuture(deltaQueryPreprocessorResponse));

        when(downloadExecutors.get(0).execute(any()))
                .thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        downloadExternalTableExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeKafkaGetLogicalSchemaError() {
        Promise<QueryResult> promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
            deltaQueryPreprocessor, downloadExecutors, viewReplacerService);
        String insertSql = "insert into test.download_table " + SELECT_SQL;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        when(viewReplacerService.replace(any(SqlNode.class), any())).thenReturn(Future.succeededFuture(sqlNode));

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(logicalSchemaProvider.getSchemaFromQuery(any(), any()))
        .thenReturn(Future.failedFuture(new DtmException("")));

        downloadExternalTableExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeKafkaDeltaProcessError() {
        Promise<QueryResult> promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
            deltaQueryPreprocessor, downloadExecutors, viewReplacerService);
        String insertSql = "insert into test.download_table " + SELECT_SQL;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        when(viewReplacerService.replace(any(SqlNode.class), any())).thenReturn(Future.succeededFuture(sqlNode));

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(logicalSchemaProvider.getSchemaFromQuery(any(), any()))
                .thenReturn(Future.succeededFuture(schema));

        when(deltaQueryPreprocessor.process(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        downloadExternalTableExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }

    @Test
    void executeKafkaExecutorError() {
        Promise<QueryResult> promise = Promise.promise();
        when(downloadExecutors.get(0).getDownloadType()).thenReturn(ExternalTableLocationType.KAFKA);
        downloadExternalTableExecutor = new DownloadExternalTableExecutor(logicalSchemaProvider,
            deltaQueryPreprocessor, downloadExecutors, viewReplacerService);
        String insertSql = "insert into test.download_table " + SELECT_SQL;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);
        SqlInsert sqlNode = (SqlInsert) definitionService.processingQuery(queryRequest.getSql());
        when(viewReplacerService.replace(any(SqlNode.class), any())).thenReturn(Future.succeededFuture(sqlNode));

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, sqlNode, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);

        when(logicalSchemaProvider.getSchemaFromQuery(any(), any()))
                .thenReturn(Future.succeededFuture(schema));

        when(downloadExecutors.get(0).execute(any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        downloadExternalTableExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }
}
