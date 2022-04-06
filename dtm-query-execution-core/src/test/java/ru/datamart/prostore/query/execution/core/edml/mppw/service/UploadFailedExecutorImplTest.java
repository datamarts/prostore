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
package ru.datamart.prostore.query.execution.core.edml.mppw.service;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.CrashException;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.impl.DeltaServiceDaoImpl;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import ru.datamart.prostore.query.execution.core.rollback.dto.RollbackRequest;
import ru.datamart.prostore.query.execution.core.rollback.dto.RollbackRequestContext;
import ru.datamart.prostore.query.execution.core.rollback.factory.RollbackRequestContextFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UploadFailedExecutorImplTest {

    private final DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService = mock(EvictQueryTemplateCacheService.class);
    private final RollbackRequestContextFactory rollbackRequestContextFactory = mock(RollbackRequestContextFactory.class);
    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private UploadFailedExecutor uploadFailedExecutor;
    private QueryRequest queryRequest;
    private Set<SourceType> sourceTypes = new HashSet<>();
    private Entity sourceEntity;
    private Entity destEntity;

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
    }

    @Test
    void executeSuccess() {
        Promise<Void> promise = Promise.promise();
        uploadFailedExecutor = new UploadFailedExecutor(deltaServiceDao,
                rollbackRequestContextFactory, pluginService, evictQueryTemplateCacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, null, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(1L);

        final RollbackRequestContext rollbackRequestContext = new RollbackRequestContext(
                new RequestMetrics(),
                "test",
                RollbackRequest.builder()
                        .queryRequest(context.getRequest().getQueryRequest())
                        .datamart(context.getSourceEntity().getName())
                        .destinationTable(context.getDestinationEntity().getName())
                        .sysCn(context.getSysCn())
                        .entity(context.getDestinationEntity())
                        .build(),
                mock(SqlNode.class)
        );

        when(rollbackRequestContextFactory.create(any()))
                .thenReturn(rollbackRequestContext);

        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
        when(pluginService.hasSourceType(Mockito.any(SourceType.class))).thenAnswer(invocationOnMock -> sourceTypes.contains(invocationOnMock.getArgument(0, SourceType.class)));

        when(pluginService.rollback(any(), any(), any()))
                .thenReturn(Future.succeededFuture());

        when(deltaServiceDao.deleteWriteOperation(eq(sourceEntity.getSchema()), eq(context.getSysCn())))
                .thenReturn(Future.succeededFuture());

        uploadFailedExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executePluginRollbackError() {
        Promise<Void> promise = Promise.promise();
        uploadFailedExecutor = new UploadFailedExecutor(deltaServiceDao,
                rollbackRequestContextFactory, pluginService, evictQueryTemplateCacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, null, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(1L);
        final RollbackRequestContext rollbackRequestContext = new RollbackRequestContext(
                new RequestMetrics(),
                "test",
                RollbackRequest.builder()
                        .queryRequest(context.getRequest().getQueryRequest())
                        .datamart(context.getSourceEntity().getName())
                        .destinationTable(context.getDestinationEntity().getName())
                        .sysCn(context.getSysCn())
                        .entity(context.getDestinationEntity())
                        .build(),
                mock(SqlNode.class)
                );

        when(rollbackRequestContextFactory.create(any()))
                .thenReturn(rollbackRequestContext);

        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
        when(pluginService.hasSourceType(Mockito.any(SourceType.class))).thenAnswer(invocationOnMock -> sourceTypes.contains(invocationOnMock.getArgument(0, SourceType.class)));

        when(pluginService.rollback(any(), any(), any()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        uploadFailedExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof CrashException);
    }

    @Test
    void executeDeleteOperationError() {
        Promise<Void> promise = Promise.promise();
        uploadFailedExecutor = new UploadFailedExecutor(deltaServiceDao,
                rollbackRequestContextFactory, pluginService, evictQueryTemplateCacheService);
        String selectSql = "(select id, lst_nam FROM test.upload_table)";
        String insertSql = "insert into test.pso " + selectSql;
        queryRequest.setSql(insertSql);
        DatamartRequest request = new DatamartRequest(queryRequest);

        EdmlRequestContext context = new EdmlRequestContext(new RequestMetrics(), request, null, "env");
        context.setDestinationEntity(destEntity);
        context.setSourceEntity(sourceEntity);
        context.setSysCn(1L);

        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);

        when(pluginService.rollback(any(), any(), any()))
                .thenReturn(Future.succeededFuture());

        when(deltaServiceDao.deleteWriteOperation(eq(sourceEntity.getSchema()), eq(context.getSysCn())))
                .thenReturn(Future.failedFuture(new DtmException("")));

        uploadFailedExecutor.execute(context)
                .onComplete(promise);
        assertTrue(promise.future().failed());
    }
}
