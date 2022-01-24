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
package io.arenadata.dtm.query.execution.core.eddl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.*;
import io.arenadata.dtm.common.plugin.exload.Type;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.base.service.avro.AvroSchemaGenerator;
import io.arenadata.dtm.query.execution.core.eddl.dto.CreateDownloadExternalTableQuery;
import io.arenadata.dtm.query.execution.core.eddl.service.EddlExecutor;
import io.arenadata.dtm.query.execution.core.eddl.service.download.CreateDownloadExternalTableExecutor;
import io.arenadata.dtm.query.execution.core.edml.configuration.EdmlProperties;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateDownloadExternalTableExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final EdmlProperties edmlProperties = mock(EdmlProperties.class);
    private final AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator();
    private EddlExecutor createDownloadExteranlTableExecutor;
    private CreateDownloadExternalTableQuery query;
    private String schema;
    private Entity entity;
    private final Integer defaultChunkSize = 1000;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        when(edmlProperties.getDefaultChunkSize()).thenReturn(defaultChunkSize);
        createDownloadExteranlTableExecutor = new CreateDownloadExternalTableExecutor(serviceDbFacade, edmlProperties);

        schema = "shares";
        String table = "accounts";
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(20);
        entity = new Entity(table, schema, Arrays.asList(f1, f2));
        entity.setEntityType(EntityType.DOWNLOAD_EXTERNAL_TABLE);
        Schema avroSchema = avroSchemaGenerator.generateTableSchema(entity, false);
        int chunkSize = 10;
        String locationPath = "kafka://localhost:2181/KAFKA_TOPIC";
        query = CreateDownloadExternalTableQuery.builder()
                .schemaName(schema)
                .tableName(table)
                .entity(entity)
                .locationType(Type.KAFKA_TOPIC)
                .locationPath(locationPath)
                .format(ExternalTableFormat.AVRO)
                .tableSchema(avroSchema.toString())
                .chunkSize(chunkSize)
                .build();

    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeDefaultChunkSizeSuccess() {
        Promise<QueryResult> promise = Promise.promise();
        query.setChunkSize(null);
        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.succeededFuture());

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals(query.getEntity().getExternalTableDownloadChunkSize(), defaultChunkSize);
    }

    @Test
    void executeDatamartNotExists() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(false));

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof DatamartNotExistsException);
    }

    @Test
    void executeEntityExists() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new EntityAlreadyExistsException(entity.getNameWithSchema())));

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof EntityAlreadyExistsException);
    }

    @Test
    void executeExistsDatamartError() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.failedFuture(new DtmException("exists datamart error")));

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals("exists datamart error", promise.future().cause().getMessage());
    }

    @Test
    void executeCreateEntityError() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(datamartDao.existsDatamart(eq(schema)))
                .thenReturn(Future.succeededFuture(true));

        Mockito.when(entityDao.existsEntity(eq(schema), eq(entity.getName())))
                .thenReturn(Future.succeededFuture(false));

        Mockito.when(entityDao.createEntity(any()))
                .thenReturn(Future.failedFuture(new DtmException("create entity error")));

        createDownloadExteranlTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertEquals("create entity error", promise.future().cause().getMessage());
    }
}
