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
package ru.datamart.prostore.query.execution.core.base.service.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;
import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.service.hsql.HSQLClient;
import ru.datamart.prostore.query.execution.core.base.service.metadata.query.DdlQueryGenerator;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static ru.datamart.prostore.query.execution.core.utils.TestUtils.loadTextFromFile;

@ExtendWith(MockitoExtension.class)
class InformationSchemaServiceTest {

    private static final String DATAMART = "DATAMART";
    public static final String TABLE_ENTITY = "TABLE_ENTITY";
    public static final String VIEW_ENTITY = "VIEW_ENTITY";
    public static final String MATVIEW_ENTITY = "MATVIEW_ENTITY";
    private static final List<Entity> ENTITIES = Arrays.asList(
            Entity.builder()
                    .schema(DATAMART)
                    .name(TABLE_ENTITY)
                    .entityType(EntityType.TABLE)
                    .fields(Collections.emptyList())
                    .build(),
            Entity.builder()
                    .schema(DATAMART)
                    .name(VIEW_ENTITY)
                    .entityType(EntityType.VIEW)
                    .fields(Collections.emptyList())
                    .build(),
            Entity.builder()
                    .schema(DATAMART)
                    .name(MATVIEW_ENTITY)
                    .entityType(EntityType.MATERIALIZED_VIEW)
                    .fields(Collections.emptyList())
                    .build());

    @Mock
    private HSQLClient client;
    @Mock
    private DatamartDao datamartDao;
    @Mock
    private EntityDao entityDao;
    @Mock
    private DdlQueryGenerator ddlQueryGenerator;
    @Mock
    private ApplicationContext applicationContext;
    @Mock
    private InformationSchemaQueryFactory informationSchemaQueryFactory;
    @Mock
    private CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;

    private InformationSchemaService informationSchemaService;

    @BeforeEach
    void setUp() {
        informationSchemaService = new InformationSchemaService(client, datamartDao, entityDao, ddlQueryGenerator,
                applicationContext, informationSchemaQueryFactory, materializedViewCacheService);
    }

    @Test
    void initInformationSchemaEmptyDtmExistedDatasourcesSuccess() throws JsonProcessingException {
        when(client.executeBatch(anyList())).thenReturn(Future.succeededFuture());
        when(datamartDao.getDatamarts()).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(informationSchemaQueryFactory.createInitEntitiesQuery()).thenReturn("");
        val results = CoreSerialization.mapper()
                .readValue(loadTextFromFile("schema/system_views_column_metadata.json"), new TypeReference<List<List<Object>>>() {
                }).stream()
                .map(JsonArray::new)
                .collect(Collectors.toList());
        when(client.getQueryResult(anyString())).thenReturn(Future.succeededFuture(new ResultSet().setResults(results)));
        when(datamartDao.existsDatamart(anyString())).thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(anyString(), anyString())).thenReturn(Future.succeededFuture(true));

        Promise<Void> promise = Promise.promise();
        informationSchemaService.initInformationSchema()
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        verify(client, times(3)).executeBatch(anyList());
        verify(datamartDao).getDatamarts();
        verify(informationSchemaQueryFactory).createInitEntitiesQuery();
        verify(client).getQueryResult(anyString());
        verify(datamartDao).existsDatamart(anyString());
        verify(entityDao, times(10)).existsEntity(anyString(), anyString());
    }

    @Test
    void initInformationSchemaEmptyDtmNotExistedDatasourcesSuccess() throws JsonProcessingException {
        when(client.executeBatch(anyList())).thenReturn(Future.succeededFuture());
        when(datamartDao.getDatamarts()).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(informationSchemaQueryFactory.createInitEntitiesQuery()).thenReturn("");
        val results = CoreSerialization.mapper()
                .readValue(loadTextFromFile("schema/system_views_column_metadata.json"), new TypeReference<List<List<Object>>>() {
                }).stream()
                .map(JsonArray::new)
                .collect(Collectors.toList());
        when(client.getQueryResult(anyString())).thenReturn(Future.succeededFuture(new ResultSet().setResults(results)));
        when(datamartDao.existsDatamart(anyString())).thenReturn(Future.succeededFuture(false));
        when(datamartDao.createDatamart(anyString())).thenReturn(Future.succeededFuture());
        when(entityDao.existsEntity(anyString(), anyString())).thenReturn(Future.succeededFuture(false));
        when(entityDao.createEntity(any(Entity.class))).thenReturn(Future.succeededFuture());

        Promise<Void> promise = Promise.promise();
        informationSchemaService.initInformationSchema()
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        verify(client, times(3)).executeBatch(anyList());
        verify(datamartDao).getDatamarts();
        verify(informationSchemaQueryFactory).createInitEntitiesQuery();
        verify(client).getQueryResult(anyString());
        verify(datamartDao).existsDatamart(anyString());
        verify(entityDao, times(10)).existsEntity(anyString(), anyString());
    }

    @Test
    void initInformationSchemaExistedDatasourcesSuccess() throws JsonProcessingException {
        when(client.executeBatch(anyList()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.succeededFuture());
        when(datamartDao.getDatamarts()).thenReturn(Future.succeededFuture(Collections.singletonList(DATAMART)));
        when(entityDao.getEntityNamesByDatamart(DATAMART)).thenReturn(Future.succeededFuture(Arrays.asList(TABLE_ENTITY, VIEW_ENTITY, MATVIEW_ENTITY)));
        when(entityDao.getEntity(eq(DATAMART), anyString()))
                .thenReturn(Future.succeededFuture(ENTITIES.get(0)))
                .thenReturn(Future.succeededFuture(ENTITIES.get(1)))
                .thenReturn(Future.succeededFuture(ENTITIES.get(2)));

        when(informationSchemaQueryFactory.createInitEntitiesQuery()).thenReturn("");
        val results = CoreSerialization.mapper()
                .readValue(loadTextFromFile("schema/system_views_column_metadata.json"), new TypeReference<List<List<Object>>>() {
                }).stream()
                .map(JsonArray::new)
                .collect(Collectors.toList());
        when(client.getQueryResult(anyString())).thenReturn(Future.succeededFuture(new ResultSet().setResults(results)));
        when(datamartDao.existsDatamart(anyString())).thenReturn(Future.succeededFuture(true));
        when(entityDao.existsEntity(anyString(), anyString())).thenReturn(Future.succeededFuture(true));
        when(ddlQueryGenerator.generateCreateTableQuery(any())).thenReturn("");
        when(ddlQueryGenerator.generateCreateViewQuery(any(), anyString())).thenReturn("");

        Promise<Void> promise = Promise.promise();
        informationSchemaService.initInformationSchema()
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        verify(client, times(3)).executeBatch(anyList());
        verify(datamartDao).getDatamarts();
        verify(entityDao).getEntityNamesByDatamart(DATAMART);
        verify(entityDao, times(3)).getEntity(eq(DATAMART), anyString());
        verify(informationSchemaQueryFactory).createInitEntitiesQuery();
        verify(client).getQueryResult(anyString());
        verify(datamartDao).existsDatamart(anyString());
        verify(entityDao, times(10)).existsEntity(anyString(), anyString());
        verify(ddlQueryGenerator, times(2)).generateCreateTableQuery(any(Entity.class));
        verify(ddlQueryGenerator, times(2)).generateCreateViewQuery(any(Entity.class), anyString());
        verify(materializedViewCacheService).put(any(EntityKey.class), any(MaterializedViewCacheValue.class));
    }
}
