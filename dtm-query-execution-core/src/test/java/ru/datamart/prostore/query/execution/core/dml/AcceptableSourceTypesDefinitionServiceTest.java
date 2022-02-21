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
package ru.datamart.prostore.query.execution.core.dml;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QuerySourceRequest;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.dml.service.AcceptableSourceTypesDefinitionService;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AcceptableSourceTypesDefinitionServiceTest {

    private final EntityDao entityDao = mock(EntityDao.class);
    private AcceptableSourceTypesDefinitionService acceptableSourceTypesDefinitionService;

    @BeforeEach
    void setUp() {
        acceptableSourceTypesDefinitionService = new AcceptableSourceTypesDefinitionService(entityDao);
    }

    @Test
    void getAcceptableSourceTypesSuccess() {
        Promise<Set<SourceType>> promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select t1.id from table_1 t1 join dtm_1.table_2 t2 " +
                "ON t2.id = t1.id JOIN dtm_2.table_3 t3 ON t3.id = t2.id");
        List<Datamart> schema = createLogicalSchema();
        Set<SourceType> expectedSourcetTypes = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG));
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        sourceRequest.setLogicalSchema(schema);
        sourceRequest.setQueryRequest(request);
        sourceRequest.setMetadata(Collections.singletonList(new ColumnMetadata("id", ColumnType.BIGINT)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(0)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(1).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(1)));

        when(entityDao.getEntity(eq(schema.get(1).getMnemonic()),
                eq(schema.get(1).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(1).getEntities().get(0)));

        acceptableSourceTypesDefinitionService.define(sourceRequest.getLogicalSchema())
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(expectedSourcetTypes, promise.future().result());
    }

    @Test
    void getAcceptableSourceTypesEntitiesFailed() {
        Promise<Set<SourceType>> promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select t1.id from table_1 t1 join dtm_1.table_2 t2 " +
                "ON t2.id = t1.id JOIN dtm_2.table_3 t3 ON t3.id = t2.id");
        List<Datamart> schema = createLogicalSchema();
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        sourceRequest.setLogicalSchema(schema);
        sourceRequest.setQueryRequest(request);
        sourceRequest.setMetadata(Collections.singletonList(new ColumnMetadata("id", ColumnType.BIGINT)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(0).getName())))
                .thenReturn(Future.failedFuture(new DtmException(schema.get(0).getEntities().get(0).getName())));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(1).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(1)));

        when(entityDao.getEntity(eq(schema.get(1).getMnemonic()),
                eq(schema.get(1).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(1).getEntities().get(0)));

        acceptableSourceTypesDefinitionService.define(sourceRequest.getLogicalSchema())
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }

    @Test
    void getAcceptableSourceTypesNoSingleDatasourceError() {
        Promise<Set<SourceType>> promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select t1.id from table_1 t1 join dtm_1.table_2 t2 " +
                "ON t2.id = t1.id JOIN dtm_2.table_3 t3 ON t3.id = t2.id");
        List<Datamart> schema = createLogicalSchema();
        schema.get(0).getEntities().get(0).setDestination(Collections.singleton(SourceType.ADB));
        schema.get(0).getEntities().get(1).setDestination(Collections.singleton(SourceType.ADG));
        schema.get(1).getEntities().get(0).setDestination(Collections.singleton(SourceType.ADQM));
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        sourceRequest.setLogicalSchema(schema);
        sourceRequest.setQueryRequest(request);
        sourceRequest.setMetadata(Collections.singletonList(new ColumnMetadata("id", ColumnType.BIGINT)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(0)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(1).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(1)));

        when(entityDao.getEntity(eq(schema.get(1).getMnemonic()),
                eq(schema.get(1).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(1).getEntities().get(0)));

        acceptableSourceTypesDefinitionService.define(sourceRequest.getLogicalSchema())
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }

    @Test
    void getAcceptableSourceTypesSingleTable() {
        Promise<Set<SourceType>> promise = Promise.promise();
        QueryRequest request = new QueryRequest();
        request.setSql("select t1.id from table_1 t1");
        List<Datamart> schema = createLogicalSchema();
        schema.get(0).getEntities().remove(1);
        schema.remove(1);
        schema.get(0).getEntities().get(0).setDestination(Collections.singleton(SourceType.ADB));
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        sourceRequest.setLogicalSchema(schema);
        sourceRequest.setQueryRequest(request);
        sourceRequest.setMetadata(Collections.singletonList(new ColumnMetadata("id", ColumnType.BIGINT)));

        when(entityDao.getEntity(eq(schema.get(0).getMnemonic()),
                eq(schema.get(0).getEntities().get(0).getName())))
                .thenReturn(Future.succeededFuture(schema.get(0).getEntities().get(0)));

        acceptableSourceTypesDefinitionService.define(sourceRequest.getLogicalSchema())
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
        assertEquals(new HashSet<>(Collections.singletonList(SourceType.ADB)), promise.future().result());
    }

    private List<Datamart> createLogicalSchema() {
        List<Entity> entities1 = new ArrayList<>();
        entities1.addAll(Arrays.asList(Entity.builder()
                        .name("table_1")
                        .schema("dtm_1")
                        .entityType(EntityType.TABLE)
                        .destination(new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG, SourceType.ADQM)))
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("id")
                                        .nullable(false)
                                        .type(ColumnType.BIGINT)
                                        .ordinalPosition(0)
                                        .primaryOrder(0)
                                        .build(),
                                EntityField.builder()
                                        .name("name")
                                        .nullable(true)
                                        .type(ColumnType.VARCHAR)
                                        .ordinalPosition(1)
                                        .size(100)
                                        .build()))
                        .build(),
                Entity.builder()
                        .name("table_2")
                        .schema("dtm_1")
                        .entityType(EntityType.TABLE)
                        .destination(new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG, SourceType.ADQM)))
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("id")
                                        .nullable(false)
                                        .type(ColumnType.BIGINT)
                                        .ordinalPosition(0)
                                        .primaryOrder(0)
                                        .build(),
                                EntityField.builder()
                                        .name("name")
                                        .nullable(true)
                                        .type(ColumnType.VARCHAR)
                                        .ordinalPosition(1)
                                        .size(100)
                                        .build()))
                        .build()
        ));
        List<Entity> entities2 = new ArrayList<>();
        entities2.add(Entity.builder()
                .name("table_3")
                .schema("dtm_2")
                .entityType(EntityType.TABLE)
                .destination(new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG)))
                .fields(Arrays.asList(EntityField.builder()
                                .name("id")
                                .nullable(false)
                                .type(ColumnType.BIGINT)
                                .ordinalPosition(0)
                                .primaryOrder(0)
                                .build(),
                        EntityField.builder()
                                .name("name")
                                .nullable(true)
                                .type(ColumnType.VARCHAR)
                                .ordinalPosition(1)
                                .size(100)
                                .build()
                ))
                .build());
        Datamart d1 = new Datamart("dtm_1", true, entities1);
        Datamart d2 = new Datamart("dtm_2", true, entities2);
        return new ArrayList<>(Arrays.asList(d1, d2));
    }
}
