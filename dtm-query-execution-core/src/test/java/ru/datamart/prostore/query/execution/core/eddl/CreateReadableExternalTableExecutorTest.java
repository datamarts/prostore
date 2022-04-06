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
package ru.datamart.prostore.query.execution.core.eddl;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.*;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.eddl.dto.CreateStandaloneExternalTableQuery;
import ru.datamart.prostore.query.execution.core.eddl.dto.EddlAction;
import ru.datamart.prostore.query.execution.core.eddl.service.standalone.CreateReadableExternalTableExecutor;
import ru.datamart.prostore.query.execution.core.eddl.service.standalone.UpdateInfoSchemaStandalonePostExecutor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class CreateReadableExternalTableExecutorTest {

    private static final String ENV = "test";
    private static final String DATAMART = "dtm";
    private static final Entity ENTITY_WITH_CREATE = getEntity(true);
    private static final Entity ENTITY_WITHOUT_CREATE = getEntity(false);
    private static final SourceType EXPECTED_SOURCE_TYPE = SourceType.ADP;
    private static final CreateStandaloneExternalTableQuery QUERY_WITH_CREATE = CreateStandaloneExternalTableQuery.builder()
            .eddlAction(EddlAction.CREATE_READABLE_EXTERNAL_TABLE)
            .envName(ENV)
            .schemaName(DATAMART)
            .entity(ENTITY_WITH_CREATE)
            .sourceType(EXPECTED_SOURCE_TYPE)
            .build();
    private static final CreateStandaloneExternalTableQuery QUERY_WITHOUT_CREATE = CreateStandaloneExternalTableQuery.builder()
            .eddlAction(EddlAction.CREATE_READABLE_EXTERNAL_TABLE)
            .envName(ENV)
            .schemaName(DATAMART)
            .entity(ENTITY_WITHOUT_CREATE)
            .sourceType(EXPECTED_SOURCE_TYPE)
            .build();

    @Mock
    private DataSourcePluginService dataSourcePluginService;

    @Mock
    private EntityDao entityDao;

    @Mock
    private DatamartDao datamartDao;

    @Mock
    private UpdateInfoSchemaStandalonePostExecutor postExecutor;

    @InjectMocks
    private CreateReadableExternalTableExecutor executor;

    @Test
    void shouldSuccessWhenAutoCreateTrue(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(DATAMART)).thenReturn(Future.succeededFuture(true));
        when(dataSourcePluginService.eddl(eq(EXPECTED_SOURCE_TYPE), any(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.createEntity(ENTITY_WITH_CREATE)).thenReturn(Future.succeededFuture());

        //act
        executor.execute(QUERY_WITH_CREATE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());

                    verify(dataSourcePluginService).eddl(eq(EXPECTED_SOURCE_TYPE), any(), any());
                    verify(entityDao).createEntity(ENTITY_WITH_CREATE);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenAutoCreateFalse(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(DATAMART)).thenReturn(Future.succeededFuture(true));
        when(entityDao.createEntity(ENTITY_WITHOUT_CREATE)).thenReturn(Future.succeededFuture());

        //act
        executor.execute(QUERY_WITHOUT_CREATE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());

                    verifyNoInteractions(dataSourcePluginService);
                    verify(entityDao).createEntity(ENTITY_WITHOUT_CREATE);
                }).completeNow());
    }

    @Test
    void shouldFailedWhenPluginEddlFailed(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(DATAMART)).thenReturn(Future.succeededFuture(true));
        when(dataSourcePluginService.eddl(eq(EXPECTED_SOURCE_TYPE), any(), any())).thenReturn(Future.failedFuture("error"));

        //act
        executor.execute(QUERY_WITH_CREATE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());

                    verify(dataSourcePluginService).eddl(eq(EXPECTED_SOURCE_TYPE), any(), any());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenCreateEntityFailedWithAutoCreate(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(DATAMART)).thenReturn(Future.succeededFuture(true));
        when(dataSourcePluginService.eddl(eq(EXPECTED_SOURCE_TYPE), any(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.createEntity(ENTITY_WITH_CREATE)).thenReturn(Future.failedFuture("error"));

        //act
        executor.execute(QUERY_WITH_CREATE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());

                    verify(dataSourcePluginService).eddl(eq(EXPECTED_SOURCE_TYPE), any(), any());
                    verify(entityDao).createEntity(ENTITY_WITH_CREATE);
                }).completeNow());
    }

    @Test
    void shouldFailedWhenCreateEntityFailedWithoutAutoCreate(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(DATAMART)).thenReturn(Future.succeededFuture(true));
        when(entityDao.createEntity(ENTITY_WITHOUT_CREATE)).thenReturn(Future.failedFuture("error"));

        //act
        executor.execute(QUERY_WITHOUT_CREATE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());

                    verifyNoInteractions(dataSourcePluginService);
                    verify(entityDao).createEntity(ENTITY_WITHOUT_CREATE);
                }).completeNow());
    }

    @Test
    void shouldFailedWhenDatamartNotExist(VertxTestContext testContext) {
        //arrange
        when(datamartDao.existsDatamart(DATAMART)).thenReturn(Future.failedFuture(new DtmException("Database dtm does not exist")));

        //act
        executor.execute(QUERY_WITH_CREATE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());

                    verifyNoInteractions(dataSourcePluginService, entityDao);
                    assertEquals("Database dtm does not exist", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void souldReturnCorrectEddlAction() {
        assertEquals(EddlAction.CREATE_READABLE_EXTERNAL_TABLE, executor.getAction());
    }

    private static Entity getEntity(boolean autoCreateTableEnabled) {
        Map<String, String> options = new HashMap<>();
        options.put("auto.create.table.enable", String.valueOf(autoCreateTableEnabled));
        return Entity.builder()
                .schema(DATAMART)
                .name("wr_ext_table")
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .destination(Collections.singleton(SourceType.ADP))
                .externalTableLocationType(ExternalTableLocationType.CORE_ADP)
                .externalTableLocationPath("public.debit")
                .externalTableOptions(options)
                .fields(Arrays.asList(EntityField.builder()
                                .name("id")
                                .type(ColumnType.INT)
                                .ordinalPosition(1)
                                .primaryOrder(1)
                                .shardingOrder(1)
                                .build(),
                        EntityField.builder()
                                .name("name")
                                .type(ColumnType.VARCHAR)
                                .size(30)
                                .ordinalPosition(2)
                                .build()))
                .build();
    }
}
