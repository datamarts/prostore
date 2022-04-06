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
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.validate.RelatedViewChecker;
import ru.datamart.prostore.query.execution.core.eddl.dto.DropStandaloneExternalTableQuery;
import ru.datamart.prostore.query.execution.core.eddl.dto.EddlAction;
import ru.datamart.prostore.query.execution.core.eddl.service.standalone.DropReadableExternalTableExecutor;
import ru.datamart.prostore.query.execution.core.eddl.service.standalone.UpdateInfoSchemaStandalonePostExecutor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class DropReadableExternalTableExecutorTest {

    private static final String ENV = "test";
    private static final String DATAMART = "dtm";
    private static final String WR_EXT_TABLE_NAME = "wr_ext_table";
    private static final Entity READABLE_ENTITY = getEntity(EntityType.READABLE_EXTERNAL_TABLE);
    private static final Entity NOT_READABLE_ENTITY = getEntity(EntityType.TABLE);
    private static final SourceType EXPECTED_SOURCE_TYPE = SourceType.ADP;
    private static final DropStandaloneExternalTableQuery QUERY_WITH_DROP = DropStandaloneExternalTableQuery.builder()
            .eddlAction(EddlAction.DROP_READABLE_EXTERNAL_TABLE)
            .envName(ENV)
            .schemaName(DATAMART)
            .tableName(WR_EXT_TABLE_NAME)
            .options(getDropCreateOption(true))
            .build();
    private static final DropStandaloneExternalTableQuery QUERY_WITHOUT_DROP = DropStandaloneExternalTableQuery.builder()
            .eddlAction(EddlAction.DROP_READABLE_EXTERNAL_TABLE)
            .envName(ENV)
            .schemaName(DATAMART)
            .tableName(WR_EXT_TABLE_NAME)
            .options(getDropCreateOption(false))
            .build();

    @Mock
    private DataSourcePluginService dataSourcePluginService;

    @Mock
    private EntityDao entityDao;

    @Mock
    private RelatedViewChecker relatedViewChecker;

    @Mock
    private EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Mock
    private UpdateInfoSchemaStandalonePostExecutor postExecutor;

    @InjectMocks
    private DropReadableExternalTableExecutor executor;

    @Test
    void shouldSuccessWhenAutoDropTrue(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.succeededFuture(READABLE_ENTITY));
        when(relatedViewChecker.checkRelatedViews(any(), any())).thenReturn(Future.succeededFuture());
        when(dataSourcePluginService.eddl(eq(EXPECTED_SOURCE_TYPE), any(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.deleteEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.succeededFuture());

        //act
        executor.execute(QUERY_WITH_DROP)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());

                    verify(entityDao).getEntity(DATAMART, WR_EXT_TABLE_NAME);
                    verify(dataSourcePluginService).eddl(eq(EXPECTED_SOURCE_TYPE), any(), any());
                    verify(entityDao).deleteEntity(DATAMART, WR_EXT_TABLE_NAME);
                    verify(evictQueryTemplateCacheService).evictByEntityName(eq(DATAMART), eq(WR_EXT_TABLE_NAME));
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenAutoDropFalse(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.succeededFuture(READABLE_ENTITY));
        when(relatedViewChecker.checkRelatedViews(any(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.deleteEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.succeededFuture());

        //act
        executor.execute(QUERY_WITHOUT_DROP)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());

                    verify(entityDao).getEntity(DATAMART, WR_EXT_TABLE_NAME);
                    verifyNoInteractions(dataSourcePluginService);
                    verify(entityDao).deleteEntity(DATAMART, WR_EXT_TABLE_NAME);
                    verify(evictQueryTemplateCacheService).evictByEntityName(eq(DATAMART), eq(WR_EXT_TABLE_NAME));
                }).completeNow());
    }

    @Test
    void shouldFailedWhenGetEntityFailed(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.failedFuture(new EntityNotExistsException(DATAMART, WR_EXT_TABLE_NAME)));

        //act
        executor.execute(QUERY_WITH_DROP)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof EntityNotExistsException);

                    verify(entityDao).getEntity(DATAMART, WR_EXT_TABLE_NAME);
                    verifyNoInteractions(dataSourcePluginService);
                    verifyNoMoreInteractions(entityDao);
                }).completeNow());
    }

    @Test
    void shouldFailedWhenWrongEntityType(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.succeededFuture(NOT_READABLE_ENTITY));

        //act
        executor.execute(QUERY_WITH_DROP)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);

                    verify(entityDao).getEntity(DATAMART, WR_EXT_TABLE_NAME);
                    verifyNoInteractions(dataSourcePluginService);
                    verifyNoMoreInteractions(entityDao);
                }).completeNow());
    }

    @Test
    void shouldFailedWhenRelatedViewsExists(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.succeededFuture(READABLE_ENTITY));
        when(relatedViewChecker.checkRelatedViews(any(), any())).thenReturn(Future.failedFuture("Check failed"));

        //act
        executor.execute(QUERY_WITHOUT_DROP)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals("Check failed", ar.cause().getMessage());

                    verify(entityDao).getEntity(DATAMART, WR_EXT_TABLE_NAME);
                    verifyNoInteractions(dataSourcePluginService);
                    verifyNoMoreInteractions(entityDao);
                }).completeNow());
    }

    @Test
    void shouldFailedWhenPluginEddlFailed(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.succeededFuture(READABLE_ENTITY));
        when(relatedViewChecker.checkRelatedViews(any(), any())).thenReturn(Future.succeededFuture());
        when(dataSourcePluginService.eddl(eq(EXPECTED_SOURCE_TYPE), any(), any())).thenReturn(Future.failedFuture("error"));

        //act
        executor.execute(QUERY_WITH_DROP)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());

                    verify(entityDao).getEntity(DATAMART, WR_EXT_TABLE_NAME);
                    verify(dataSourcePluginService).eddl(eq(EXPECTED_SOURCE_TYPE), any(), any());
                    verifyNoMoreInteractions(entityDao);
                }).completeNow());
    }

    @Test
    void shouldFailedWhenDeleteEntityFailedWithAutoDrop(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.succeededFuture(READABLE_ENTITY));
        when(relatedViewChecker.checkRelatedViews(any(), any())).thenReturn(Future.succeededFuture());
        when(dataSourcePluginService.eddl(eq(EXPECTED_SOURCE_TYPE), any(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.deleteEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.failedFuture("error"));

        //act
        executor.execute(QUERY_WITH_DROP)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());

                    verify(entityDao).getEntity(DATAMART, WR_EXT_TABLE_NAME);
                    verify(dataSourcePluginService).eddl(eq(EXPECTED_SOURCE_TYPE), any(), any());
                    verify(entityDao).deleteEntity(DATAMART, WR_EXT_TABLE_NAME);
                }).completeNow());
    }

    @Test
    void shouldFailedWhenDeleteEntityFailedWithoutAutoDrop(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.succeededFuture(READABLE_ENTITY));
        when(relatedViewChecker.checkRelatedViews(any(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.deleteEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.failedFuture("error"));

        //act
        executor.execute(QUERY_WITHOUT_DROP)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());

                    verify(entityDao).getEntity(DATAMART, WR_EXT_TABLE_NAME);
                    verifyNoMoreInteractions(dataSourcePluginService);
                    verify(entityDao).deleteEntity(DATAMART, WR_EXT_TABLE_NAME);
                }).completeNow());
    }

    @Test
    void souldReturnCorrectEddlAction() {
        assertEquals(EddlAction.DROP_READABLE_EXTERNAL_TABLE, executor.getAction());
    }

    @Test
    void shouldFailWhenEvictFailed(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.succeededFuture(READABLE_ENTITY));
        when(relatedViewChecker.checkRelatedViews(any(), any())).thenReturn(Future.succeededFuture());
        when(dataSourcePluginService.eddl(eq(EXPECTED_SOURCE_TYPE), any(), any())).thenReturn(Future.succeededFuture());
        when(entityDao.deleteEntity(DATAMART, WR_EXT_TABLE_NAME)).thenReturn(Future.succeededFuture());
        doThrow(new DtmException("Exception")).when(evictQueryTemplateCacheService).evictByEntityName(any(), any());

        //act
        executor.execute(QUERY_WITH_DROP)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());

                    verify(entityDao).getEntity(DATAMART, WR_EXT_TABLE_NAME);
                    verify(dataSourcePluginService).eddl(eq(EXPECTED_SOURCE_TYPE), any(), any());
                    verify(entityDao).deleteEntity(DATAMART, WR_EXT_TABLE_NAME);
                }).completeNow());
    }

    private static Entity getEntity(EntityType entityType) {
        return Entity.builder()
                .schema(DATAMART)
                .name(WR_EXT_TABLE_NAME)
                .entityType(entityType)
                .destination(Collections.singleton(SourceType.ADP))
                .externalTableLocationType(ExternalTableLocationType.CORE_ADP)
                .build();
    }

    private static Map<String, String> getDropCreateOption(boolean autoCreateTableEnabled) {
        Map<String, String> options = new HashMap<>();
        options.put("auto.drop.table.enable", String.valueOf(autoCreateTableEnabled));
        return options;
    }
}
