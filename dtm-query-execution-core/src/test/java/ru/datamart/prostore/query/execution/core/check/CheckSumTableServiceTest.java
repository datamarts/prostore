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
package ru.datamart.prostore.query.execution.core.check;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckSumType;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.dto.CheckSumRequestContext;
import ru.datamart.prostore.query.execution.core.check.exception.CheckSumException;
import ru.datamart.prostore.query.execution.core.check.service.impl.CheckSumTableService;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
class CheckSumTableServiceTest {

    private final static Set<SourceType> SOURCE_TYPES = Stream.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM)
            .collect(Collectors.toSet());
    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private CheckSumTableService checkSumTableService;
    private final static String DATAMART_MNEMONIC = "test";
    private final QueryRequest queryRequest = QueryRequest.builder().datamartMnemonic(DATAMART_MNEMONIC).build();

    @BeforeEach
    void setUp() {
        checkSumTableService = new CheckSumTableService(dataSourcePluginService, entityDao);
    }

    @Test
    void calcHashSumTableWithoutColumns(VertxTestContext testContext) {
        long hashInt32Value = 12345L;
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(10)
                .entity(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build())
                .build();

        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumTable(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                }).completeNow());
    }

    @Test
    void calcHashSumTableWithoutColumnsOnSnapshot(VertxTestContext testContext) {
        long hashInt32Value = 12345L;
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(10)
                .checksumType(CheckSumType.SNAPSHOT)
                .entity(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build())
                .build();

        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataSnapshotByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumTable(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    assertEquals(hashInt32Value, ar.result());
                }).completeNow());
    }

    @Test
    void calcHashSumTableWithColumns(VertxTestContext testContext) {
        long hashInt32Value = 12345L;
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(10)
                .entity(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build())
                .columns(new HashSet<>(Arrays.asList("f1", "f2")))
                .build();

        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumTable(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                }).completeNow());
    }

    @Test
    void calcHashSumTable(VertxTestContext testContext) {
        long hashInt32Value = 12345L;
        Set<SourceType> types = new HashSet<>(Collections.singletonList(SourceType.ADB));
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(0)
                .entity(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(types)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build())
                .build();
        when(dataSourcePluginService.getSourceTypes()).thenReturn(types);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumTable(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    assertEquals(hashInt32Value, ar.result());
                }).completeNow());
    }

    @Test
    void calcHashSumTableForSeveralSysCn(VertxTestContext testContext) {
        long hashInt32Value = 12345L;
        Set<SourceType> types = new HashSet<>(Collections.singletonList(SourceType.ADB));
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(1)
                .entity(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(types)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build())
                .build();
        when(dataSourcePluginService.getSourceTypes()).thenReturn(types);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumTable(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    assertEquals(hashInt32Value, ar.result());
                }).completeNow());
    }

    @Test
    void calcHashSumTableNonEqualPluginsHashSum(VertxTestContext testContext) {
        long hashInt32Value = 12345L;

        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(0)
                .entity(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build())
                .build();

        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);

        doAnswer(invocation -> {
            SourceType st = invocation.getArgument(0);
            if (st == SourceType.ADB) {
                return Future.succeededFuture(hashInt32Value);
            } else if (st == SourceType.ADQM) {
                return Future.succeededFuture(hashInt32Value);
            } else {
                return Future.succeededFuture(0L);
            }
        }).when(dataSourcePluginService).checkDataByHashInt32(any(), any(), any());

        checkSumTableService.calcCheckSumTable(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals(CheckSumException.class, ar.cause().getClass());
                }).completeNow());
    }

    @Test
    void calcHashSumAllTables(VertxTestContext testContext) {
        long hashInt32Value = 12345L;
        List<Entity> entities = Arrays.asList(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build(),
                Entity.builder()
                        .name("test_table_2")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build());
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(3)
                .build();

        when(entityDao.getEntityNamesByDatamart(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(entities.stream()
                        .map(Entity::getName)
                        .collect(Collectors.toList())));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(0).getName()))
                .thenReturn(Future.succeededFuture(entities.get(0)));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(1).getName()))
                .thenReturn(Future.succeededFuture(entities.get(1)));
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumForAllTables(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                }).completeNow());
    }

    @Test
    void calcHashSumAllTablesSeveralSysCn(VertxTestContext testContext) {
        long hashInt32Value = 12345L;
        List<Entity> entities = Arrays.asList(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build(),
                Entity.builder()
                        .name("test_table_2")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build());
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(1)
                .build();

        when(entityDao.getEntityNamesByDatamart(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(entities.stream()
                        .map(Entity::getName)
                        .collect(Collectors.toList())));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(0).getName()))
                .thenReturn(Future.succeededFuture(entities.get(0)));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(1).getName()))
                .thenReturn(Future.succeededFuture(entities.get(1)));
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumForAllTables(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                    assertEquals(hashInt32Value + hashInt32Value, ar.result());
                }).completeNow());
    }

    @Test
    void calcHashSumAllTablesGetEntityError(VertxTestContext testContext) {
        long hashInt32Value = 12345L;
        List<Entity> entities = Arrays.asList(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build(),
                Entity.builder()
                        .name("test_table_2")
                        .schema(DATAMART_MNEMONIC)
                        .entityType(EntityType.TABLE)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build());
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(3)
                .build();

        when(entityDao.getEntityNamesByDatamart(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(entities.stream()
                        .map(Entity::getName)
                        .collect(Collectors.toList())));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(0).getName()))
                .thenReturn(Future.succeededFuture(entities.get(0)));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(1).getName()))
                .thenReturn(Future.failedFuture(new DtmException("")));

        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));

        checkSumTableService.calcCheckSumForAllTables(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    @Test
    void calcHashSumAllTablesNonEqualsHashSums(VertxTestContext testContext) {
        long hashInt32Value = 12345L;
        List<Entity> entities = Arrays.asList(Entity.builder()
                        .name("test_table")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build(),
                Entity.builder()
                        .name("test_table_2")
                        .entityType(EntityType.TABLE)
                        .schema(DATAMART_MNEMONIC)
                        .destination(SOURCE_TYPES)
                        .fields(Arrays.asList(EntityField.builder()
                                        .name("f1")
                                        .build(),
                                EntityField.builder()
                                        .name("f2")
                                        .build(),
                                EntityField.builder()
                                        .name("f3")
                                        .build()))
                        .build());
        CheckSumRequestContext context = CheckSumRequestContext.builder()
                .checkContext(CheckContext.builder()
                        .metrics(new RequestMetrics())
                        .envName("env")
                        .request(new DatamartRequest(queryRequest))
                        .checkType(CheckType.SUM)
                        .build())
                .datamart(DATAMART_MNEMONIC)
                .cnFrom(0)
                .cnTo(3)
                .build();

        when(entityDao.getEntityNamesByDatamart(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(entities.stream()
                        .map(Entity::getName)
                        .collect(Collectors.toList())));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(0).getName()))
                .thenReturn(Future.succeededFuture(entities.get(0)));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entities.get(1).getName()))
                .thenReturn(Future.succeededFuture(entities.get(1)));
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkDataByHashInt32(any(), any(), any()))
                .thenReturn(Future.succeededFuture(hashInt32Value));
        doAnswer(invocation -> {
            SourceType st = invocation.getArgument(0);
            if (st == SourceType.ADB) {
                return Future.succeededFuture(hashInt32Value);
            } else if (st == SourceType.ADQM) {
                return Future.succeededFuture(hashInt32Value);
            } else {
                return Future.succeededFuture(0L);
            }
        }).when(dataSourcePluginService).checkDataByHashInt32(any(), any(), any());

        checkSumTableService.calcCheckSumForAllTables(context)
                .onComplete(ar -> testContext.verify(() -> {
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }
}
