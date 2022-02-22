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

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckMaterializedView;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.factory.CheckMaterializedViewResultFactory;
import ru.datamart.prostore.query.execution.core.check.service.impl.CheckMaterializedViewExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.query.execution.core.check.factory.CheckMaterializedViewResultFactory.MatviewEntry;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class CheckMaterializedViewExecutorTest {

    private static final String REQUEST_DATAMART = "reqDatamart";
    private static final String QUERY_DATAMART = "queryDatamart";
    private static final String MATVIEW = "matview";
    private static final String TABLE = "table";
    private static final Long DELTA_NUM = 1L;

    private static final Entity MATVIEW_ENTITY = Entity.builder().entityType(EntityType.MATERIALIZED_VIEW).schema("schema").name("name").build();
    private static final Entity TABLE_ENTITY = Entity.builder().entityType(EntityType.TABLE).schema("schema").name("name").build();
    private static final MaterializedViewCacheValue CACHE_VALUE = new MaterializedViewCacheValue(MATVIEW_ENTITY);

    @Mock
    private CheckMaterializedViewResultFactory resultFactory;
    @Mock
    private EntityDao entityDao;
    @Mock
    private DeltaServiceDao deltaServiceDao;
    @Mock
    private CacheService<EntityKey, MaterializedViewCacheValue> cacheService;
    @InjectMocks
    private CheckMaterializedViewExecutor executor;
    @Captor
    private ArgumentCaptor<List<MatviewEntry>> entriesCaptor;

    @BeforeEach
    void setUp() {
        lenient().when(resultFactory.create(ArgumentMatchers.any(), ArgumentMatchers.anyList())).thenReturn(QueryResult.builder().build());
        lenient().when(cacheService.get(any())).thenReturn(CACHE_VALUE);
    }

    @Test
    void shouldSuccessWhenDatamartFromQuery(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, SqlNodeTemplates.identifier(QUERY_DATAMART, MATVIEW));

        when(entityDao.getEntity(anyString(), anyString())).thenReturn(Future.succeededFuture(MATVIEW_ENTITY));
        when(deltaServiceDao.getDeltaOk(anyString())).thenReturn(Future.succeededFuture(OkDelta.builder().deltaNum(DELTA_NUM).build()));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(entityDao).getEntity(eq(QUERY_DATAMART), eq(MATVIEW));
                    verify(deltaServiceDao).getDeltaOk(eq(QUERY_DATAMART));
                    verify(resultFactory).create(eq(DELTA_NUM), entriesCaptor.capture());

                    List<MatviewEntry> entries = entriesCaptor.getValue();
                    assertEquals(1, entries.size());
                    assertSame(entries.get(0).getEntity(), MATVIEW_ENTITY);
                    assertSame(entries.get(0).getCacheValue(), CACHE_VALUE);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenDatamartFromQueryAndNoOkDelta(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, SqlNodeTemplates.identifier(QUERY_DATAMART, MATVIEW));

        when(entityDao.getEntity(anyString(), anyString())).thenReturn(Future.succeededFuture(MATVIEW_ENTITY));
        when(deltaServiceDao.getDeltaOk(anyString())).thenReturn(Future.succeededFuture(null));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(entityDao).getEntity(eq(QUERY_DATAMART), eq(MATVIEW));
                    verify(deltaServiceDao).getDeltaOk(eq(QUERY_DATAMART));
                    verify(resultFactory).create(isNull(), entriesCaptor.capture());

                    List<MatviewEntry> entries = entriesCaptor.getValue();
                    assertEquals(1, entries.size());
                    assertSame(entries.get(0).getEntity(), MATVIEW_ENTITY);
                    assertSame(entries.get(0).getCacheValue(), CACHE_VALUE);
                }).completeNow());
    }

    @Test
    void shouldFailWhenDeltaGetFails(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, SqlNodeTemplates.identifier(QUERY_DATAMART, MATVIEW));

        when(entityDao.getEntity(anyString(), anyString())).thenReturn(Future.succeededFuture(MATVIEW_ENTITY));
        when(deltaServiceDao.getDeltaOk(anyString())).thenReturn(Future.failedFuture("Failure"));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Failure", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenEntityGetFails(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, SqlNodeTemplates.identifier(QUERY_DATAMART, MATVIEW));

        when(entityDao.getEntity(anyString(), anyString())).thenReturn(Future.failedFuture("Failure"));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Failure", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenEntityByDatamartGetFails(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, null);

        when(entityDao.getEntityNamesByDatamart(anyString())).thenReturn(Future.failedFuture("Failure"));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Failure", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenCacheGetFails(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, SqlNodeTemplates.identifier(QUERY_DATAMART, MATVIEW));

        when(entityDao.getEntity(anyString(), anyString())).thenReturn(Future.succeededFuture(MATVIEW_ENTITY));
        when(deltaServiceDao.getDeltaOk(anyString())).thenReturn(Future.succeededFuture(OkDelta.builder().deltaNum(DELTA_NUM).build()));
        reset(cacheService);
        when(cacheService.get(any())).thenThrow(new RuntimeException("Failure"));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Failure", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenResultFactoryFails(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, SqlNodeTemplates.identifier(QUERY_DATAMART, MATVIEW));

        when(entityDao.getEntity(anyString(), anyString())).thenReturn(Future.succeededFuture(MATVIEW_ENTITY));
        when(deltaServiceDao.getDeltaOk(anyString())).thenReturn(Future.succeededFuture(OkDelta.builder().deltaNum(DELTA_NUM).build()));
        reset(resultFactory);
        when(resultFactory.create(any(), anyList())).thenThrow(new RuntimeException("Failure"));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Failure", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldSuccessAndEmptyWhenMatviewDeletedInProcess(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, SqlNodeTemplates.identifier(QUERY_DATAMART, MATVIEW));

        when(entityDao.getEntity(anyString(), anyString())).thenReturn(Future.succeededFuture(MATVIEW_ENTITY));
        when(deltaServiceDao.getDeltaOk(anyString())).thenReturn(Future.succeededFuture(OkDelta.builder().deltaNum(DELTA_NUM).build()));
        reset(cacheService);
        when(cacheService.get(any())).thenReturn(null);

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(entityDao).getEntity(eq(QUERY_DATAMART), eq(MATVIEW));
                    verify(deltaServiceDao).getDeltaOk(eq(QUERY_DATAMART));
                    verify(resultFactory).create(eq(DELTA_NUM), entriesCaptor.capture());

                    List<MatviewEntry> entries = entriesCaptor.getValue();
                    assertTrue(entries.isEmpty());
                }).completeNow());
    }

    @Test
    void shouldFailWhenNoDatamart(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(null, null);

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Default datamart must be set or present in query", ar.cause().getMessage());
                    verifyNoInteractions(resultFactory, entityDao, deltaServiceDao, cacheService);
                }).completeNow());
    }

    @Test
    void shouldFailWhenEntityIsNotMaterializedView(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, SqlNodeTemplates.identifier(QUERY_DATAMART, TABLE));

        when(entityDao.getEntity(anyString(), anyString())).thenReturn(Future.succeededFuture(TABLE_ENTITY));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    verify(entityDao).getEntity(eq(QUERY_DATAMART), eq(TABLE));
                    assertEquals("Entity [schema.name] is not MATERIALIZED_VIEW", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenDatamartFromRequest(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, SqlNodeTemplates.identifier(MATVIEW));

        when(entityDao.getEntity(anyString(), anyString())).thenReturn(Future.succeededFuture(MATVIEW_ENTITY));
        when(deltaServiceDao.getDeltaOk(anyString())).thenReturn(Future.succeededFuture(OkDelta.builder().deltaNum(DELTA_NUM).build()));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(entityDao).getEntity(eq(REQUEST_DATAMART), eq(MATVIEW));
                    verify(deltaServiceDao).getDeltaOk(eq(REQUEST_DATAMART));
                    verify(resultFactory).create(eq(DELTA_NUM), entriesCaptor.capture());

                    List<MatviewEntry> entries = entriesCaptor.getValue();
                    assertEquals(1, entries.size());
                    assertSame(entries.get(0).getEntity(), MATVIEW_ENTITY);
                    assertSame(entries.get(0).getCacheValue(), CACHE_VALUE);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenNoIdInQueryAndMatviewInDatamart(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, null);

        when(entityDao.getEntityNamesByDatamart(anyString())).thenReturn(Future.succeededFuture(Collections.singletonList(MATVIEW)));
        when(entityDao.getEntity(anyString(), eq(MATVIEW))).thenReturn(Future.succeededFuture(MATVIEW_ENTITY));
        when(deltaServiceDao.getDeltaOk(anyString())).thenReturn(Future.succeededFuture(OkDelta.builder().deltaNum(DELTA_NUM).build()));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(entityDao).getEntity(eq(REQUEST_DATAMART), eq(MATVIEW));
                    verify(deltaServiceDao).getDeltaOk(eq(REQUEST_DATAMART));
                    verify(resultFactory).create(eq(DELTA_NUM), entriesCaptor.capture());

                    List<MatviewEntry> entries = entriesCaptor.getValue();
                    assertEquals(1, entries.size());
                    assertSame(entries.get(0).getEntity(), MATVIEW_ENTITY);
                    assertSame(entries.get(0).getCacheValue(), CACHE_VALUE);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenNoIdInQueryAndMultipleEntitiesInDatamart(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, null);

        when(entityDao.getEntityNamesByDatamart(anyString())).thenReturn(Future.succeededFuture(Arrays.asList(MATVIEW, TABLE)));
        when(entityDao.getEntity(anyString(), eq(MATVIEW))).thenReturn(Future.succeededFuture(MATVIEW_ENTITY));
        when(entityDao.getEntity(anyString(), eq(TABLE))).thenReturn(Future.succeededFuture(TABLE_ENTITY));
        when(deltaServiceDao.getDeltaOk(anyString())).thenReturn(Future.succeededFuture(OkDelta.builder().deltaNum(DELTA_NUM).build()));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(entityDao).getEntity(eq(REQUEST_DATAMART), eq(MATVIEW));
                    verify(entityDao).getEntity(eq(REQUEST_DATAMART), eq(TABLE));
                    verify(deltaServiceDao).getDeltaOk(eq(REQUEST_DATAMART));
                    verify(resultFactory).create(eq(DELTA_NUM), entriesCaptor.capture());

                    List<MatviewEntry> entries = entriesCaptor.getValue();
                    assertEquals(1, entries.size());
                    assertSame(entries.get(0).getEntity(), MATVIEW_ENTITY);
                    assertSame(entries.get(0).getCacheValue(), CACHE_VALUE);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenNoIdInQueryAndNoMatviewsInDatamart(VertxTestContext testContext) {
        // arrange
        val context = getCheckContext(REQUEST_DATAMART, null);

        when(entityDao.getEntityNamesByDatamart(anyString())).thenReturn(Future.succeededFuture(Collections.singletonList(TABLE)));
        when(entityDao.getEntity(anyString(), eq(TABLE))).thenReturn(Future.succeededFuture(TABLE_ENTITY));
        when(deltaServiceDao.getDeltaOk(anyString())).thenReturn(Future.succeededFuture(OkDelta.builder().deltaNum(DELTA_NUM).build()));

        // act
        executor.execute(context)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(entityDao).getEntity(eq(REQUEST_DATAMART), eq(TABLE));
                    verify(deltaServiceDao).getDeltaOk(eq(REQUEST_DATAMART));
                    verify(resultFactory).create(eq(DELTA_NUM), entriesCaptor.capture());

                    List<MatviewEntry> entries = entriesCaptor.getValue();
                    assertTrue(entries.isEmpty());
                }).completeNow());
    }

    private CheckContext getCheckContext(String requestDatamart, SqlIdentifier sqlNodeIdentifier) {
        return CheckContext.builder()
                .request(new DatamartRequest(QueryRequest.builder().datamartMnemonic(requestDatamart).build()))
                .sqlCheckCall(new SqlCheckMaterializedView(SqlParserPos.ZERO, sqlNodeIdentifier))
                .build();
    }
}