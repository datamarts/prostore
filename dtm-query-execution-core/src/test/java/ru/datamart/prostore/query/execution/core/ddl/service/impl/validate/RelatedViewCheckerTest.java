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
package ru.datamart.prostore.query.execution.core.ddl.service.impl.validate;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.service.hsql.HSQLClient;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class RelatedViewCheckerTest {

    private static final String DATAMART = "datamart";
    private static final String TABLE = "table";
    private static final String VIEW_1 = "view1";
    private static final String VIEW_2 = "view2";

    @Mock
    private HSQLClient hsqlClient;

    @Mock
    private EntityDao entityDao;

    @InjectMocks
    private RelatedViewChecker relatedViewChecker;

    @Test
    void shouldSuccessWhenCheckDatamartAndNoViews(VertxTestContext testContext) {
        // arrange
        val resultSet = new ResultSet();
        resultSet.setResults(new ArrayList<>());
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));

        // act
        relatedViewChecker.checkRelatedViews(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                }).completeNow());
    }

    @Test
    void shouldFailWhenCheckDatamartAndHasViews(VertxTestContext testContext) {
        // arrange
        val resultSet = new ResultSet();
        List<JsonArray> result = new ArrayList<>();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(DATAMART);
        jsonArray.add(VIEW_1);
        result.add(jsonArray);
        resultSet.setResults(result);
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));

        // act
        relatedViewChecker.checkRelatedViews(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals("Views [datamart.view1] using the 'DATAMART' must be dropped first", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenCheckEntityAndNoViews(VertxTestContext testContext) {
        // arrange
        val resultSet = new ResultSet();
        resultSet.setResults(new ArrayList<>());
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));

        val entity = Entity.builder()
                .schema(DATAMART)
                .name(TABLE)
                .destination(EnumSet.allOf(SourceType.class))
                .build();

        // act
        relatedViewChecker.checkRelatedViews(entity, SourceType.ADB)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenCheckEntityAndNoViewsAndNotPickedDatasourceType(VertxTestContext testContext) {
        // arrange
        val resultSet = new ResultSet();
        resultSet.setResults(new ArrayList<>());
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));

        val entity = Entity.builder()
                .schema(DATAMART)
                .name(TABLE)
                .destination(EnumSet.allOf(SourceType.class))
                .build();

        // act
        relatedViewChecker.checkRelatedViews(entity, null)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenCheckEntityAndMaterializedViewFoundThatLooksNotAtDeletedDatasource(VertxTestContext testContext) {
        // arrange
        val resultSet = new ResultSet();
        List<JsonArray> result = new ArrayList<>();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(DATAMART);
        jsonArray.add(VIEW_1);
        result.add(jsonArray);
        resultSet.setResults(result);
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        when(entityDao.getEntity(eq(DATAMART), eq(VIEW_1))).thenReturn(Future.succeededFuture(Entity.builder()
                .schema(DATAMART)
                .name(VIEW_1)
                .entityType(EntityType.MATERIALIZED_VIEW)
                .materializedDataSource(SourceType.ADB)
                .build()));

        val entity = Entity.builder()
                .schema(DATAMART)
                .name(TABLE)
                .destination(EnumSet.allOf(SourceType.class))
                .build();

        // act
        relatedViewChecker.checkRelatedViews(entity, SourceType.ADQM)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }
                }).completeNow());
    }

    @Test
    void shouldFailWhenCheckEntityAndMaterializedViewFoundThatLooksAtDeletedDatasource(VertxTestContext testContext) {
        // arrange
        val resultSet = new ResultSet();
        List<JsonArray> result = new ArrayList<>();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(DATAMART);
        jsonArray.add(VIEW_1);
        result.add(jsonArray);
        resultSet.setResults(result);
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        when(entityDao.getEntity(eq(DATAMART), eq(VIEW_1))).thenReturn(Future.succeededFuture(Entity.builder()
                .schema(DATAMART)
                .name(VIEW_1)
                .entityType(EntityType.MATERIALIZED_VIEW)
                .materializedDataSource(SourceType.ADB)
                .build()));

        val entity = Entity.builder()
                .schema(DATAMART)
                .name(TABLE)
                .destination(EnumSet.allOf(SourceType.class))
                .build();

        // act
        relatedViewChecker.checkRelatedViews(entity, SourceType.ADB)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals("Views [datamart.view1] using the 'TABLE' must be dropped first", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenCheckEntityAndMaterializedViewFoundAndAllDatasourceDeleted(VertxTestContext testContext) {
        // arrange
        val resultSet = new ResultSet();
        List<JsonArray> result = new ArrayList<>();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(DATAMART);
        jsonArray.add(VIEW_1);
        result.add(jsonArray);
        resultSet.setResults(result);
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        when(entityDao.getEntity(eq(DATAMART), eq(VIEW_1))).thenReturn(Future.succeededFuture(Entity.builder()
                .schema(DATAMART)
                .name(VIEW_1)
                .entityType(EntityType.MATERIALIZED_VIEW)
                .materializedDataSource(SourceType.ADB)
                .build()));

        val entity = Entity.builder()
                .schema(DATAMART)
                .name(TABLE)
                .destination(EnumSet.allOf(SourceType.class))
                .build();

        // act
        relatedViewChecker.checkRelatedViews(entity, null)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals("Views [datamart.view1] using the 'TABLE' must be dropped first", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenCheckEntityAndViewFoundWithPickedDatasource(VertxTestContext testContext) {
        // arrange
        val resultSet = new ResultSet();
        List<JsonArray> result = new ArrayList<>();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(DATAMART);
        jsonArray.add(VIEW_1);
        result.add(jsonArray);
        resultSet.setResults(result);
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        when(entityDao.getEntity(eq(DATAMART), eq(VIEW_1))).thenReturn(Future.succeededFuture(Entity.builder()
                .schema(DATAMART)
                .name(VIEW_1)
                .entityType(EntityType.VIEW)
                .build()));

        val entity = Entity.builder()
                .schema(DATAMART)
                .name(TABLE)
                .destination(EnumSet.allOf(SourceType.class))
                .build();

        // act
        relatedViewChecker.checkRelatedViews(entity, SourceType.ADQM)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals("Views [datamart.view1] using the 'TABLE' must be dropped first", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenCheckEntityAndViewFoundWithNoDatasource(VertxTestContext testContext) {
        // arrange
        val resultSet = new ResultSet();
        List<JsonArray> result = new ArrayList<>();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(DATAMART);
        jsonArray.add(VIEW_1);
        result.add(jsonArray);
        resultSet.setResults(result);
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        when(entityDao.getEntity(eq(DATAMART), eq(VIEW_1))).thenReturn(Future.succeededFuture(Entity.builder()
                .schema(DATAMART)
                .name(VIEW_1)
                .entityType(EntityType.VIEW)
                .build()));

        val entity = Entity.builder()
                .schema(DATAMART)
                .name(TABLE)
                .destination(EnumSet.allOf(SourceType.class))
                .build();

        // act
        relatedViewChecker.checkRelatedViews(entity, null)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals("Views [datamart.view1] using the 'TABLE' must be dropped first", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenMultipleViewsFoundAndOneSafeToDeleteMatview(VertxTestContext testContext) {
        // arrange
        val resultSet = new ResultSet();
        List<JsonArray> result = new ArrayList<>();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(DATAMART);
        jsonArray.add(VIEW_1);
        result.add(jsonArray);
        JsonArray jsonArray2 = new JsonArray();
        jsonArray2.add(DATAMART);
        jsonArray2.add(VIEW_2);
        result.add(jsonArray2);
        resultSet.setResults(result);
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        when(entityDao.getEntity(eq(DATAMART), eq(VIEW_1))).thenReturn(Future.succeededFuture(Entity.builder()
                .schema(DATAMART)
                .name(VIEW_1)
                .entityType(EntityType.VIEW)
                .build()));
        when(entityDao.getEntity(eq(DATAMART), eq(VIEW_2))).thenReturn(Future.succeededFuture(Entity.builder()
                .schema(DATAMART)
                .name(VIEW_2)
                .entityType(EntityType.MATERIALIZED_VIEW)
                .materializedDataSource(SourceType.ADB)
                .build()));

        val entity = Entity.builder()
                .schema(DATAMART)
                .name(TABLE)
                .destination(EnumSet.allOf(SourceType.class))
                .build();

        // act
        relatedViewChecker.checkRelatedViews(entity, SourceType.ADQM)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals("Views [datamart.view1] using the 'TABLE' must be dropped first", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenMultipleViewsFoundAndOneNotSafeToDeleteMatview(VertxTestContext testContext) {
        // arrange
        val resultSet = new ResultSet();
        List<JsonArray> result = new ArrayList<>();
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(DATAMART);
        jsonArray.add(VIEW_1);
        result.add(jsonArray);
        JsonArray jsonArray2 = new JsonArray();
        jsonArray2.add(DATAMART);
        jsonArray2.add(VIEW_2);
        result.add(jsonArray2);
        resultSet.setResults(result);
        when(hsqlClient.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        when(entityDao.getEntity(eq(DATAMART), eq(VIEW_1))).thenReturn(Future.succeededFuture(Entity.builder()
                .schema(DATAMART)
                .name(VIEW_1)
                .entityType(EntityType.VIEW)
                .build()));
        when(entityDao.getEntity(eq(DATAMART), eq(VIEW_2))).thenReturn(Future.succeededFuture(Entity.builder()
                .schema(DATAMART)
                .name(VIEW_2)
                .entityType(EntityType.MATERIALIZED_VIEW)
                .materializedDataSource(SourceType.ADB)
                .build()));

        val entity = Entity.builder()
                .schema(DATAMART)
                .name(TABLE)
                .destination(EnumSet.allOf(SourceType.class))
                .build();

        // act
        relatedViewChecker.checkRelatedViews(entity, SourceType.ADB)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                    assertEquals("Views [datamart.view1, datamart.view2] using the 'TABLE' must be dropped first", ar.cause().getMessage());
                }).completeNow());
    }
}