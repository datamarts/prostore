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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service.delete;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.converter.AdgPluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryExecutorService;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryTemplateExtractor;
import ru.datamart.prostore.query.execution.plugin.api.request.DeleteRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;
import static ru.datamart.prostore.query.execution.plugin.adg.utils.TestUtils.DEFINITION_SERVICE;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdgStandaloneDeleteServiceTest {

    private final AdgCalciteConfiguration calciteConfiguration = new AdgCalciteConfiguration();
    private final SqlDialect sqlDialect = calciteConfiguration.adgSqlDialect();

    @Mock
    private AdgQueryExecutorService executor;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    private AdgStandaloneDeleteService deleteService;

    @BeforeEach
    void setUp() {
        val templateExtractor = new AdgQueryTemplateExtractor(DEFINITION_SERVICE, sqlDialect);
        deleteService = new AdgStandaloneDeleteService(executor, new AdgPluginSpecificLiteralConverter(), templateExtractor, sqlDialect);

        lenient().when(executor.executeUpdate(anyString(), any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenCondition(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ?");

        // act
        deleteService.execute(request).onComplete(testContext.succeeding(result ->
                testContext.verify(() -> {
                    // assert
                    verify(executor).executeUpdate(sqlCaptor.capture(), any());
                    String sql = sqlCaptor.getValue();
                    assertEquals("DELETE FROM \"standalone\" WHERE \"id\" > 10 AND \"col1\" = '17532'", sql);
                }).completeNow()));
    }

    @Test
    void shouldSuccessWhenNoCondition(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequestWithoutCondition();

        // act
        deleteService.execute(request).onComplete(testContext.succeeding(result ->
                testContext.verify(() -> {
                    // assert
                    verify(executor).executeUpdate(sqlCaptor.capture(), any());
                    String sql = sqlCaptor.getValue();
                    assertEquals("DELETE FROM \"standalone\"", sql);
                }).completeNow()));
    }

    @Test
    void shouldSuccessWhenConditionAndAlias(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc as a WHERE a.id > ? AND a.col1 = ?");

        // act
        deleteService.execute(request).onComplete(testContext.succeeding(result ->
                testContext.verify(() -> {
                    // assert
                    verify(executor).executeUpdate(sqlCaptor.capture(), any());
                    String sql = sqlCaptor.getValue();
                    assertEquals("DELETE FROM \"standalone\" AS \"a\" WHERE \"a\".\"id\" > 10 AND \"a\".\"col1\" = '17532'", sql);
                }).completeNow()));
    }

    @Test
    void shouldFailWhenWrongQuery(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE unknown_col = ?");

        // act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    @Test
    void shouldFailWhenExecutorThrows(VertxTestContext testContext) {
        // arrange
        reset(executor);
        when(executor.executeUpdate(any(), any())).thenThrow(new RuntimeException("Exception"));
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ?");

        // act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    @Test
    void shouldFailWhenExecutorFails(VertxTestContext testContext) {
        // arrange
        reset(executor);
        when(executor.executeUpdate(any(), any())).thenReturn(Future.failedFuture("Failed"));
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ?");

        // act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenConditionWithPreparedStatementParams(VertxTestContext testContext) {
        // arrange
        val sqlNode = (SqlDelete) DEFINITION_SERVICE.processingQuery("DELETE FROM abc WHERE id > ? AND col1 = ?");
        Entity entity = getEntity();
        entity.setEntityType(EntityType.WRITEABLE_EXTERNAL_TABLE);
        entity.setExternalTableLocationPath("table");

        val schema = Datamart.builder()
                .mnemonic("datamart")
                .entities(singletonList(entity))
                .isDefault(true)
                .build();

        val extractedParams = Arrays.<SqlNode>asList(new SqlDynamicParam(0, SqlParserPos.ZERO), new SqlDynamicParam(1, SqlParserPos.ZERO));
        val parameters = new QueryParameters(Arrays.asList(10L, 17532L), Arrays.asList(ColumnType.BIGINT, ColumnType.DATE));
        val request = new DeleteRequest(UUID.randomUUID(), "dev", "datamart", entity, sqlNode, 1L, 0L,
                Collections.singletonList(schema), parameters, extractedParams, Arrays.asList(SqlTypeName.INTEGER, SqlTypeName.DATE));

        // act
        deleteService.execute(request).onComplete(testContext.succeeding(result ->
                testContext.verify(() -> {
                    // assert
                    verify(executor).executeUpdate(sqlCaptor.capture(), Mockito.same(parameters));
                    String sql = sqlCaptor.getValue();
                    assertEquals("DELETE FROM \"table\" WHERE \"id\" > ? AND \"col1\" = ?", sql);
                }).completeNow()));
    }

    private DeleteRequest getDeleteRequestWithoutCondition() {
        val sqlNode = (SqlDelete) DEFINITION_SERVICE.processingQuery("DELETE FROM abc");
        Entity entity = getEntity();

        val schema = Datamart.builder()
                .mnemonic("datamart")
                .entities(singletonList(entity))
                .isDefault(true)
                .build();

        return new DeleteRequest(UUID.randomUUID(), "dev", "datamart", entity, sqlNode, 1L, 0L, Collections.singletonList(schema), null, null, null);
    }

    private DeleteRequest getDeleteRequest(String sql) {
        val sqlNode = (SqlDelete) DEFINITION_SERVICE.processingQuery(sql);
        Entity entity = getEntity();

        val schema = Datamart.builder()
                .mnemonic("datamart")
                .entities(singletonList(entity))
                .isDefault(true)
                .build();

        return new DeleteRequest(UUID.randomUUID(), "dev", "datamart", entity, sqlNode, 1L, 0L, Collections.singletonList(schema), null, getExtractedParams(), Arrays.asList(SqlTypeName.INTEGER, SqlTypeName.DATE));
    }

    private Entity getEntity() {
        return Entity.builder()
                .name("abc")
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .externalTableLocationPath("standalone")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.DATE)
                                .nullable(true)
                                .build()
                ))
                .build();
    }

    private List<SqlNode> getExtractedParams() {
        return Arrays.asList(SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO),
                SqlLiteral.createCharString("2018-01-01", SqlParserPos.ZERO));
    }

}