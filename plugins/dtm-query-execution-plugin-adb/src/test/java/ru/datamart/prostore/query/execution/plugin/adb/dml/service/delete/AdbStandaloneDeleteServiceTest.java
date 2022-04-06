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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service.delete;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbDmlQueryExtendWithoutHistoryService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbSchemaExtender;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.impl.AdbQueryTemplateExtractor;
import ru.datamart.prostore.query.execution.plugin.adb.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.DeleteRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryExtendService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdbStandaloneDeleteServiceTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlDialect sqlDialect = calciteConfiguration.adbSqlDialect();

    @Mock
    private DatabaseExecutor executor;

    @Captor
    private ArgumentCaptor<String> executorArgCaptor;

    private AdbStandaloneDeleteService adbDeleteService;

    @BeforeEach
    void setUp(Vertx vertx) {
        val templateExtractor = new AdbQueryTemplateExtractor(TestUtils.DEFINITION_SERVICE, sqlDialect);
        adbDeleteService = new AdbStandaloneDeleteService(executor, templateExtractor, calciteConfiguration.adbSqlDialect());

        lenient().when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenCondition(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ? AND col2 <> ?", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                    val executorParam = executorArgCaptor.getValue();
                    assertEquals("DELETE FROM dtm.abc WHERE id > 0 AND col1 = 1 AND col2 <> 2", executorParam);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenNotNullableFields(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ? AND col2 <> ?", false);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                    val executorParam = executorArgCaptor.getValue();
                    assertEquals("DELETE FROM dtm.abc WHERE id > 0 AND col1 = 1 AND col2 <> 2", executorParam);

                }).completeNow());
    }

    @Test
    void shouldSuccessWhenNoCondition(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                    val executorParam = executorArgCaptor.getValue();
                    assertEquals("DELETE FROM dtm.abc", executorParam);

                }).completeNow());
    }

    @Test
    void shouldSuccessWhenConditionAndAlias(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc as a WHERE a.id > ? AND a.col1 = ? AND a.col2 <> ?", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                    val executorParam = executorArgCaptor.getValue();
                    assertEquals("DELETE FROM dtm.abc AS a WHERE a.id > 0 AND a.col1 = 1 AND a.col2 <> 2", executorParam);

                }).completeNow());
    }

    @Test
    void shouldFailWhenWrongQuery(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc WHERE unknown_col = ?", true);

        // act
        adbDeleteService.execute(request)
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
        when(executor.executeWithParams(anyString(), any(), any())).thenThrow(new RuntimeException("Exception"));
        val request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ? AND col2 <> ?", true);

        // act
        adbDeleteService.execute(request)
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
        when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.failedFuture("Failed"));
        val request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ? AND col2 <> ?", true);

        // act
        adbDeleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    private DeleteRequest getDeleteRequest(String sql, boolean nullable) {
        val sqlNode = (SqlDelete) TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val entity = Entity.builder()
                .name("abc")
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .externalTableLocationPath("dtm.abc")
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
                                .type(ColumnType.BIGINT)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.BIGINT)
                                .nullable(nullable)
                                .build()
                ))
                .build();

        val schema = Datamart.builder()
                .mnemonic("datamart")
                .entities(singletonList(entity))
                .isDefault(true)
                .build();

        return new DeleteRequest(UUID.randomUUID(), "dev", "datamart", entity, sqlNode, 1L, 0L, Collections.singletonList(schema), null, getExtractedParams(), Arrays.asList(SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.INTEGER));
    }


    private List<SqlNode> getExtractedParams() {
        return Arrays.asList(SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO));
    }
}