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
package ru.datamart.prostore.query.execution.plugin.adp.dml;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.factory.AdpCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.factory.AdpSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.service.AdpCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.service.AdpCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.enrichment.service.AdpDmlQueryExtendService;
import ru.datamart.prostore.query.execution.plugin.adp.enrichment.service.AdpQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adp.enrichment.service.AdpQueryGenerator;
import ru.datamart.prostore.query.execution.plugin.adp.enrichment.service.AdpSchemaExtender;
import ru.datamart.prostore.query.execution.plugin.adp.mppw.dto.AdpTransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.adp.mppw.transfer.AdpTransferDataService;
import ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.DeleteRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdpDeleteServiceTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final QueryExtendService queryExtender = new AdpDmlQueryExtendService();
    private final AdpCalciteContextProvider contextProvider = new AdpCalciteContextProvider(
            calciteConfiguration.configDdlParser(calciteConfiguration.ddlParserImplFactory()),
            new AdpCalciteSchemaFactory(new AdpSchemaFactory()));
    private final SqlDialect sqlDialect = calciteConfiguration.adpSqlDialect();
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
    private final AdpQueryGenerator adpQueryGeneratorimpl = new AdpQueryGenerator(queryExtender, calciteConfiguration.adpSqlDialect(), relToSqlConverter);
    private final QueryEnrichmentService adpQueryEnrichmentService = new AdpQueryEnrichmentService(adpQueryGeneratorimpl, contextProvider, new AdpSchemaExtender());

    @Mock
    private DatabaseExecutor executor;

    @Mock
    private AdpTransferDataService mppwTransferDataHandler;

    @Captor
    private ArgumentCaptor<String> executorArgCaptor;

    @Captor
    private ArgumentCaptor<AdpTransferDataRequest> transferRequestCaptor;

    private AdpDeleteService adpDeleteService;

    @BeforeEach
    void setUp(Vertx vertx) {
        val queryParserService = new AdpCalciteDMLQueryParserService(contextProvider, vertx);
        val templateExtractor = new AdpQueryTemplateExtractor(TestUtils.DEFINITION_SERVICE, sqlDialect);
        adpDeleteService = new AdpDeleteService(executor, mppwTransferDataHandler, adpQueryEnrichmentService, queryParserService, templateExtractor, calciteConfiguration.adpSqlDialect());

        lenient().when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        lenient().when(mppwTransferDataHandler.transferData(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenCondition(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ? AND col2 <> ?", true);

        // act
        adpDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.failed()) {
                            fail(ar.cause());
                        }

                        verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                        val executorParam = executorArgCaptor.getValue();
                        assertEquals("INSERT INTO datamart.abc_staging (id,sys_op) SELECT id, 1 AS EXPR__1 FROM (SELECT id, col1, col2 FROM datamart.abc_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t0 WHERE id > 0 AND (col1 = 1 AND col2 <> 2)", executorParam);

                        verify(mppwTransferDataHandler).transferData(transferRequestCaptor.capture());
                        val mppwRequest = transferRequestCaptor.getValue();
                        assertEquals(request.getDatamartMnemonic(), mppwRequest.getDatamart());
                        assertEquals(request.getEntity().getName(), mppwRequest.getTableName());
                        assertEquals(Arrays.asList("id", "col1", "col2"), mppwRequest.getAllFields());
                        assertEquals(Arrays.asList("id"), mppwRequest.getPrimaryKeys());
                        assertEquals(request.getSysCn(), mppwRequest.getSysCn());
                    }).completeNow();
                });
    }

    @Test
    void shouldSuccessWhenNotNullableFields(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ? AND col2 <> ?", false);

        // act
        adpDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.failed()) {
                            fail(ar.cause());
                        }

                        verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                        val executorParam = executorArgCaptor.getValue();
                        assertEquals("INSERT INTO datamart.abc_staging (id,col2,sys_op) SELECT id, col2, 1 AS EXPR__2 FROM (SELECT id, col1, col2 FROM datamart.abc_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t0 WHERE id > 0 AND (col1 = 1 AND col2 <> 2)", executorParam);

                        verify(mppwTransferDataHandler).transferData(transferRequestCaptor.capture());
                        val mppwRequest = transferRequestCaptor.getValue();
                        assertEquals(request.getDatamartMnemonic(), mppwRequest.getDatamart());
                        assertEquals(request.getEntity().getName(), mppwRequest.getTableName());
                        assertEquals(Arrays.asList("id", "col1", "col2"), mppwRequest.getAllFields());
                        assertEquals(Arrays.asList("id"), mppwRequest.getPrimaryKeys());
                        assertEquals(request.getSysCn(), mppwRequest.getSysCn());
                    }).completeNow();
                });
    }

    @Test
    void shouldSuccessWhenNoCondition(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc", true);

        // act
        adpDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.failed()) {
                            fail(ar.cause());
                        }

                        verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                        val executorParam = executorArgCaptor.getValue();
                        assertEquals("INSERT INTO datamart.abc_staging (id,sys_op) SELECT id, 1 AS EXPR__1 FROM datamart.abc_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0", executorParam);

                        verify(mppwTransferDataHandler).transferData(transferRequestCaptor.capture());
                        val mppwRequest = transferRequestCaptor.getValue();
                        assertEquals(request.getDatamartMnemonic(), mppwRequest.getDatamart());
                        assertEquals(request.getEntity().getName(), mppwRequest.getTableName());
                        assertEquals(Arrays.asList("id", "col1", "col2"), mppwRequest.getAllFields());
                        assertEquals(Arrays.asList("id"), mppwRequest.getPrimaryKeys());
                        assertEquals(request.getSysCn(), mppwRequest.getSysCn());
                    }).completeNow();
                });
    }

    @Test
    void shouldSuccessWhenConditionAndAlias(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc as a WHERE a.id > ? AND a.col1 = ? AND a.col2 <> ?", true);

        // act
        adpDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.failed()) {
                            fail(ar.cause());
                        }

                        verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                        val executorParam = executorArgCaptor.getValue();
                        assertEquals("INSERT INTO datamart.abc_staging (id,sys_op) SELECT id, 1 AS EXPR__1 FROM (SELECT id, col1, col2 FROM datamart.abc_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t0 WHERE id > 0 AND (col1 = 1 AND col2 <> 2)", executorParam);

                        verify(mppwTransferDataHandler).transferData(transferRequestCaptor.capture());
                        val mppwRequest = transferRequestCaptor.getValue();
                        assertEquals(request.getDatamartMnemonic(), mppwRequest.getDatamart());
                        assertEquals(request.getEntity().getName(), mppwRequest.getTableName());
                        assertEquals(Arrays.asList("id", "col1", "col2"), mppwRequest.getAllFields());
                        assertEquals(Arrays.asList("id"), mppwRequest.getPrimaryKeys());
                        assertEquals(request.getSysCn(), mppwRequest.getSysCn());
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenWrongQuery(VertxTestContext testContext) {
        // arrange
        val request = getDeleteRequest("DELETE FROM abc WHERE unknown_col = ?", true);

        // act
        adpDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.succeeded()) {
                            fail("Unexpected success");
                        }
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenExecutorThrows(VertxTestContext testContext) {
        // arrange
        reset(executor);
        when(executor.executeWithParams(anyString(), any(), any())).thenThrow(new RuntimeException("Exception"));
        val request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ? AND col2 <> ?", true);

        // act
        adpDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.succeeded()) {
                            fail("Unexpected success");
                        }
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenExecutorFails(VertxTestContext testContext) {
        // arrange
        reset(executor);
        when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.failedFuture("Failed"));
        val request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ? AND col2 <> ?", true);

        // act
        adpDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.succeeded()) {
                            fail("Unexpected success");
                        }
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenTransferThrows(VertxTestContext testContext) {
        // arrange
        reset(mppwTransferDataHandler);
        when(mppwTransferDataHandler.transferData(any())).thenThrow(new RuntimeException("Exception"));
        val request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ? AND col2 <> ?", true);

        // act
        adpDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.succeeded()) {
                            fail("Unexpected success");
                        }
                    }).completeNow();
                });
    }

    @Test
    void shouldFailWhenTransferFails(VertxTestContext testContext) {
        // arrange
        reset(mppwTransferDataHandler);
        when(mppwTransferDataHandler.transferData(any())).thenReturn(Future.failedFuture("Failed"));
        val request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ? AND col2 <> ?", true);

        // act
        adpDeleteService.execute(request)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        // assert
                        if (ar.succeeded()) {
                            fail("Unexpected success");
                        }
                    }).completeNow();
                });
    }

    private DeleteRequest getDeleteRequest(String sql, boolean nullable) {
        val sqlNode = (SqlDelete) TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val entity = Entity.builder()
                .name("abc")
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