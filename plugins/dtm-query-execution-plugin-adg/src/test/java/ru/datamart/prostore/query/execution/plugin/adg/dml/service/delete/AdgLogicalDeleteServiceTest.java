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
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgTransferDataEtlRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.converter.AdgPluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.factory.AdgCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.factory.AdgSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.service.AdgCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.service.AdgCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adg.enrichment.service.AdgCollateValueReplacer;
import ru.datamart.prostore.query.execution.plugin.adg.enrichment.service.AdgDmlQueryExtendService;
import ru.datamart.prostore.query.execution.plugin.adg.enrichment.service.AdgQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adg.enrichment.service.AdgSchemaExtender;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryExecutorService;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryTemplateExtractor;
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
import static ru.datamart.prostore.query.execution.plugin.adg.utils.TestUtils.DEFINITION_SERVICE;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdgLogicalDeleteServiceTest {

    private final AdgCalciteConfiguration calciteConfiguration = new AdgCalciteConfiguration();
    private final AdgHelperTableNamesFactory helperTableNamesFactory = new AdgHelperTableNamesFactory();
    private final QueryExtendService queryExtender = new AdgDmlQueryExtendService(helperTableNamesFactory);
    private final AdgCalciteContextProvider contextProvider = new AdgCalciteContextProvider(
            calciteConfiguration.configDdlParser(calciteConfiguration.ddlParserImplFactory()),
            new AdgCalciteSchemaFactory(new AdgSchemaFactory()));
    private final SqlDialect sqlDialect = calciteConfiguration.adgSqlDialect();
    private final DtmRelToSqlConverter relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
    private final AdgCollateValueReplacer collateReplacer = new AdgCollateValueReplacer();
    private final AdgSchemaExtender adgSchemaExtender = new AdgSchemaExtender(helperTableNamesFactory);
    private final QueryEnrichmentService queryEnrichmentService = new AdgQueryEnrichmentService(queryExtender, sqlDialect, relToSqlConverter, collateReplacer);

    @Mock
    private AdgQueryExecutorService executor;

    @Mock
    private AdgCartridgeClient cartridgeClient;

    @Captor
    private ArgumentCaptor<AdgTransferDataEtlRequest> transferRequestCaptor;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    private AdgLogicalDeleteService deleteService;

    @BeforeEach
    void setUp(Vertx vertx) {
        val queryParserService = new AdgCalciteDMLQueryParserService(contextProvider, vertx, adgSchemaExtender);
        val templateExtractor = new AdgQueryTemplateExtractor(DEFINITION_SERVICE, sqlDialect);
        deleteService = new AdgLogicalDeleteService(executor, cartridgeClient, helperTableNamesFactory, queryEnrichmentService, queryParserService, new AdgPluginSpecificLiteralConverter(), templateExtractor, sqlDialect);

        lenient().when(executor.executeUpdate(anyString(), any())).thenReturn(Future.succeededFuture());
        lenient().when(cartridgeClient.transferDataToScdTable(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenCondition(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ?", true);

        // act
        deleteService.execute(request).onComplete(testContext.succeeding(result ->
                testContext.verify(() -> {
                    // assert
                    verify(executor).executeUpdate(sqlCaptor.capture(), any());
                    String sql = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"sys_op\") SELECT \"id\", 1 AS \"EXPR__1\" FROM (SELECT \"id\", \"col1\" FROM \"dev__datamart__abc_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\" FROM \"dev__datamart__abc_actual\" WHERE \"sys_from\" <= 0) AS \"t3\" WHERE \"id\" > 10 AND \"col1\" = '17532'", sql);

                    verify(cartridgeClient).transferDataToScdTable(transferRequestCaptor.capture());
                    val transferDataEtlRequest = transferRequestCaptor.getValue();
                    assertEquals("dev__datamart__abc_actual", transferDataEtlRequest.getHelperTableNames().getActual());
                    assertEquals("dev__datamart__abc_history", transferDataEtlRequest.getHelperTableNames().getHistory());
                    assertEquals("dev__datamart__abc_staging", transferDataEtlRequest.getHelperTableNames().getStaging());
                    assertEquals(request.getSysCn(), transferDataEtlRequest.getDeltaNumber());
                }).completeNow()));
    }

    @Test
    void shouldSuccessWhenNotNullableFields(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ?", false);

        // act
        deleteService.execute(request).onComplete(testContext.succeeding(result ->
                testContext.verify(() -> {
                    // assert
                    verify(executor).executeUpdate(sqlCaptor.capture(), any());
                    String sql = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"col1\",\"sys_op\") SELECT \"id\", \"col1\", 1 AS \"EXPR__2\" FROM (SELECT \"id\", \"col1\" FROM \"dev__datamart__abc_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\" FROM \"dev__datamart__abc_actual\" WHERE \"sys_from\" <= 0) AS \"t3\" WHERE \"id\" > 10 AND \"col1\" = '17532'", sql);

                    verify(cartridgeClient).transferDataToScdTable(transferRequestCaptor.capture());
                    val transferDataEtlRequest = transferRequestCaptor.getValue();
                    assertEquals("dev__datamart__abc_actual", transferDataEtlRequest.getHelperTableNames().getActual());
                    assertEquals("dev__datamart__abc_history", transferDataEtlRequest.getHelperTableNames().getHistory());
                    assertEquals("dev__datamart__abc_staging", transferDataEtlRequest.getHelperTableNames().getStaging());
                    assertEquals(request.getSysCn(), transferDataEtlRequest.getDeltaNumber());
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
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"sys_op\") SELECT \"id\", 1 AS \"EXPR__1\" FROM (SELECT \"id\", \"col1\" FROM \"dev__datamart__abc_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\" FROM \"dev__datamart__abc_actual\" WHERE \"sys_from\" <= 0) AS \"t3\"", sql);

                    verify(cartridgeClient).transferDataToScdTable(transferRequestCaptor.capture());
                    val transferDataEtlRequest = transferRequestCaptor.getValue();
                    assertEquals("dev__datamart__abc_actual", transferDataEtlRequest.getHelperTableNames().getActual());
                    assertEquals("dev__datamart__abc_history", transferDataEtlRequest.getHelperTableNames().getHistory());
                    assertEquals("dev__datamart__abc_staging", transferDataEtlRequest.getHelperTableNames().getStaging());
                    assertEquals(request.getSysCn(), transferDataEtlRequest.getDeltaNumber());
                }).completeNow()));
    }

    @Test
    void shouldSuccessWhenConditionAndAlias(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc as a WHERE a.id > ? AND a.col1 = ?", true);

        // act
        deleteService.execute(request).onComplete(testContext.succeeding(result ->
                testContext.verify(() -> {
                    // assert
                    verify(executor).executeUpdate(sqlCaptor.capture(), any());
                    String sql = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"sys_op\") SELECT \"id\", 1 AS \"EXPR__1\" FROM (SELECT \"id\", \"col1\" FROM \"dev__datamart__abc_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\" FROM \"dev__datamart__abc_actual\" WHERE \"sys_from\" <= 0) AS \"t3\" WHERE \"id\" > 10 AND \"col1\" = '17532'", sql);

                    verify(cartridgeClient).transferDataToScdTable(transferRequestCaptor.capture());
                    val transferDataEtlRequest = transferRequestCaptor.getValue();
                    assertEquals("dev__datamart__abc_actual", transferDataEtlRequest.getHelperTableNames().getActual());
                    assertEquals("dev__datamart__abc_history", transferDataEtlRequest.getHelperTableNames().getHistory());
                    assertEquals("dev__datamart__abc_staging", transferDataEtlRequest.getHelperTableNames().getStaging());
                    assertEquals(request.getSysCn(), transferDataEtlRequest.getDeltaNumber());
                }).completeNow()));
    }

    @Test
    void shouldFailWhenWrongQuery(VertxTestContext testContext) {
        // arrange
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE unknown_col = ?", true);

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
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ?", true);

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
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ?", true);

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
    void shouldFailWhenTransferThrows(VertxTestContext testContext) {
        // arrange
        reset(cartridgeClient);
        when(cartridgeClient.transferDataToScdTable(any())).thenThrow(new RuntimeException("Exception"));
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ?", true);

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
    void shouldFailWhenTransferFails(VertxTestContext testContext) {
        // arrange
        reset(cartridgeClient);
        when(cartridgeClient.transferDataToScdTable(any())).thenReturn(Future.failedFuture("Failed"));
        DeleteRequest request = getDeleteRequest("DELETE FROM abc WHERE id > ? AND col1 = ?", true);

        // act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }
                }).completeNow());
    }

    private DeleteRequest getDeleteRequestWithoutCondition() {
        val sqlNode = (SqlDelete) DEFINITION_SERVICE.processingQuery("DELETE FROM abc");
        Entity entity = getEntity(true);

        val schema = Datamart.builder()
                .mnemonic("datamart")
                .entities(singletonList(entity))
                .isDefault(true)
                .build();

        return new DeleteRequest(UUID.randomUUID(), "dev", "datamart", entity, sqlNode, 1L, 0L, Collections.singletonList(schema), null, null, null);
    }

    private DeleteRequest getDeleteRequest(String sql, boolean nullable) {
        val sqlNode = (SqlDelete) DEFINITION_SERVICE.processingQuery(sql);
        Entity entity = getEntity(nullable);

        val schema = Datamart.builder()
                .mnemonic("datamart")
                .entities(singletonList(entity))
                .isDefault(true)
                .build();

        return new DeleteRequest(UUID.randomUUID(), "dev", "datamart", entity, sqlNode, 1L, 0L, Collections.singletonList(schema), null, getExtractedParams(), Arrays.asList(SqlTypeName.INTEGER, SqlTypeName.DATE));
    }

    private Entity getEntity(boolean nullable) {
        return Entity.builder()
                .name("abc")
                .entityType(EntityType.TABLE)
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
                                .nullable(nullable)
                                .build()
                ))
                .build();
    }

    private List<SqlNode> getExtractedParams() {
        return Arrays.asList(SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO),
                SqlLiteral.createCharString("2018-01-01", SqlParserPos.ZERO));
    }

}