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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service.insert;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlInsert;
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
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.converter.AdgPluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.factory.AdgCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.factory.AdgSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.service.AdgCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.service.AdgCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adg.enrichment.service.*;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryExecutorService;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryTemplateExtractor;
import ru.datamart.prostore.query.execution.plugin.adg.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class InsertSelectToAdgHandlerTest {

    @Mock
    private AdgQueryExecutorService queryExecutorService;
    @Mock
    private AdgCartridgeClient cartridgeClient;

    private InsertSelectToAdgHandler handler;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    @Captor
    private ArgumentCaptor<QueryParameters> queryParametersCaptor;

    @BeforeEach
    void setUp(Vertx vertx) {
        val calciteConfiguration = new AdgCalciteConfiguration();
        val sqlParserImplFactory = calciteConfiguration.ddlParserImplFactory();
        val configParser = calciteConfiguration.configDdlParser(sqlParserImplFactory);
        val schemaFactory = new AdgSchemaFactory();
        val calciteSchemaFactory = new AdgCalciteSchemaFactory(schemaFactory);
        val contextProvider = new AdgCalciteContextProvider(configParser, calciteSchemaFactory);
        val queryParserService = new AdgCalciteDMLQueryParserService(contextProvider, vertx);
        val sqlDialect = calciteConfiguration.adgSqlDialect();
        val helperTableNamesFactory = new AdgHelperTableNamesFactory();
        val queryExtendService = new AdgDmlQueryExtendService(helperTableNamesFactory);
        val relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
        val collateReplacer = new AdgCollateValueReplacer();
        val adgQueryGenerator = new AdgQueryGenerator(queryExtendService, sqlDialect, relToSqlConverter, collateReplacer);
        val adgSchemaExtender = new AdgSchemaExtender(helperTableNamesFactory);
        val queryEnrichmentService = new AdgQueryEnrichmentService(contextProvider, adgQueryGenerator, adgSchemaExtender);
        val pluginSpecificLiteralConverter = new AdgPluginSpecificLiteralConverter();
        val queryTemplateExtractor = new AdgQueryTemplateExtractor(TestUtils.DEFINITION_SERVICE, sqlDialect);
        val adgHelperTableNamesFactory = new AdgHelperTableNamesFactory();

        handler = new InsertSelectToAdgHandler(queryParserService, queryEnrichmentService, pluginSpecificLiteralConverter, queryTemplateExtractor, queryExecutorService, cartridgeClient, adgHelperTableNamesFactory, sqlDialect);

        lenient().when(queryExecutorService.executeUpdate(any(), any())).thenReturn(Future.succeededFuture());
        lenient().when(cartridgeClient.transferDataToScdTable(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenWithColumns(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc (id, col1, col2, col3, col4) SELECT id, col1, col2, col3, col4 FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(queryExecutorService).executeUpdate(sqlCaptor.capture(), any());
                    verify(cartridgeClient).transferDataToScdTable(any());

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"col1\",\"col2\",\"col3\",\"col4\",\"sys_op\") SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\", 0 AS \"EXPR__5\" FROM (SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_actual\" WHERE \"sys_from\" <= 0) AS \"t3\"", query);
                    assertEquals(SourceType.ADG, handler.getDestinations());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenPreparedStatementWithTemplateParams(VertxTestContext testContext) {
        // arrange
        val extractedParams = Arrays.asList(SqlNodeTemplates.longLiteral(123L), new SqlDynamicParam(1, SqlParserPos.ZERO));
        val extractedParamsTypes = Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.DYNAMIC_STAR);
        val queryParameters = new QueryParameters(Arrays.asList(true), Arrays.asList(ColumnType.BOOLEAN));
        val request = getInsertRequest("INSERT INTO datamart.abc (id) SELECT id FROM datamart.src WHERE id>? and col4=?",
                queryParameters, extractedParams, extractedParamsTypes);

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(queryExecutorService).executeUpdate(sqlCaptor.capture(), queryParametersCaptor.capture());
                    verify(cartridgeClient).transferDataToScdTable(any());

                    QueryParameters parameters = queryParametersCaptor.getValue();
                    assertThat(parameters, allOf(
                            hasProperty("values", contains(
                                    is(true)
                            )),
                            hasProperty("types", contains(
                                    is(ColumnType.BOOLEAN)
                            ))
                    ));

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"sys_op\") SELECT \"id\", 0 AS \"EXPR__1\" FROM (SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_actual\" WHERE \"sys_from\" <= 0) AS \"t3\" WHERE \"id\" > 123 AND \"col4\" = ?", query);
                }).completeNow());
    }

    @Test
    void shouldSuccessWithOffsetLimitParams(VertxTestContext testContext) {
        // arrange
        val extractedParams = Arrays.asList(SqlNodeTemplates.longLiteral(123L), new SqlDynamicParam(1, SqlParserPos.ZERO), new SqlDynamicParam(2, SqlParserPos.ZERO), new SqlDynamicParam(3, SqlParserPos.ZERO));
        val extractedParamsTypes = Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.DYNAMIC_STAR, SqlTypeName.DYNAMIC_STAR, SqlTypeName.DYNAMIC_STAR);
        val queryParameters = new QueryParameters(Arrays.asList(true, 1L, 2L), Arrays.asList(ColumnType.BOOLEAN, ColumnType.BIGINT, ColumnType.BIGINT));
        val request = getInsertRequest("INSERT INTO datamart.abc (id) SELECT id FROM datamart.src WHERE id>? and col4=? ORDER BY id LIMIT ? OFFSET ?",
                queryParameters, extractedParams, extractedParamsTypes);

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }
                    verify(queryExecutorService).executeUpdate(sqlCaptor.capture(), queryParametersCaptor.capture());
                    verify(cartridgeClient).transferDataToScdTable(any());

                    QueryParameters parameters = queryParametersCaptor.getValue();
                    assertThat(parameters, allOf(
                            hasProperty("values", contains(
                                    is(true), is(1L), is(2L)
                            )),
                            hasProperty("types", contains(
                                    is(ColumnType.BOOLEAN), is(ColumnType.BIGINT), is(ColumnType.BIGINT)
                            ))
                    ));


                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"sys_op\") SELECT \"id\", 0 AS \"EXPR__1\" FROM (SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_actual\" WHERE \"sys_from\" <= 0) AS \"t3\" WHERE \"id\" > 123 AND \"col4\" = ? ORDER BY \"id\" LIMIT ? OFFSET ?", query);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenOnlyPkColumn(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc (id) SELECT id FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(queryExecutorService).executeUpdate(sqlCaptor.capture(), any());
                    verify(cartridgeClient).transferDataToScdTable(any());

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"sys_op\") SELECT \"id\", 0 AS \"EXPR__1\" FROM (SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_actual\" WHERE \"sys_from\" <= 0) AS \"t3\"", query);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenWithoutColumns(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc SELECT id, col1, col2, col3, col4 FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(queryExecutorService).executeUpdate(sqlCaptor.capture(), any());
                    verify(cartridgeClient).transferDataToScdTable(any());

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"col1\",\"col2\",\"col3\",\"col4\",\"sys_op\") SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\", 0 AS \"EXPR__5\" FROM (SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_actual\" WHERE \"sys_from\" <= 0) AS \"t3\"", query);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenStarQuery(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc SELECT * FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(queryExecutorService).executeUpdate(sqlCaptor.capture(), any());
                    verify(cartridgeClient).transferDataToScdTable(any());

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"dev__datamart__abc_staging\" (\"id\",\"col1\",\"col2\",\"col3\",\"col4\",\"sys_op\") SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\", 0 AS \"EXPR__5\" FROM (SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_history\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0 UNION ALL SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\" FROM \"dev__datamart__src_actual\" WHERE \"sys_from\" <= 0) AS \"t3\"", query);
                }).completeNow());
    }

    @Test
    void shouldFailWhenExecuteWithParamsFail(VertxTestContext testContext) {
        // arrange
        when(queryExecutorService.executeUpdate(any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        val request = getInsertRequest("INSERT INTO datamart.abc (id) SELECT id FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Exception", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenTransferFail(VertxTestContext testContext) {
        // arrange
        when(cartridgeClient.transferDataToScdTable(any()))
                .thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        val request = getInsertRequest("INSERT INTO datamart.abc (id) SELECT id FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Exception", ar.cause().getMessage());
                }).completeNow());
    }

    private InsertSelectRequest getInsertRequest(String sql) {
        return getInsertRequest(sql, null, Collections.emptyList(), Collections.emptyList());
    }

    private InsertSelectRequest getInsertRequest(String sql, QueryParameters queryParameters, List<SqlNode> extractedParams, List<SqlTypeName> extractedParamsTypes) {
        SqlInsert sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        SqlNode source = sqlNode.getSource();
        Entity entity = prepareEntity("abc");
        Entity entity2 = prepareEntity("src");
        Datamart datamart = Datamart.builder()
                .mnemonic("datamart")
                .entities(Arrays.asList(entity, entity2))
                .isDefault(true)
                .build();
        List<Datamart> datamarts = Arrays.asList(datamart);
        List<DeltaInformation> deltaInformations = Arrays.asList(DeltaInformation.builder()
                .selectOnNum(0L)
                .tableName("src")
                .schemaName("datamart")
                .type(DeltaType.WITHOUT_SNAPSHOT)
                .build());
        return new InsertSelectRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, queryParameters, datamarts, deltaInformations, source, null, extractedParams, extractedParamsTypes);
    }

    private Entity prepareEntity(String name) {
        return Entity.builder()
                .name(name)
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.DATE)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.TIME)
                                .size(0)
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
                                .size(0)
                                .build(),
                        EntityField.builder()
                                .name("col4")
                                .ordinalPosition(4)
                                .type(ColumnType.BOOLEAN)
                                .build()
                ))
                .build();
    }
}