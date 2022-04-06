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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service.insert.select;

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
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.converter.AdgPluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.factory.AdgCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.factory.AdgSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.service.AdgCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.service.AdgCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adg.dml.service.AdgConstantReplacer;
import ru.datamart.prostore.query.execution.plugin.adg.enrichment.service.AdgCollateValueReplacer;
import ru.datamart.prostore.query.execution.plugin.adg.enrichment.service.AdgDmlQueryExtendService;
import ru.datamart.prostore.query.execution.plugin.adg.enrichment.service.AdgQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adg.enrichment.service.AdgSchemaExtender;
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
class InsertSelectToAdgStandaloneHandlerTest {

    @Mock
    private AdgQueryExecutorService queryExecutorService;

    private InsertSelectToAdgStandaloneHandler handler;

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
        val helperTableNamesFactory = new AdgHelperTableNamesFactory();
        val adgSchemaExtender = new AdgSchemaExtender(helperTableNamesFactory);
        val queryParserService = new AdgCalciteDMLQueryParserService(contextProvider, vertx, adgSchemaExtender);
        val sqlDialect = calciteConfiguration.adgSqlDialect();
        val queryExtendService = new AdgDmlQueryExtendService(helperTableNamesFactory);
        val relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
        val collateReplacer = new AdgCollateValueReplacer();
        val queryEnrichmentService = new AdgQueryEnrichmentService(queryExtendService, sqlDialect, relToSqlConverter, collateReplacer);
        val pluginSpecificLiteralConverter = new AdgPluginSpecificLiteralConverter();
        val queryTemplateExtractor = new AdgQueryTemplateExtractor(TestUtils.DEFINITION_SERVICE, sqlDialect);
        val dateConstantConverter = new AdgConstantReplacer(pluginSpecificLiteralConverter);

        handler = new InsertSelectToAdgStandaloneHandler(queryParserService, queryEnrichmentService, pluginSpecificLiteralConverter, queryTemplateExtractor, queryExecutorService, sqlDialect, dateConstantConverter);

        lenient().when(queryExecutorService.executeUpdate(any(), any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenWithColumns(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc (id, col1, col2, col3, col4, col5) SELECT id, col1, col2, col3, col4, col5 FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(queryExecutorService).executeUpdate(sqlCaptor.capture(), any());

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"db_table\" (\"id\",\"col1\",\"col2\",\"col3\",\"col4\",\"col5\") SELECT * FROM \"db_table\"", query);
                    assertEquals(SourceType.ADG, handler.getDestinations());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenPreparedStatementWithTemplateParams(VertxTestContext testContext) {
        // arrange
        val extractedParams = Arrays.asList(SqlNodeTemplates.longLiteral(123L), new SqlDynamicParam(1, SqlParserPos.ZERO));
        val extractedParamsTypes = Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.DYNAMIC_STAR);
        val queryParameters = new QueryParameters(Collections.singletonList(true), Collections.singletonList(ColumnType.BOOLEAN));
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
                    assertEquals("INSERT INTO \"db_table\" (\"id\") SELECT \"id\" FROM \"db_table\" WHERE \"id\" > 123 AND \"col4\" = ?", query);
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
                    assertEquals("INSERT INTO \"db_table\" (\"id\") SELECT \"id\" FROM \"db_table\" WHERE \"id\" > 123 AND \"col4\" = ? ORDER BY \"id\" LIMIT ? OFFSET ?", query);
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

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"db_table\" (\"id\") SELECT \"id\" FROM \"db_table\"", query);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenWithoutColumns(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc SELECT id, col1, col2, col3, col4, col5 FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(queryExecutorService).executeUpdate(sqlCaptor.capture(), any());

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"db_table\" (\"id\",\"col1\",\"col2\",\"col3\",\"col4\",\"col5\") SELECT * FROM \"db_table\"", query);
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

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"db_table\" (\"id\",\"col1\",\"col2\",\"col3\",\"col4\",\"col5\") SELECT * FROM \"db_table\"", query);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenConstantsValues(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc SELECT 1, '2020-02-01', '03:12:34', '2020-02-01 10:11:12.0', false, 'txt' FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(queryExecutorService).executeUpdate(sqlCaptor.capture(), any());

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"db_table\" (\"id\",\"col1\",\"col2\",\"col3\",\"col4\",\"col5\") SELECT 1 AS \"EXPR__0\", 18293 AS \"EXPR__1\", 11554000000 AS \"EXPR__2\", 1580551872000000 AS \"EXPR__3\", FALSE AS \"EXPR__4\", 'txt' AS \"EXPR__5\" FROM \"db_table\"", query);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenConstantsValuesWithIdentifiers(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc SELECT id, '2020-02-01', '03:12:34', '2020-02-01 10:11:12.0', col4, 'txt' FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(queryExecutorService).executeUpdate(sqlCaptor.capture(), any());

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"db_table\" (\"id\",\"col1\",\"col2\",\"col3\",\"col4\",\"col5\") SELECT \"id\", 18293 AS \"EXPR__1\", 11554000000 AS \"EXPR__2\", 1580551872000000 AS \"EXPR__3\", \"col4\", 'txt' AS \"EXPR__5\" FROM \"db_table\"", query);
                }).completeNow());
    }


    @Test
    void shouldSuccessWhenConstantsValuesWithStar(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc SELECT id, '2020-02-01', '03:12:34', '2020-02-01 10:11:12.0', col4, 'txt', * FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(queryExecutorService).executeUpdate(sqlCaptor.capture(), any());

                    String query = sqlCaptor.getValue();
                    assertEquals("INSERT INTO \"db_table\" (\"id\",\"col1\",\"col2\",\"col3\",\"col4\",\"col5\") SELECT \"id\", '2020-02-01' AS \"EXPR__1\", '03:12:34' AS \"EXPR__2\", '2020-02-01 10:11:12.0' AS \"EXPR__3\", \"col4\", 'txt' AS \"EXPR__5\", \"id\" AS \"id0\", \"col1\", \"col2\", \"col3\", \"col4\" AS \"col40\", \"col5\" FROM \"db_table\"", query);
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

    private InsertSelectRequest getInsertRequest(String sql) {
        return getInsertRequest(sql, null, Collections.emptyList(), Collections.emptyList());
    }

    private InsertSelectRequest getInsertRequest(String sql, QueryParameters queryParameters, List<SqlNode> extractedParams, List<SqlTypeName> extractedParamsTypes) {
        SqlInsert sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        SqlNode source = sqlNode.getSource();
        Entity entity = prepareEntity("table");
        Entity entity2 = prepareEntity("src");
        Datamart datamart = Datamart.builder()
                .mnemonic("datamart")
                .entities(Arrays.asList(entity, entity2))
                .isDefault(true)
                .build();
        List<Datamart> datamarts = Collections.singletonList(datamart);
        List<DeltaInformation> deltaInformations = Collections.singletonList(DeltaInformation.builder()
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
                .externalTableLocationPath("db_table")
                .entityType(EntityType.READABLE_EXTERNAL_TABLE)
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
                                .build(),
                        EntityField.builder()
                                .name("col5")
                                .ordinalPosition(5)
                                .type(ColumnType.VARCHAR)
                                .size(10)
                                .build()
                ))
                .build();
    }
}