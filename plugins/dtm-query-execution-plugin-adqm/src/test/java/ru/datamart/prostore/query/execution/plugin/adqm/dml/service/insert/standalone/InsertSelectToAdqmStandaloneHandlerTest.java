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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service.insert.standalone;

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
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adqm.base.factory.AdqmHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.base.service.converter.AdqmPluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.factory.AdqmCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.factory.AdqmSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.service.AdqmCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.service.AdqmCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.dml.service.AdqmConstantReplacer;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmDmlQueryExtendService;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmSchemaExtender;
import ru.datamart.prostore.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.AdqmQueryTemplateExtractor;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.LlrValidationService;

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
import static ru.datamart.prostore.query.execution.plugin.adqm.utils.TestUtils.DEFINITION_SERVICE;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class InsertSelectToAdqmStandaloneHandlerTest {

    @Mock
    private DatabaseExecutor databaseExecutor;
    @Mock
    private DdlProperties ddlProperties;
    @Mock
    private LlrValidationService llrValidationService;

    private InsertSelectToAdqmStandaloneHandler handler;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    @Captor
    private ArgumentCaptor<QueryParameters> queryParametersCaptor;

    @BeforeEach
    void setUp(Vertx vertx) {
        val calciteConfiguration = new CalciteConfiguration();
        val sqlDialect = calciteConfiguration.adqmSqlDialect();
        val queryTemplateExtractor = new AdqmQueryTemplateExtractor(DEFINITION_SERVICE, sqlDialect);
        val adqmCommonSqlFactory = new AdqmProcessingSqlFactory(ddlProperties, sqlDialect);
        val factory = calciteConfiguration.ddlParserImplFactory();
        val configParser = calciteConfiguration.configDdlParser(factory);
        val schemaFactory = new AdqmSchemaFactory();
        val calciteSchemaFactory = new AdqmCalciteSchemaFactory(schemaFactory);
        val contextProvider = new AdqmCalciteContextProvider(configParser, calciteSchemaFactory);
        val pluginSpecificLiteralConverter = new AdqmPluginSpecificLiteralConverter();
        val relToSqlConverter = new DtmRelToSqlConverter(sqlDialect, false);
        val helperTableNamesFactory = new AdqmHelperTableNamesFactory();
        val adqmSchemaExtender = new AdqmSchemaExtender(helperTableNamesFactory);
        val queryParserService = new AdqmCalciteDMLQueryParserService(contextProvider, vertx, adqmSchemaExtender);
        val queryExtendService = new AdqmDmlQueryExtendService(helperTableNamesFactory);
        val adqmQueryEnrichmentService = new AdqmQueryEnrichmentService(queryExtendService, sqlDialect, relToSqlConverter);
        val constantConverter = new AdqmConstantReplacer(pluginSpecificLiteralConverter);

        handler = new InsertSelectToAdqmStandaloneHandler(queryParserService, adqmQueryEnrichmentService, adqmCommonSqlFactory, databaseExecutor, pluginSpecificLiteralConverter, queryTemplateExtractor, llrValidationService, constantConverter);
        lenient().when(databaseExecutor.executeWithParams(any(), any(), any())).thenReturn(Future.succeededFuture());
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

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO standalone.abc (id, col1, col2, col3, col4)  SELECT * FROM standalone.src", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenWithColumnsWithJoinWithLogical(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc (id, col1, col2, col3, col4) " +
                        "SELECT src.id, src.col1, src.col2, logical.col3, logical.col4 FROM datamart.src JOIN datamart.logical ON src.id = logical.id",
                null, Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(), DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build())
        );

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO standalone.abc (id, col1, col2, col3, col4)  SELECT id, col1, col2, col30, col40 FROM (SELECT src.id, src.col1, src.col2, logical_actual_shard.col3 AS col30, logical_actual_shard.col4 AS col40 FROM standalone.src INNER JOIN dev__datamart.logical_actual_shard FINAL ON src.id = logical_actual_shard.id WHERE logical_actual_shard.sys_from <= 1 AND logical_actual_shard.sys_to >= 1) AS t1 WHERE (((SELECT 1 AS r FROM dev__datamart.logical_actual_shard WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT id, col1, col2, col30, col40 FROM (SELECT src0.id, src0.col1, src0.col2, logical_actual_shard1.col3 AS col30, logical_actual_shard1.col4 AS col40 FROM standalone.src AS src0 INNER JOIN dev__datamart.logical_actual_shard AS logical_actual_shard1 ON src0.id = logical_actual_shard1.id WHERE logical_actual_shard1.sys_from <= 1 AND logical_actual_shard1.sys_to >= 1) AS t10 WHERE (((SELECT 1 AS r FROM dev__datamart.logical_actual_shard WHERE sign < 0 LIMIT 1))) IS NULL", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenWithColumnsWithJoinWithLeftLogical(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc (id, col1, col2, col3, col4) " +
                        "SELECT src.id, src.col1, src.col2, logical.col3, logical.col4 FROM datamart.logical JOIN datamart.src ON src.id = logical.id",
                null, Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).build(), DeltaInformation.builder().type(DeltaType.WITHOUT_SNAPSHOT).selectOnNum(1L).build())
        );

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO standalone.abc (id, col1, col2, col3, col4)  SELECT id0, col10, col20, col3, col4 FROM (SELECT src_shard.id AS id0, src_shard.col1 AS col10, src_shard.col2 AS col20, logical_actual.col3, logical_actual.col4 FROM dev__datamart.logical_actual FINAL INNER JOIN standalone.src_shard ON logical_actual.id = src_shard.id WHERE logical_actual.sys_from <= NULL AND logical_actual.sys_to >= NULL) AS t1 WHERE (((SELECT 1 AS r FROM dev__datamart.logical_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT id0, col10, col20, col3, col4 FROM (SELECT src_shard0.id AS id0, src_shard0.col1 AS col10, src_shard0.col2 AS col20, logical_actual1.col3, logical_actual1.col4 FROM dev__datamart.logical_actual AS logical_actual1 INNER JOIN standalone.src_shard AS src_shard0 ON logical_actual1.id = src_shard0.id WHERE logical_actual1.sys_from <= NULL AND logical_actual1.sys_to >= NULL) AS t10 WHERE (((SELECT 1 AS r FROM dev__datamart.logical_actual WHERE sign < 0 LIMIT 1))) IS NULL", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWhenQueryValidationFailed(VertxTestContext testContext) {
        // arrange
        doThrow(new DtmException("Exception")).when(llrValidationService).validate(any());
        val request = getInsertRequest("INSERT INTO datamart.abc (id, col1, col2, col3, col4) SELECT id, col1, col2, col3, col4 FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Exception", ar.cause().getMessage());
                    verifyNoInteractions(databaseExecutor);
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

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), queryParametersCaptor.capture(), any());

                    QueryParameters parameters = queryParametersCaptor.getValue();
                    assertThat(parameters, allOf(
                            hasProperty("values", contains(
                                    is(true), is(true)
                            )),
                            hasProperty("types", contains(
                                    is(ColumnType.BOOLEAN), is(ColumnType.BOOLEAN)
                            ))
                    ));

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO standalone.abc (id)  SELECT id FROM standalone.src WHERE id > 123 AND col4 = ?", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
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

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), queryParametersCaptor.capture(), any());

                    QueryParameters parameters = queryParametersCaptor.getValue();
                    assertThat(parameters, allOf(
                            hasProperty("values", contains(
                                    is(true), is(1L), is(2L), is(true), is(1L), is(2L)
                            )),
                            hasProperty("types", contains(
                                    is(ColumnType.BOOLEAN), is(ColumnType.BIGINT), is(ColumnType.BIGINT), is(ColumnType.BOOLEAN), is(ColumnType.BIGINT), is(ColumnType.BIGINT)
                            ))
                    ));

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO standalone.abc (id)  SELECT id FROM standalone.src WHERE id > 123 AND col4 = ? ORDER BY id NULLS LAST LIMIT ? OFFSET ?", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
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

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO standalone.abc (id)  SELECT id FROM standalone.src", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
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

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO standalone.abc (id, col1, col2, col3, col4)  SELECT * FROM standalone.src", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
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

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO standalone.abc (id, col1, col2, col3, col4)  SELECT * FROM standalone.src", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWhenNoPkColumns(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc (col1) SELECT col1 FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.succeeded()) {
                        fail("Unexpected success");
                    }

                    assertEquals("Inserted values must contain primary keys: [id]", ar.cause().getMessage());
                    verifyNoInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenConstantsValues(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc  SELECT 1, '2020-02-01', '03:12:34', '2020-02-01 10:11:12.0', true FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO standalone.abc (id, col1, col2, col3, col4)  SELECT 1 AS __f0, 18293 AS __f1, 11554000000 AS __f2, 1580551872000000 AS __f3, 1 AS __f4 FROM standalone.src", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenConstantsValuesWithIdentifiers(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc  SELECT id, '2020-02-01', '03:12:34', '2020-02-01 10:11:12.0', col4 FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO standalone.abc (id, col1, col2, col3, col4)  SELECT id, 18293 AS __f1, 11554000000 AS __f2, 1580551872000000 AS __f3, col4 FROM standalone.src", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenConstantsValuesWithStar(VertxTestContext testContext) {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc  SELECT id, '2020-02-01', '03:12:34', '2020-02-01 10:11:12.0', col4, * FROM datamart.src");

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO standalone.abc (id, col1, col2, col3, col4)  SELECT id, '2020-02-01' AS __f1, '03:12:34' AS __f2, '2020-02-01 10:11:12.0' AS __f3, col4, id AS id0, col1, col2, col3, col4 AS col40 FROM standalone.src", allValues.get(0));
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWhenExecuteWithParamsFail(VertxTestContext testContext) {
        // arrange
        when(databaseExecutor.executeWithParams(any(), any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

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
        val deltaInformations = Arrays.asList(DeltaInformation.builder()
                .selectOnNum(0L)
                .tableName("src")
                .schemaName("datamart")
                .type(DeltaType.WITHOUT_SNAPSHOT)
                .build());
        return getInsertRequest(sql, queryParameters, extractedParams, extractedParamsTypes, deltaInformations);
    }

    private InsertSelectRequest getInsertRequest(String sql, QueryParameters queryParameters, List<SqlNode> extractedParams, List<SqlTypeName> extractedParamsTypes, List<DeltaInformation> deltaInformations) {
        SqlInsert sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        SqlNode source = sqlNode.getSource();
        Entity entity = prepareEntity("abc");
        Entity entity2 = prepareEntity("src");
        Entity entity3 = prepareLogicalEntity("logical");
        Datamart datamart = Datamart.builder()
                .mnemonic("datamart")
                .entities(Arrays.asList(entity, entity2, entity3))
                .build();
        List<Datamart> datamarts = Arrays.asList(datamart);
        return new InsertSelectRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, queryParameters, datamarts, deltaInformations, source, null, extractedParams, extractedParamsTypes);
    }

    private Entity prepareEntity(String name) {
        return Entity.builder()
                .name(name)
                .entityType(EntityType.READABLE_EXTERNAL_TABLE)
                .externalTableLocationPath("standalone." + name)
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
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
                                .build(),
                        EntityField.builder()
                                .name("col4")
                                .ordinalPosition(4)
                                .type(ColumnType.BOOLEAN)
                                .build()
                ))
                .build();
    }

    private Entity prepareLogicalEntity(String name) {
        return Entity.builder()
                .name(name)
                .entityType(EntityType.TABLE)
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
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
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