/*
 * Copyright © 2021 ProStore
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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service.insert;

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
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmDmlQueryExtendService;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service.AdqmQueryGenerator;
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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;
import static ru.datamart.prostore.query.execution.plugin.adqm.utils.TestUtils.DEFINITION_SERVICE;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class InsertSelectToAdqmHandlerTest {
    private static final String CLOSE_VERSIONS_PATTERN = "(?s)INSERT INTO dev__datamart.abc_actual \\(id, col1, col2, col3, col4, sys_from, sys_to, sys_op, sys_close_date, sign\\).*" +
            "SELECT id, col1, col2, col3, col4, sys_from, 0, 0, '\\d+-\\d+-\\d+ \\d+:\\d+:\\d+', arrayJoin\\(\\[-1, 1]\\).*" +
            "ROM dev__datamart.abc_actual.*" +
            "WHERE sys_from < 1 AND sys_to > 1 AND id IN \\(.*" +
            "SELECT id.*" +
            "FROM dev__datamart.abc_actual_shard.*" +
            "WHERE sys_from = 1.*" +
            "\\)";

    @Mock
    private DatabaseExecutor databaseExecutor;
    @Mock
    private DdlProperties ddlProperties;
    @Mock
    private LlrValidationService llrValidationService;

    private InsertSelectToAdqmHandler handler;

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
        val queryParserService = new AdqmCalciteDMLQueryParserService(contextProvider, vertx);
        val pluginSpecificLiteralConverter = new AdqmPluginSpecificLiteralConverter();
        val relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
        val helperTableNamesFactory = new AdqmHelperTableNamesFactory();
        val adqmSchemaExtender = new AdqmSchemaExtender(helperTableNamesFactory);
        val queryExtendService = new AdqmDmlQueryExtendService(helperTableNamesFactory);
        val adqmQueryGenerator = new AdqmQueryGenerator(queryExtendService, sqlDialect, relToSqlConverter);
        val adqmQueryEnrichmentService = new AdqmQueryEnrichmentService(queryParserService, contextProvider, adqmQueryGenerator, adqmSchemaExtender);
        handler = new InsertSelectToAdqmHandler(queryParserService, adqmQueryEnrichmentService, adqmCommonSqlFactory, databaseExecutor, pluginSpecificLiteralConverter, queryTemplateExtractor, llrValidationService);

        lenient().when(databaseExecutor.executeUpdate(any())).thenReturn(Future.succeededFuture());
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
                    verify(databaseExecutor, times(5)).executeUpdate(sqlCaptor.capture());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO dev__datamart.abc_actual (id, col1, col2, col3, col4, sys_from, sys_to, sys_op, sys_close_date, sign)  SELECT * FROM (SELECT id, col1, col2, col3, col4, 1 AS __f5, 9223372036854775807 AS __f6, 0 AS __f7, 9223372036854775807 AS __f8, 1 AS __f9 FROM dev__datamart.src_actual FINAL WHERE sys_from <= 0 AND sys_to >= 0) AS t0 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT id, col1, col2, col3, col4, 1 AS __f5, 9223372036854775807 AS __f6, 0 AS __f7, 9223372036854775807 AS __f8, 1 AS __f9 FROM dev__datamart.src_actual WHERE sys_from <= 0 AND sys_to >= 0) AS t6 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NULL", allValues.get(0));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(1));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(2));
                    assertThat(allValues.get(3), matchesPattern(CLOSE_VERSIONS_PATTERN));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(4));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(5));
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
                    verify(databaseExecutor, times(5)).executeUpdate(sqlCaptor.capture());

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
                    assertEquals("INSERT INTO dev__datamart.abc_actual (id, sys_from, sys_to, sys_op, sys_close_date, sign)  SELECT * FROM (SELECT id, 1 AS __f1, 9223372036854775807 AS __f2, 0 AS __f3, 9223372036854775807 AS __f4, 1 AS __f5 FROM dev__datamart.src_actual FINAL WHERE sys_from <= 0 AND sys_to >= 0 AND (id > 123 AND col4 = ?)) AS t0 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT id, 1 AS __f1, 9223372036854775807 AS __f2, 0 AS __f3, 9223372036854775807 AS __f4, 1 AS __f5 FROM dev__datamart.src_actual WHERE sys_from <= 0 AND sys_to >= 0 AND (id > 123 AND col4 = ?)) AS t6 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NULL", allValues.get(0));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(1));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(2));
                    assertThat(allValues.get(3), matchesPattern(CLOSE_VERSIONS_PATTERN));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(4));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(5));
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
                    verify(databaseExecutor, times(5)).executeUpdate(sqlCaptor.capture());

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
                    assertEquals("INSERT INTO dev__datamart.abc_actual (id, sys_from, sys_to, sys_op, sys_close_date, sign)  SELECT * FROM (SELECT id, 1 AS __f1, 9223372036854775807 AS __f2, 0 AS __f3, 9223372036854775807 AS __f4, 1 AS __f5 FROM dev__datamart.src_actual FINAL WHERE sys_from <= 0 AND sys_to >= 0 AND (id > 123 AND col4 = ?) ORDER BY id NULLS LAST LIMIT ? OFFSET ?) AS t3 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT id, 1 AS __f1, 9223372036854775807 AS __f2, 0 AS __f3, 9223372036854775807 AS __f4, 1 AS __f5 FROM dev__datamart.src_actual WHERE sys_from <= 0 AND sys_to >= 0 AND (id > 123 AND col4 = ?) ORDER BY id NULLS LAST LIMIT ? OFFSET ?) AS t12 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NULL", allValues.get(0));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(1));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(2));
                    assertThat(allValues.get(3), matchesPattern(CLOSE_VERSIONS_PATTERN));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(4));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(5));
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
                    verify(databaseExecutor, times(5)).executeUpdate(sqlCaptor.capture());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO dev__datamart.abc_actual (id, sys_from, sys_to, sys_op, sys_close_date, sign)  SELECT * FROM (SELECT id, 1 AS __f1, 9223372036854775807 AS __f2, 0 AS __f3, 9223372036854775807 AS __f4, 1 AS __f5 FROM dev__datamart.src_actual FINAL WHERE sys_from <= 0 AND sys_to >= 0) AS t0 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT id, 1 AS __f1, 9223372036854775807 AS __f2, 0 AS __f3, 9223372036854775807 AS __f4, 1 AS __f5 FROM dev__datamart.src_actual WHERE sys_from <= 0 AND sys_to >= 0) AS t6 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NULL", allValues.get(0));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(1));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(2));
                    assertThat(allValues.get(3), matchesPattern(CLOSE_VERSIONS_PATTERN));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(4));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(5));
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
                    verify(databaseExecutor, times(5)).executeUpdate(sqlCaptor.capture());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO dev__datamart.abc_actual (id, col1, col2, col3, col4, sys_from, sys_to, sys_op, sys_close_date, sign)  SELECT * FROM (SELECT id, col1, col2, col3, col4, 1 AS __f5, 9223372036854775807 AS __f6, 0 AS __f7, 9223372036854775807 AS __f8, 1 AS __f9 FROM dev__datamart.src_actual FINAL WHERE sys_from <= 0 AND sys_to >= 0) AS t0 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT id, col1, col2, col3, col4, 1 AS __f5, 9223372036854775807 AS __f6, 0 AS __f7, 9223372036854775807 AS __f8, 1 AS __f9 FROM dev__datamart.src_actual WHERE sys_from <= 0 AND sys_to >= 0) AS t6 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NULL", allValues.get(0));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(1));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(2));
                    assertThat(allValues.get(3), matchesPattern(CLOSE_VERSIONS_PATTERN));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(4));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(5));
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
                    verify(databaseExecutor, times(5)).executeUpdate(sqlCaptor.capture());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("INSERT INTO dev__datamart.abc_actual (id, col1, col2, col3, col4, sys_from, sys_to, sys_op, sys_close_date, sign)  SELECT id, col1, col2, col3, col4, __f10, __f11, __f12, __f13, __f14 FROM (SELECT * FROM (SELECT id, col1, col2, col3, col4, sys_op, sys_to, sys_from, sign, sys_close_date, 1 AS __f10, 9223372036854775807 AS __f11, 0 AS __f12, 9223372036854775807 AS __f13, 1 AS __f14 FROM dev__datamart.src_actual FINAL WHERE sys_from <= 0 AND sys_to >= 0) AS t0 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NOT NULL UNION ALL SELECT * FROM (SELECT id, col1, col2, col3, col4, sys_op, sys_to, sys_from, sign, sys_close_date, 1 AS __f10, 9223372036854775807 AS __f11, 0 AS __f12, 9223372036854775807 AS __f13, 1 AS __f14 FROM dev__datamart.src_actual WHERE sys_from <= 0 AND sys_to >= 0) AS t6 WHERE (((SELECT 1 AS r FROM dev__datamart.src_actual WHERE sign < 0 LIMIT 1))) IS NULL) AS t11", allValues.get(0));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(1));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(2));
                    assertThat(allValues.get(3), matchesPattern(CLOSE_VERSIONS_PATTERN));
                    assertEquals("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual", allValues.get(4));
                    assertEquals("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER null FINAL", allValues.get(5));
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

    @Test
    void shouldFailWhenExecuteUpdateFail(VertxTestContext testContext) {
        // arrange
        when(databaseExecutor.executeUpdate(any()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.succeededFuture())
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