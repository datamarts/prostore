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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert.select.logical;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
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
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.adg.AdgConnectorSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice.AdgColumnsCastService;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.factory.AdbSchemaFactory;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.service.AdbCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbDmlQueryExtendWithoutHistoryService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbQueryEnrichmentService;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbSchemaExtender;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.impl.AdbQueryTemplateExtractor;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adg.AdgSharedService;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedProperties;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static ru.datamart.prostore.query.execution.plugin.adb.utils.TestUtils.DEFINITION_SERVICE;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdgLogicalInsertSelectHandlerTest {

    private static final String TARANTOOL_SERVER = "tarantool_server";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final long CONNECT_TIMEOUT = 1234L;
    private static final long READ_TIMEOUT = 2345L;
    private static final long REQUEST_TIMEOUT = 3456L;
    private static final int BUFFER_SIZE = 4567;

    @Mock
    private DatabaseExecutor databaseExecutor;

    @Mock
    private AdgSharedService adgSharedService;

    private AdgLogicalInsertSelectHandler handler;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    @BeforeEach
    void setUp(Vertx vertx) {
        val calciteConfiguration = new CalciteConfiguration();
        val sqlDialect = calciteConfiguration.adbSqlDialect();
        val templateExtractor = new AdbQueryTemplateExtractor(DEFINITION_SERVICE, sqlDialect);
        val factory = calciteConfiguration.ddlParserImplFactory();
        val configParser = calciteConfiguration.configDdlParser(factory);
        val schemaFactory = new AdbSchemaFactory();
        val calciteSchemaFactory = new AdbCalciteSchemaFactory(schemaFactory);
        val contextProvider = new AdbCalciteContextProvider(configParser, calciteSchemaFactory);
        val schemaExtender = new AdbSchemaExtender();
        val parserService = new AdbCalciteDMLQueryParserService(contextProvider, vertx, schemaExtender);
        val columnsCastService = new AdgColumnsCastService(sqlDialect);
        val queryExtendService = new AdbDmlQueryExtendWithoutHistoryService();
        val relToSqlConverter = new DtmRelToSqlConverter(sqlDialect);
        val enrichmentService = new AdbQueryEnrichmentService(queryExtendService, sqlDialect, relToSqlConverter);

        lenient().when(adgSharedService.getSharedProperties()).thenReturn(new AdgSharedProperties(TARANTOOL_SERVER, USER, PASSWORD, CONNECT_TIMEOUT, READ_TIMEOUT, REQUEST_TIMEOUT, BUFFER_SIZE));
        val sqlFactory = new AdgConnectorSqlFactory(adgSharedService);
        handler = new AdgLogicalInsertSelectHandler(sqlFactory, databaseExecutor, adgSharedService, parserService, columnsCastService, enrichmentService, templateExtractor, sqlDialect);

        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
        when(databaseExecutor.executeWithParams(anyString(), any(), anyList())).thenReturn(Future.succeededFuture());
        when(adgSharedService.prepareStaging(any())).thenReturn(Future.succeededFuture());
        lenient().when(adgSharedService.transferData(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenWithoutCondition(VertxTestContext testContext) {
        val request = getInsertRequest("INSERT INTO datamart.abc (id, col1, col2, col3, col4) SELECT id, col1, col2, col3, col4 FROM datamart.src");

        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor, times(3)).executeUpdate(sqlCaptor.capture());
                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("DROP EXTERNAL TABLE IF EXISTS datamart.TARANTOOL_EXT_abc", allValues.get(0));
                    assertEquals("CREATE WRITABLE EXTERNAL TABLE datamart.TARANTOOL_EXT_abc\n" +
                            "(id int8,col1 int8,col2 int8,col3 int8,col4 bool,sys_op int8,bucket_id int8) LOCATION ('pxf://dev__datamart__abc_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool_server&USER=user&PASSWORD=password&TIMEOUT_CONNECT=1234&TIMEOUT_READ=2345&TIMEOUT_REQUEST=3456&BUFFER_SIZE=4567')\n" +
                            "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')", allValues.get(1));
                    assertEquals("DROP EXTERNAL TABLE IF EXISTS datamart.TARANTOOL_EXT_abc", allValues.get(2));
                    assertEquals("INSERT INTO datamart.TARANTOOL_EXT_abc (id, col1, col2, col3, col4, sys_op, bucket_id) (SELECT id, CAST(EXTRACT(EPOCH FROM col1) / 86400 AS BIGINT), CAST(EXTRACT(EPOCH FROM col2) * 1000000 AS BIGINT), CAST(EXTRACT(EPOCH FROM col3) * 1000000 AS BIGINT), col4, 0 AS sys_op, NULL AS bucket_id FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0)", allValues.get(3));
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenConstants(VertxTestContext testContext) {
        val request = getInsertRequest("INSERT INTO datamart.abc (id, col1, col2, col3, col4) " +
                "SELECT id, '2020-01-01', '11:11:11', '2020-01-01 11:11:11', true FROM datamart.src");

        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor, times(3)).executeUpdate(sqlCaptor.capture());
                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("DROP EXTERNAL TABLE IF EXISTS datamart.TARANTOOL_EXT_abc", allValues.get(0));
                    assertEquals("CREATE WRITABLE EXTERNAL TABLE datamart.TARANTOOL_EXT_abc\n" +
                            "(id int8,col1 int8,col2 int8,col3 int8,col4 bool,sys_op int8,bucket_id int8) LOCATION ('pxf://dev__datamart__abc_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool_server&USER=user&PASSWORD=password&TIMEOUT_CONNECT=1234&TIMEOUT_READ=2345&TIMEOUT_REQUEST=3456&BUFFER_SIZE=4567')\n" +
                            "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')", allValues.get(1));
                    assertEquals("DROP EXTERNAL TABLE IF EXISTS datamart.TARANTOOL_EXT_abc", allValues.get(2));
                    assertEquals("INSERT INTO datamart.TARANTOOL_EXT_abc (id, col1, col2, col3, col4, sys_op, bucket_id) (SELECT id, CAST(EXTRACT(EPOCH FROM CAST('2020-01-01' AS DATE)) / 86400 AS BIGINT) AS EXPR__1, CAST(EXTRACT(EPOCH FROM CAST('11:11:11' AS TIME)) * 1000000 AS BIGINT) AS EXPR__2, CAST(EXTRACT(EPOCH FROM CAST('2020-01-01 11:11:11' AS TIMESTAMP)) * 1000000 AS BIGINT) AS EXPR__3, TRUE AS EXPR__4, 0 AS sys_op, NULL AS bucket_id FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0)", allValues.get(3));
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenNullConstants(VertxTestContext testContext) {
        val request = getInsertRequest("INSERT INTO datamart.abc (id, col1, col2, col3, col4) " +
                "SELECT id, null, null, null, null FROM datamart.src");

        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor, times(3)).executeUpdate(sqlCaptor.capture());
                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("DROP EXTERNAL TABLE IF EXISTS datamart.TARANTOOL_EXT_abc", allValues.get(0));
                    assertEquals("CREATE WRITABLE EXTERNAL TABLE datamart.TARANTOOL_EXT_abc\n" +
                            "(id int8,col1 int8,col2 int8,col3 int8,col4 bool,sys_op int8,bucket_id int8) LOCATION ('pxf://dev__datamart__abc_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool_server&USER=user&PASSWORD=password&TIMEOUT_CONNECT=1234&TIMEOUT_READ=2345&TIMEOUT_REQUEST=3456&BUFFER_SIZE=4567')\n" +
                            "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')", allValues.get(1));
                    assertEquals("DROP EXTERNAL TABLE IF EXISTS datamart.TARANTOOL_EXT_abc", allValues.get(2));
                    assertEquals("INSERT INTO datamart.TARANTOOL_EXT_abc (id, col1, col2, col3, col4, sys_op, bucket_id) (SELECT id, CAST(EXTRACT(EPOCH FROM CAST(NULL AS DATE)) / 86400 AS BIGINT) AS EXPR__1, CAST(EXTRACT(EPOCH FROM CAST(NULL AS TIME)) * 1000000 AS BIGINT) AS EXPR__2, CAST(EXTRACT(EPOCH FROM CAST(NULL AS TIMESTAMP)) * 1000000 AS BIGINT) AS EXPR__3, NULL AS EXPR__4, 0 AS sys_op, NULL AS bucket_id FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0)", allValues.get(3));
                }).completeNow());
    }

    @Test
    void shouldSuccessAndNotCopyOffsetLimitParams(VertxTestContext testContext) {
        // arrange
        val extractedParams = Arrays.asList((SqlNode) SqlNodeTemplates.longLiteral(123L), SqlLiteral.createBoolean(true, SqlParserPos.ZERO), SqlNodeTemplates.longLiteral(1L), SqlNodeTemplates.longLiteral(2L));
        val extractedParamsTypes = Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.BOOLEAN, SqlTypeName.BIGINT, SqlTypeName.BIGINT);
        val request = getInsertRequest("INSERT INTO datamart.abc (id) SELECT id FROM datamart.src WHERE id>? and col4=? ORDER BY id LIMIT ? OFFSET ?",
                null, extractedParams, extractedParamsTypes);

        // act
        handler.handle(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (!ar.succeeded()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor, times(3)).executeUpdate(sqlCaptor.capture());
                    verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());

                    List<String> allValues = sqlCaptor.getAllValues();
                    assertEquals("DROP EXTERNAL TABLE IF EXISTS datamart.TARANTOOL_EXT_abc", allValues.get(0));
                    assertEquals("CREATE WRITABLE EXTERNAL TABLE datamart.TARANTOOL_EXT_abc\n" +
                            "(id int8,col1 int8,col2 int8,col3 int8,col4 bool,sys_op int8,bucket_id int8) LOCATION ('pxf://dev__datamart__abc_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool_server&USER=user&PASSWORD=password&TIMEOUT_CONNECT=1234&TIMEOUT_READ=2345&TIMEOUT_REQUEST=3456&BUFFER_SIZE=4567')\n" +
                            "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')", allValues.get(1));
                    assertEquals("DROP EXTERNAL TABLE IF EXISTS datamart.TARANTOOL_EXT_abc", allValues.get(2));
                    assertEquals("INSERT INTO datamart.TARANTOOL_EXT_abc (id, sys_op, bucket_id) (SELECT id, 0 AS sys_op, NULL AS bucket_id FROM (SELECT id, col1, col2, col3, col4 FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0) AS t0 WHERE id > 123 AND col4 = TRUE ORDER BY id LIMIT 1 OFFSET 2)", allValues.get(3));
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
                    assertEquals("INSERT INTO datamart.TARANTOOL_EXT_abc (id, sys_op, bucket_id) (SELECT id, 0 AS sys_op, NULL AS bucket_id FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0)", sqlCaptor.getValue());
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
                    assertEquals("INSERT INTO datamart.TARANTOOL_EXT_abc (id, col1, col2, col3, col4, sys_op, bucket_id) (SELECT id, CAST(EXTRACT(EPOCH FROM col1) / 86400 AS BIGINT), CAST(EXTRACT(EPOCH FROM col2) * 1000000 AS BIGINT), CAST(EXTRACT(EPOCH FROM col3) * 1000000 AS BIGINT), col4, 0 AS sys_op, NULL AS bucket_id FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0)", sqlCaptor.getValue());
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
                    assertEquals("INSERT INTO datamart.TARANTOOL_EXT_abc (id, col1, col2, col3, col4, sys_op, bucket_id) (SELECT id, CAST(EXTRACT(EPOCH FROM col1) / 86400 AS BIGINT), CAST(EXTRACT(EPOCH FROM col2) * 1000000 AS BIGINT), CAST(EXTRACT(EPOCH FROM col3) * 1000000 AS BIGINT), col4, 0 AS sys_op, NULL AS bucket_id FROM datamart.src_actual WHERE sys_from <= 0 AND COALESCE(sys_to, 9223372036854775807) >= 0)", sqlCaptor.getValue());
                    verifyNoMoreInteractions(databaseExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWhenExecuteWithParamsFail(VertxTestContext testContext) {
        // arrange
        reset(databaseExecutor);
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
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
        SqlInsert sqlNode = (SqlInsert) DEFINITION_SERVICE.processingQuery(sql);
        SqlNode source = sqlNode.getSource();
        Entity entity = prepareEntity("abc");
        Entity entity2 = prepareEntity("src");
        Datamart datamart = Datamart.builder()
                .mnemonic("datamart")
                .isDefault(true)
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
                .schema("datamart")
                .name(name)
                .entityType(EntityType.TABLE)
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.DATE)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.TIME)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col4")
                                .ordinalPosition(4)
                                .type(ColumnType.BOOLEAN)
                                .nullable(true)
                                .build()
                ))
                .build();
    }
}
