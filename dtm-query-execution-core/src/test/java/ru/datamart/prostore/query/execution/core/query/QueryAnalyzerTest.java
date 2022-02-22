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
package ru.datamart.prostore.query.execution.core.query;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.InputQueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.dml.DmlType;
import ru.datamart.prostore.query.execution.core.base.dto.request.CoreRequestContext;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationExtractor;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.query.factory.QueryRequestFactory;
import ru.datamart.prostore.query.execution.core.query.factory.RequestContextFactory;
import ru.datamart.prostore.query.execution.core.query.service.QueryAnalyzer;
import ru.datamart.prostore.query.execution.core.query.service.QueryDispatcher;
import ru.datamart.prostore.query.execution.core.query.service.QuerySemicolonRemover;
import ru.datamart.prostore.query.execution.core.query.utils.DatamartMnemonicExtractor;
import ru.datamart.prostore.query.execution.core.query.utils.DefaultDatamartSetter;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class QueryAnalyzerTest {
    @Mock
    private EntityDao entityDao;
    @Mock
    private QueryDispatcher queryDispatcher;
    private QueryAnalyzer queryAnalyzer;

    @Captor
    private ArgumentCaptor<CoreRequestContext<?, ?>> contextArgumentCaptor;

    @BeforeEach
    void setUp(Vertx vertx) {
        val requestContextFactory = new RequestContextFactory(TestUtils.getCoreConfiguration("test"), entityDao);
        queryAnalyzer = new QueryAnalyzer(queryDispatcher,
                TestUtils.DEFINITION_SERVICE,
                requestContextFactory,
                vertx,
                new DatamartMnemonicExtractor(new DeltaInformationExtractor()),
                new DefaultDatamartSetter(),
                new QuerySemicolonRemover(),
                new QueryRequestFactory());

        lenient().when(queryDispatcher.dispatch(contextArgumentCaptor.capture())).thenReturn(Future.succeededFuture(QueryResult.emptyResult()));

        val table = Entity.builder().entityType(EntityType.TABLE).build();
        val materializedView = Entity.builder().entityType(EntityType.MATERIALIZED_VIEW).build();
        val view = Entity.builder().entityType(EntityType.VIEW).build();
        val downloadExternalTable = Entity.builder().entityType(EntityType.DOWNLOAD_EXTERNAL_TABLE).build();
        val uploadExternalTable = Entity.builder().entityType(EntityType.UPLOAD_EXTERNAL_TABLE).build();
        lenient().when(entityDao.getEntity(eq("test_datamart"), eq("logical_table"))).thenReturn(Future.succeededFuture(table));
        lenient().when(entityDao.getEntity(eq("test_datamart"), eq("materializedview"))).thenReturn(Future.succeededFuture(materializedView));
        lenient().when(entityDao.getEntity(eq("test_datamart"), eq("view"))).thenReturn(Future.succeededFuture(view));
        lenient().when(entityDao.getEntity(eq("test_datamart"), eq("downloadexternaltable"))).thenReturn(Future.succeededFuture(downloadExternalTable));
        lenient().when(entityDao.getEntity(eq("test_datamart"), eq("uploadexternaltable"))).thenReturn(Future.succeededFuture(uploadExternalTable));
    }

    @Test
    void shouldDispatchDml(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("select count(*) from (SelEct * from TEST_DATAMART.LOGICAL_TABLE where LST_NAM='test' " +
                "union all " +
                "SelEct * from TEST_DATAMART.VIEW where LST_NAM='test1') " +
                "group by ID " +
                "order by 1 desc");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.LLR, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallEdmlWhenInsertIntoDownloadExternalTable(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("INSERT INTO TEST_DATAMART.DOWNLOADEXTERNALTABLE SELECT * FROM TEST_DATAMART.LOGICAL_TABLE");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.EDML, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallEdmlWhenInsertFromUploadExternalTable(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("INSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.uploadexternaltable");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.EDML, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallEdmlWhenRollbackOperations(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("ROLLBACK CRASHED_WRITE_OPERATIONS");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.EDML, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenInsertIntoDownloadExternalTable(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("INSERT INTO TEST_DATAMART.DOWNLOADEXTERNALTABLE SELECT * FROM TEST_DATAMART.LOGICAL_TABLE");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.EDML, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenInsertFromTable(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("INSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.LOGICAL_TABLE");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.INSERT_SELECT, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenInsertFromView(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("INSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.VIEW");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.INSERT_SELECT, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenInsertFromMaterializedView(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("INSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.MATERIALIZEDVIEW");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.INSERT_SELECT, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenInsertValues(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("INSERT INTO TEST_DATAMART.LOGICAL_TABLE VALUES (1,1,1)");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.INSERT_VALUES, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenUpsertFromTable(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("UPSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.LOGICAL_TABLE");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.UPSERT_SELECT, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenUpsertFromView(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("UPSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.VIEW");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.UPSERT_SELECT, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenUpsertFromMaterializedView(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("UPSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.MATERIALIZEDVIEW");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.UPSERT_SELECT, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenUpsertValues(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("UPSERT INTO TEST_DATAMART.LOGICAL_TABLE VALUES (1,1,1)");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.UPSERT_VALUES, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenSelectWithDatasourceType(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("SELECT * FROM TEST_DATAMART.LOGICAL_TABLE DATASOURCE_TYPE='ADB'");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.LLR, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenSelectWithSystemTime(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("SELECT * FROM TEST_DATAMART.LOGICAL_TABLE for system_time" +
                " as of '2011-01-02 00:00:00' where 1=1");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.LLR, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallEddlWhenDropDownloadExternal(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("DROP DOWNLOAD EXTERNAL TABLE TEST_DATAMART.DOWNLOADEXTERNALTABLE");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.EDDL, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDdlWhenDropTable(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("DROP TABLE TEST_DATAMART.LOGICAL_TABLE");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DDL, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDeltaWhenGetDelta(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("GET_DELTA_BY_NUM(1)");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DELTA, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDeltaWhenGetDeltaOk(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("GET_DELTA_OK()");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DELTA, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDeltaWhenGetDeltaHot(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("GET_DELTA_HOT()");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DELTA, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDeltaWhenGeDeltaByDate(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("GET_DELTA_BY_DATETIME('2018-01-01')");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DELTA, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDeltaWhenBeginDelta(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("BEGIN DELTA");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DELTA, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDeltaWhenCommitDelta(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("COMMIT DELTA");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DELTA, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDeltaWhenRollbackDelta(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("ROLLBACK DELTA");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DELTA, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallCheckWhenCheckDatabaseWithDatamart(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_DATABASE(datamart)");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.CHECK, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallCheckWhenCheckDatabase(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_DATABASE()");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.CHECK, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallCheckWhenCheckDatabaseWithoutParens(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_DATABASE");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.CHECK, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallCheckWhenCheckTable(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_TABLE(tbl1)");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.CHECK, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallCheckWhenCheckData(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_DATA(tbl1, 1)");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.CHECK, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallCheckWhenCheckSum(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_SUM(1)");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.CHECK, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallCheckWhenCheckVersions(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_VERSIONS()");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.CHECK, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallConfigWhenConfigStorageAdd(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CONFIG_STORAGE_ADD('ADB') ");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.CONFIG, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDdlWhenAlterView(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("ALTER VIEW test.view_a AS SELECT * FROM test.test_data");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DDL, context.getProcessingType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenUse(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("USE datamart");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.USE, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallDmlWhenDeleteFromTable(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("DELETE FROM test_datamart.logical_table WHERE 1=1");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DML, context.getProcessingType());
                    val dmlContext = (DmlRequestContext) context;
                    assertSame(DmlType.DELETE, dmlContext.getType());
                }).completeNow());
    }

    @Test
    void shouldCallEdmlWhenTruncate(VertxTestContext testContext) {
        // arrange
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("TRUNCATE HISTORY test_datamart.logical_table FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'\n" +
                "WHERE product_units < 10");

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(SqlProcessingType.DDL, context.getProcessingType());
                }).completeNow());
    }
}
