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
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.query.request.RequestContextPreparer;
import ru.datamart.prostore.query.execution.core.query.request.specific.*;
import ru.datamart.prostore.query.execution.core.query.service.QueryAnalyzer;
import ru.datamart.prostore.query.execution.core.query.service.QueryDispatcher;
import ru.datamart.prostore.query.execution.core.query.service.QuerySemicolonRemover;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;

import java.util.Arrays;

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
        val appConfiguration = TestUtils.getCoreConfiguration("test");
        val requestContextPreparer = new RequestContextPreparer(Arrays.asList(
                new CheckRequestContextPreparer(appConfiguration),
                new ConfigRequestContextPreparer(appConfiguration),
                new DdlRequestContextPreparer(appConfiguration),
                new DeltaRequestContextPreparer(appConfiguration),
                new DmlEdmlRequestContextPreparer(appConfiguration, entityDao),
                new EddlRequestContextPreparer(appConfiguration)
        ));

        queryAnalyzer = new QueryAnalyzer(queryDispatcher,
                TestUtils.DEFINITION_SERVICE,
                requestContextPreparer,
                vertx,
                new QuerySemicolonRemover());

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
        assertDmlType(testContext, "select count(*) from (SelEct * from TEST_DATAMART.LOGICAL_TABLE where LST_NAM='test' " +
                "union all " +
                "SelEct * from TEST_DATAMART.VIEW where LST_NAM='test1') " +
                "group by ID " +
                "order by 1 desc", DmlType.LLR);
    }

    @Test
    void shouldCallEdmlWhenInsertIntoDownloadExternalTable(VertxTestContext testContext) {
        assertProcessingType(testContext, "INSERT INTO TEST_DATAMART.DOWNLOADEXTERNALTABLE SELECT * FROM TEST_DATAMART.LOGICAL_TABLE", SqlProcessingType.EDML);
    }

    @Test
    void shouldCallEdmlWhenInsertFromUploadExternalTable(VertxTestContext testContext) {
        assertProcessingType(testContext, "INSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.uploadexternaltable", SqlProcessingType.EDML);
    }

    @Test
    void shouldCallEdmlWhenRollbackOperations(VertxTestContext testContext) {
        assertProcessingType(testContext, "ROLLBACK CRASHED_WRITE_OPERATIONS", SqlProcessingType.EDML);
    }

    @Test
    void shouldCallDmlWhenInsertIntoDownloadExternalTable(VertxTestContext testContext) {
        assertProcessingType(testContext, "INSERT INTO TEST_DATAMART.DOWNLOADEXTERNALTABLE SELECT * FROM TEST_DATAMART.LOGICAL_TABLE", SqlProcessingType.EDML);
    }

    @Test
    void shouldCallDmlWhenInsertFromTable(VertxTestContext testContext) {
        assertDmlType(testContext, "INSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.LOGICAL_TABLE", DmlType.INSERT_SELECT);
    }

    @Test
    void shouldCallDmlWhenInsertFromView(VertxTestContext testContext) {
        assertDmlType(testContext, "INSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.VIEW", DmlType.INSERT_SELECT);
    }

    @Test
    void shouldCallDmlWhenInsertFromMaterializedView(VertxTestContext testContext) {
        assertDmlType(testContext, "INSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.MATERIALIZEDVIEW", DmlType.INSERT_SELECT);
    }

    @Test
    void shouldCallDmlWhenInsertValues(VertxTestContext testContext) {
        assertDmlType(testContext, "INSERT INTO TEST_DATAMART.LOGICAL_TABLE VALUES (1,1,1)", DmlType.INSERT_VALUES);
    }

    @Test
    void shouldCallDmlWhenUpsertFromTable(VertxTestContext testContext) {
        assertDmlType(testContext, "UPSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.LOGICAL_TABLE", DmlType.UPSERT_SELECT);
    }

    @Test
    void shouldCallDmlWhenUpsertFromView(VertxTestContext testContext) {
        assertDmlType(testContext, "UPSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.VIEW", DmlType.UPSERT_SELECT);
    }

    @Test
    void shouldCallDmlWhenUpsertFromMaterializedView(VertxTestContext testContext) {
        assertDmlType(testContext, "UPSERT INTO TEST_DATAMART.LOGICAL_TABLE SELECT * FROM TEST_DATAMART.MATERIALIZEDVIEW", DmlType.UPSERT_SELECT);
    }

    @Test
    void shouldCallDmlWhenUpsertValues(VertxTestContext testContext) {
        assertDmlType(testContext, "UPSERT INTO TEST_DATAMART.LOGICAL_TABLE VALUES (1,1,1)", DmlType.UPSERT_VALUES);
    }

    @Test
    void shouldCallDmlWhenSelectWithDatasourceType(VertxTestContext testContext) {
        assertDmlType(testContext, "SELECT * FROM TEST_DATAMART.LOGICAL_TABLE DATASOURCE_TYPE='ADB'", DmlType.LLR);
    }

    @Test
    void shouldCallDmlWhenSelectWithSystemTime(VertxTestContext testContext) {
        assertDmlType(testContext, "SELECT * FROM TEST_DATAMART.LOGICAL_TABLE for system_time" +
                " as of '2011-01-02 00:00:00' where 1=1", DmlType.LLR);
    }

    @Test
    void shouldCallEddlWhenDropDownloadExternal(VertxTestContext testContext) {
        assertProcessingType(testContext, "DROP DOWNLOAD EXTERNAL TABLE TEST_DATAMART.DOWNLOADEXTERNALTABLE", SqlProcessingType.EDDL);
    }

    @Test
    void shouldCallDdlWhenDropTable(VertxTestContext testContext) {
        assertProcessingType(testContext, "DROP TABLE TEST_DATAMART.LOGICAL_TABLE", SqlProcessingType.DDL);
    }

    @Test
    void shouldCallDdlWhenEraseChangeOperation(VertxTestContext testContext) {
        assertProcessingType(testContext, "ERASE_CHANGE_OPERATION(0)", SqlProcessingType.DDL);
    }

    @Test
    void shouldCallDdlWhenAlterView(VertxTestContext testContext) {
        assertProcessingType(testContext, "ALTER VIEW test.view_a AS SELECT * FROM test.test_data", SqlProcessingType.DDL);
    }

    @Test
    void shouldCallDdlWhenTruncate(VertxTestContext testContext) {
        assertProcessingType(testContext, "TRUNCATE HISTORY test_datamart.logical_table FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14'\n" +
                "WHERE product_units < 10", SqlProcessingType.DDL);
    }

    @Test
    void shouldCallDeltaWhenGetDelta(VertxTestContext testContext) {
        assertProcessingType(testContext, "GET_DELTA_BY_NUM(1)", SqlProcessingType.DELTA);
    }

    @Test
    void shouldCallDeltaWhenGetDeltaOk(VertxTestContext testContext) {
        assertProcessingType(testContext, "GET_DELTA_OK()", SqlProcessingType.DELTA);
    }

    @Test
    void shouldCallDeltaWhenGetDeltaHot(VertxTestContext testContext) {
        assertProcessingType(testContext, "GET_DELTA_HOT()", SqlProcessingType.DELTA);
    }

    @Test
    void shouldCallDeltaWhenGeDeltaByDate(VertxTestContext testContext) {
        assertProcessingType(testContext, "GET_DELTA_BY_DATETIME('2018-01-01')", SqlProcessingType.DELTA);
    }

    @Test
    void shouldCallDeltaWhenBeginDelta(VertxTestContext testContext) {
        assertProcessingType(testContext, "BEGIN DELTA", SqlProcessingType.DELTA);
    }

    @Test
    void shouldCallDeltaWhenCommitDelta(VertxTestContext testContext) {
        assertProcessingType(testContext, "COMMIT DELTA", SqlProcessingType.DELTA);
    }

    @Test
    void shouldCallDeltaWhenRollbackDelta(VertxTestContext testContext) {

        assertProcessingType(testContext, "ROLLBACK DELTA", SqlProcessingType.DELTA);
    }

    @Test
    void shouldCallCheckWhenCheckDatabaseWithDatamart(VertxTestContext testContext) {
        assertProcessingType(testContext, "CHECK_DATABASE(datamart)", SqlProcessingType.CHECK);
    }

    @Test
    void shouldCallCheckWhenCheckDatabase(VertxTestContext testContext) {
        assertProcessingType(testContext, "CHECK_DATABASE()", SqlProcessingType.CHECK);
    }

    @Test
    void shouldCallCheckWhenCheckDatabaseWithoutParens(VertxTestContext testContext) {
        assertProcessingType(testContext, "CHECK_DATABASE", SqlProcessingType.CHECK);
    }

    @Test
    void shouldCallCheckWhenCheckTable(VertxTestContext testContext) {
        assertProcessingType(testContext, "CHECK_TABLE(tbl1)", SqlProcessingType.CHECK);
    }

    @Test
    void shouldCallCheckWhenCheckData(VertxTestContext testContext) {
        assertProcessingType(testContext, "CHECK_DATA(tbl1, 1)", SqlProcessingType.CHECK);
    }

    @Test
    void shouldCallCheckWhenCheckSum(VertxTestContext testContext) {
        assertProcessingType(testContext, "CHECK_SUM(1)", SqlProcessingType.CHECK);
    }

    @Test
    void shouldCallCheckWhenCheckVersions(VertxTestContext testContext) {
        assertProcessingType(testContext, "CHECK_VERSIONS()", SqlProcessingType.CHECK);
    }

    @Test
    void shouldCallConfigWhenConfigStorageAdd(VertxTestContext testContext) {
        assertProcessingType(testContext, "CONFIG_STORAGE_ADD('ADB') ", SqlProcessingType.CONFIG);
    }

    @Test
    void shouldCallDmlWhenUse(VertxTestContext testContext) {
        assertDmlType(testContext, "USE datamart", DmlType.USE);
    }

    @Test
    void shouldCallDmlWhenDeleteFromTable(VertxTestContext testContext) {
        assertDmlType(testContext, "DELETE FROM test_datamart.logical_table WHERE 1=1", DmlType.DELETE);
    }

    private void assertProcessingType(VertxTestContext testContext, String sql, SqlProcessingType processingType) {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql(sql);

        // act
        queryAnalyzer.analyzeAndExecute(queryRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    val context = contextArgumentCaptor.getValue();
                    assertSame(processingType, context.getProcessingType());
                }).completeNow());
    }

    private void assertDmlType(VertxTestContext testContext, String sql, DmlType dmlType) {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql(sql);

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
                    assertSame(dmlType, dmlContext.getType());
                }).completeNow());
    }
}
