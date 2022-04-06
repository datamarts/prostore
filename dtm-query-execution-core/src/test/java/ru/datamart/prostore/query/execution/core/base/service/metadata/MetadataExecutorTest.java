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
package ru.datamart.prostore.query.execution.core.base.service.metadata;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import static org.apache.calcite.sql.SqlKind.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class MetadataExecutorTest {
    private final static Set<SourceType> SOURCES = EnumSet.of(SourceType.ADB, SourceType.ADQM, SourceType.ADG, SourceType.ADP);

    @Captor
    ArgumentCaptor<DdlRequest> ddlRequestCaptor;

    @Mock
    private DataSourcePluginService dataSourcePluginService;
    @InjectMocks
    private MetadataExecutor metadataExecutor;

    @Test
    void shouldSuccess(VertxTestContext testContext) {
        //arrange
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCES);
        when(dataSourcePluginService.ddl(any(), any(), any())).thenReturn(Future.succeededFuture());

        //act
        metadataExecutor.execute(getDdlContext(SELECT))
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenEmptySourceTypes(VertxTestContext testContext) {
        //arrange
        when(dataSourcePluginService.getSourceTypes()).thenReturn(Collections.emptySet());

        //act
        metadataExecutor.execute(getDdlContext(SELECT))
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenDdlFailed(VertxTestContext testContext) {
        //arrange
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCES);
        when(dataSourcePluginService.ddl(any(), any(), any())).thenReturn(Future.failedFuture("error"));

        //act
        metadataExecutor.execute(getDdlContext(SELECT))
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertEquals("error", ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldReplaceSqlKindForCreateMaterializedView(VertxTestContext testContext) {
        //arrange
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCES);
        when(dataSourcePluginService.ddl(any(), any(), ddlRequestCaptor.capture())).thenReturn(Future.succeededFuture());

        //act
        metadataExecutor.execute(getDdlContext(CREATE_MATERIALIZED_VIEW))
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(CREATE_TABLE, ddlRequestCaptor.getValue().getSqlKind());
                }).completeNow());
    }

    @Test
    void shouldReplaceSqlKindForDropMaterializedView(VertxTestContext testContext) {
        //arrange
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCES);
        when(dataSourcePluginService.ddl(any(), any(), ddlRequestCaptor.capture())).thenReturn(Future.succeededFuture());

        //act
        metadataExecutor.execute(getDdlContext(DROP_MATERIALIZED_VIEW))
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(DROP_TABLE, ddlRequestCaptor.getValue().getSqlKind());
                }).completeNow());
    }

    private DdlRequestContext getDdlContext(SqlKind sqlKind) {
        val datamartRequest = new DatamartRequest(QueryRequest.builder()
                .datamartMnemonic("dtm")
                .build());
        SqlNode query = null;
        if (sqlKind == CREATE_MATERIALIZED_VIEW) {
            query = TestUtils.DEFINITION_SERVICE.processingQuery("CREATE MATERIALIZED VIEW DB.COUNT_BOOKS_MATVIEW (\n" +
                    "BOOK_ID INT NOT NULL,\n" +
                    "BOOK_NAME VARCHAR(200) NOT NULL,\n" +
                    "BOOKS_COUNT INT NOT NULL,\n" +
                    "PRIMARY KEY (BOOK_ID)\n" +
                    ") DISTRIBUTED BY (BOOK_ID)\n" +
                    " AS SELECT BOOK_ID AS BOOK_ID, BOOK_NAME AS BOOK_NAME, COUNT(*) AS BOOKS_COUNT FROM L_DB.BOOKS GROUP BY BOOK_ID, BOOK_NAME\n" +
                    "DATASOURCE_TYPE = 'ADQM'");
        } else if (sqlKind == DROP_MATERIALIZED_VIEW) {
            query = TestUtils.DEFINITION_SERVICE.processingQuery("DROP MATERIALIZED VIEW DB.COUNT_BOOKS_MATVIEW");
        } else if (sqlKind == SELECT) {
            query = TestUtils.DEFINITION_SERVICE.processingQuery("SELECT * FROM DB.COUNT_BOOKS_MATVIEW");
        }
        return new DdlRequestContext(null, datamartRequest, query, null, null);
    }

}