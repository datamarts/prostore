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
package ru.datamart.prostore.query.execution.plugin.adp.dml.upsert.values;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlInsert;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.dml.AdpUpsertDataTransferService;
import ru.datamart.prostore.query.execution.plugin.adp.dml.dto.UpsertTransferRequest;
import ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.UpsertValuesRequest;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdpStandaloneUpsertValuesServiceTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlDialect sqlDialect = calciteConfiguration.adpSqlDialect();

    @Mock
    private DatabaseExecutor executor;

    @Captor
    private ArgumentCaptor<String> executorArgCaptor;

    @InjectMocks
    private AdpStandaloneUpsertValuesService service;

    @BeforeEach
    void setUp() {
        service = new AdpStandaloneUpsertValuesService(sqlDialect, executor);

        lenient().when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenUpsertWithColumns(VertxTestContext testContext) {
        // arrange
        val request = getRequest("UPSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)");

        // act
        service.execute(request)
                .onComplete(result -> testContext.verify(() -> {

                    //assert
                    assertTrue(result.succeeded());

                    verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                    val executorParam = executorArgCaptor.getValue();
                    Assertions.assertThat(executorParam).isEqualToIgnoringNewLines("INSERT INTO null AS dest (id, col1, col2) VALUES  (1, 2, 3),\n" +
                            " (1, 2, 3),\n" +
                            " (1, 3, 3) \n" +
                            "ON CONFLICT (id) \n" +
                            "DO UPDATE SET\n" +
                            " col1 = EXCLUDED.col1, col2 = EXCLUDED.col2");
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenUpsertWithoutColumns(VertxTestContext testContext) {
        // arrange
        val request = getRequest("UPSERT INTO a.abc VALUES (1,2,3), (1,2,3), (1,3,3)");

        // act
        service.execute(request)
                .onComplete(result -> testContext.verify(() -> {

                    //assert
                    assertTrue(result.succeeded());

                    verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                    val executorParam = executorArgCaptor.getValue();
                    Assertions.assertThat(executorParam).isEqualToIgnoringNewLines("INSERT INTO null AS dest (id, col1, col2) VALUES  (1, 2, 3),\n" +
                            " (1, 2, 3),\n" +
                            " (1, 3, 3) \n" +
                            "ON CONFLICT (id) \n" +
                            "DO UPDATE SET\n" +
                            " col1 = EXCLUDED.col1, col2 = EXCLUDED.col2");
                }).completeNow());

    }

    @Test
    void shouldSuccessWhenUpsertWithOnlyOnePkKey(VertxTestContext testContext) {
        // arrange
        val request = getRequest("UPSERT INTO a.abc(id) VALUES (1)");

        // act
        service.execute(request)
                .onComplete(result -> testContext.verify(() -> {

                    //assert
                    assertTrue(result.succeeded());

                    verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                    val executorParam = executorArgCaptor.getValue();
                    Assertions.assertThat(executorParam).isEqualToIgnoringNewLines("INSERT INTO null AS dest (id) " +
                            "VALUES  (1) \n" +
                            "ON CONFLICT (id) \n" +
                            "DO NOTHING");
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenUpsertWithAllPkKeys(VertxTestContext testContext) {
        // arrange
        val sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery("UPSERT INTO a.abc(id, id2, id3) VALUES (1, 2, 3)");
        val entity = getEntityWithAllPkColumns();
        val request = new UpsertValuesRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, null);

        // act
        service.execute(request)
                .onComplete(result -> testContext.verify(() -> {

                    //assert
                    assertTrue(result.succeeded());

                    verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
                    val executorParam = executorArgCaptor.getValue();
                    Assertions.assertThat(executorParam).isEqualToIgnoringNewLines("INSERT INTO table_path AS dest (id, id2, id3) " +
                            "VALUES  (1, 2, 3) \n" +
                            "ON CONFLICT (id, id2, id3) \n" +
                            "DO NOTHING");
                }).completeNow());
    }

    @Test
    void shouldFailWhenUnknownColumn(VertxTestContext testContext) {
        // arrange
        val request = getRequest("UPSERT INTO a.abc(unknown_col) VALUES (1)");

        // act
        service.execute(request)
                .onComplete(result -> testContext.verify(() -> {

                    //assert
                    assertTrue(result.failed());
                    assertEquals("Column [unknown_col] not exists", result.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenExecutorThrows(VertxTestContext testContext) {
        // arrange
        reset(executor);
        when(executor.executeWithParams(anyString(), any(), any())).thenThrow(new RuntimeException("Exception"));
        val request = getRequest("UPSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)");

        // act
        service.execute(request)
                .onComplete(result -> testContext.verify(() -> {

                    //assert
                    assertTrue(result.failed());
                }).completeNow());
    }

    @Test
    void shouldFailWhenExecutorFails(VertxTestContext testContext) {
        // arrange
        reset(executor);
        when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.failedFuture("Failed"));
        val request = getRequest("UPSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)");

        // act
        service.execute(request)
                .onComplete(result -> testContext.verify(() -> {

                    //assert
                    assertTrue(result.failed());
                }).completeNow());
    }

    private UpsertValuesRequest getRequest(String s) {
        val sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery(s);
        val entity = getEntity();

        return new UpsertValuesRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, null);
    }

    private Entity getEntity() {
        return Entity.builder()
                .name("abc")
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
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.BIGINT)
                                .build()
                ))
                .build();
    }

    private Entity getEntityWithAllPkColumns() {
        return Entity.builder()
                .name("abc")
                .externalTableLocationPath("table_path")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("id2")
                                .primaryOrder(2)
                                .ordinalPosition(1)
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("id3")
                                .primaryOrder(3)
                                .ordinalPosition(2)
                                .type(ColumnType.BIGINT)
                                .build()
                ))
                .build();
    }

}
