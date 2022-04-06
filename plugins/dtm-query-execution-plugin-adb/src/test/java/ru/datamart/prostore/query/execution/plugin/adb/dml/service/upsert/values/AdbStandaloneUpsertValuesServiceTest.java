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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service.upsert.values;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlInsert;
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
import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.UpsertValuesRequest;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdbStandaloneUpsertValuesServiceTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlDialect sqlDialect = calciteConfiguration.adbSqlDialect();

    @Mock
    private DatabaseExecutor executor;

    @InjectMocks
    private AdbStandaloneUpsertValuesService service;

    @Captor
    private ArgumentCaptor<List<PreparedStatementRequest>> executorArgCaptor;

    @BeforeEach
    void setUp() {
        service = new AdbStandaloneUpsertValuesService(sqlDialect, executor);

        lenient().when(executor.executeInTransaction(anyList())).thenReturn(Future.succeededFuture());
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

                    verify(executor).executeInTransaction(executorArgCaptor.capture());
                    val executorParam = executorArgCaptor.getValue();

                    assertThat(executorParam.get(0).getSql()).isEqualToIgnoringNewLines("CREATE TEMP TABLE tmp_4ca47678_b70e_48e0_94a5_e2f455e03775 (id int8, col1 int8, col2 int8) \n" +
                            "ON COMMIT DROP");
                    assertThat(executorParam.get(1).getSql()).isEqualToIgnoringNewLines("INSERT INTO tmp_4ca47678_b70e_48e0_94a5_e2f455e03775 (id, col1, col2) VALUES  (1, 2, 3),\n" +
                            " (1, 2, 3),\n" +
                            " (1, 3, 3)");
                    assertThat(executorParam.get(2).getSql()).isEqualToIgnoringNewLines("UPDATE public.abc dst\n" +
                            "  SET \n" +
                            "    col1 = tmp.col1, col2 = tmp.col2  FROM tmp_4ca47678_b70e_48e0_94a5_e2f455e03775 tmp\n" +
                            "  WHERE dst.id = tmp.id;");
                    assertThat(executorParam.get(3).getSql()).isEqualToIgnoringNewLines("INSERT INTO public.abc (id, col1, col2)\n" +
                            "    SELECT tmp.id, tmp.col1, tmp.col2 \n" +
                            "    FROM tmp_4ca47678_b70e_48e0_94a5_e2f455e03775 tmp\n" +
                            "      LEFT JOIN public.abc dst ON dst.id = tmp.id\n" +
                            "    WHERE dst.id IS null;");
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenUpsertWithOnlyPkColumn(VertxTestContext testContext) {
        // arrange
        val request = getRequest("UPSERT INTO a.abc(id) VALUES (1)");

        // act
        service.execute(request)
                .onComplete(result -> testContext.verify(() -> {

                    //assert
                    assertTrue(result.succeeded());

                    verify(executor).executeInTransaction(executorArgCaptor.capture());
                    val executorParam = executorArgCaptor.getValue();

                    assertThat(executorParam.get(0).getSql()).isEqualToIgnoringNewLines("CREATE TEMP TABLE tmp_4ca47678_b70e_48e0_94a5_e2f455e03775 (id int8) \n" +
                            "ON COMMIT DROP");
                    assertThat(executorParam.get(1).getSql()).isEqualToIgnoringNewLines("INSERT INTO tmp_4ca47678_b70e_48e0_94a5_e2f455e03775 (id) VALUES  (1)");
                    assertThat(executorParam.get(2).getSql()).isEqualToIgnoringNewLines("INSERT INTO public.abc (id)\n" +
                            "    SELECT tmp.id \n" +
                            "    FROM tmp_4ca47678_b70e_48e0_94a5_e2f455e03775 tmp\n" +
                            "      LEFT JOIN public.abc dst ON dst.id = tmp.id\n" +
                            "    WHERE dst.id IS null;");
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

                    verify(executor).executeInTransaction(executorArgCaptor.capture());
                    val executorParam = executorArgCaptor.getValue();

                    assertThat(executorParam.get(0).getSql()).isEqualToIgnoringNewLines("CREATE TEMP TABLE tmp_4ca47678_b70e_48e0_94a5_e2f455e03775 (id int8, col1 int8, col2 int8) \n" +
                            "ON COMMIT DROP");
                    assertThat(executorParam.get(1).getSql()).isEqualToIgnoringNewLines("INSERT INTO tmp_4ca47678_b70e_48e0_94a5_e2f455e03775 (id, col1, col2) VALUES  (1, 2, 3),\n" +
                            " (1, 2, 3),\n" +
                            " (1, 3, 3)");
                    assertThat(executorParam.get(2).getSql()).isEqualToIgnoringNewLines("UPDATE public.abc dst\n" +
                            "  SET \n" +
                            "    col1 = tmp.col1, col2 = tmp.col2  FROM tmp_4ca47678_b70e_48e0_94a5_e2f455e03775 tmp\n" +
                            "  WHERE dst.id = tmp.id;");
                    assertThat(executorParam.get(3).getSql()).isEqualToIgnoringNewLines("INSERT INTO public.abc (id, col1, col2)\n" +
                            "    SELECT tmp.id, tmp.col1, tmp.col2 \n" +
                            "    FROM tmp_4ca47678_b70e_48e0_94a5_e2f455e03775 tmp\n" +
                            "      LEFT JOIN public.abc dst ON dst.id = tmp.id\n" +
                            "    WHERE dst.id IS null;");
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
        when(executor.executeInTransaction(anyList())).thenThrow(new RuntimeException("Exception"));
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
        when(executor.executeInTransaction(anyList())).thenReturn(Future.failedFuture("Failed"));
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

        return new UpsertValuesRequest(UUID.fromString("4ca47678-b70e-48e0-94a5-e2f455e03775"), "dev", "datamart", 1L, entity, sqlNode, null);
    }

    private Entity getEntity() {
        return Entity.builder()
                .name("abc")
                .externalTableLocationPath("public.abc")
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

}
