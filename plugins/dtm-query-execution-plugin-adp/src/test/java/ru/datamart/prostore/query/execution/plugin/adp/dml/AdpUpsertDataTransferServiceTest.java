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
package ru.datamart.prostore.query.execution.plugin.adp.dml;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.dml.dto.UpsertTransferRequest;
import ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdpUpsertDataTransferServiceTest {

    @Mock
    private DatabaseExecutor executor;

    @InjectMocks
    private AdpUpsertDataTransferService transferService;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    @Test
    void transferShouldSuccess(VertxTestContext testContext) {
        //arrange
        when(executor.executeUpdate(sqlCaptor.capture())).thenReturn(Future.succeededFuture());

        //act
        transferService.transfer(getRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    verify(executor, times(3)).executeUpdate(anyString());
                    val sqls = sqlCaptor.getAllValues();
                    assertThat(sqls.get(0)).isEqualToIgnoringNewLines("UPDATE datamart.table_actual actual\n" +
                            "SET \n" +
                            "  sys_to = 0,\n" +
                            "  sys_op = staging.sys_op\n" +
                            "FROM (\n" +
                            "  SELECT id, MAX(sys_op) as sys_op\n" +
                            "  FROM datamart.table_staging\n" +
                            "  GROUP BY id\n" +
                            "    ) staging\n" +
                            "WHERE staging.id = actual.id \n" +
                            "  AND actual.sys_from < 1\n" +
                            "  AND actual.sys_to IS NULL;");
                    assertThat(sqls.get(1)).isEqualToIgnoringNewLines("INSERT INTO datamart.table_actual (id, varchar_col, char_col, bigint_col, int_col, int32_col, double_col, float_col, date_col, time_col, timestamp_col, boolean_col, uuid_col, link_col, sys_from, sys_op)\n" +
                            "  SELECT DISTINCT ON (staging.id) staging.id, old.varchar_col, staging.char_col, old.bigint_col, staging.int_col, old.int32_col, old.double_col, old.float_col, old.date_col, old.time_col, old.timestamp_col, old.boolean_col, old.uuid_col, old.link_col, 1 AS sys_from, 0 AS sys_op \n" +
                            "  FROM datamart.table_staging staging\n" +
                            "    LEFT JOIN datamart.table_actual old\n" +
                            "      ON old.id = staging.id AND old.sys_to = 0   \n" +
                            "    LEFT JOIN datamart.table_actual actual \n" +
                            "      ON staging.id = actual.id AND actual.sys_from = 1\n" +
                            "  WHERE actual.sys_from IS NULL AND staging.sys_op <> 1;");
                    assertThat(sqls.get(2)).isEqualToIgnoringNewLines("TRUNCATE datamart.table_staging;");
                }).completeNow());
    }

    @Test
    void transferShouldFailWhenTruncateFailed(VertxTestContext testContext) {
        //arrange
        when(executor.executeUpdate(sqlCaptor.capture()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.failedFuture("Failed"));

        //act
        transferService.transfer(getRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    verify(executor, times(3)).executeUpdate(anyString());
                    val sqls = sqlCaptor.getAllValues();
                    assertThat(sqls.get(0)).isEqualToIgnoringNewLines("UPDATE datamart.table_actual actual\n" +
                            "SET \n" +
                            "  sys_to = 0,\n" +
                            "  sys_op = staging.sys_op\n" +
                            "FROM (\n" +
                            "  SELECT id, MAX(sys_op) as sys_op\n" +
                            "  FROM datamart.table_staging\n" +
                            "  GROUP BY id\n" +
                            "    ) staging\n" +
                            "WHERE staging.id = actual.id \n" +
                            "  AND actual.sys_from < 1\n" +
                            "  AND actual.sys_to IS NULL;");
                    assertThat(sqls.get(1)).isEqualToIgnoringNewLines("INSERT INTO datamart.table_actual (id, varchar_col, char_col, bigint_col, int_col, int32_col, double_col, float_col, date_col, time_col, timestamp_col, boolean_col, uuid_col, link_col, sys_from, sys_op)\n" +
                            "  SELECT DISTINCT ON (staging.id) staging.id, old.varchar_col, staging.char_col, old.bigint_col, staging.int_col, old.int32_col, old.double_col, old.float_col, old.date_col, old.time_col, old.timestamp_col, old.boolean_col, old.uuid_col, old.link_col, 1 AS sys_from, 0 AS sys_op \n" +
                            "  FROM datamart.table_staging staging\n" +
                            "    LEFT JOIN datamart.table_actual old\n" +
                            "      ON old.id = staging.id AND old.sys_to = 0   \n" +
                            "    LEFT JOIN datamart.table_actual actual \n" +
                            "      ON staging.id = actual.id AND actual.sys_from = 1\n" +
                            "  WHERE actual.sys_from IS NULL AND staging.sys_op <> 1;");
                    assertThat(sqls.get(2)).isEqualToIgnoringNewLines("TRUNCATE datamart.table_staging;");
                }).completeNow());
    }

    @Test
    void transferShouldFailWhenInsertFailed(VertxTestContext testContext) {
        //arrange
        when(executor.executeUpdate(sqlCaptor.capture()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.failedFuture("Failed"));

        //act
        transferService.transfer(getRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    verify(executor, times(2)).executeUpdate(anyString());
                    val sqls = sqlCaptor.getAllValues();
                    assertThat(sqls.get(0)).isEqualToIgnoringNewLines("UPDATE datamart.table_actual actual\n" +
                            "SET \n" +
                            "  sys_to = 0,\n" +
                            "  sys_op = staging.sys_op\n" +
                            "FROM (\n" +
                            "  SELECT id, MAX(sys_op) as sys_op\n" +
                            "  FROM datamart.table_staging\n" +
                            "  GROUP BY id\n" +
                            "    ) staging\n" +
                            "WHERE staging.id = actual.id \n" +
                            "  AND actual.sys_from < 1\n" +
                            "  AND actual.sys_to IS NULL;");
                    assertThat(sqls.get(1)).isEqualToIgnoringNewLines("INSERT INTO datamart.table_actual (id, varchar_col, char_col, bigint_col, int_col, int32_col, double_col, float_col, date_col, time_col, timestamp_col, boolean_col, uuid_col, link_col, sys_from, sys_op)\n" +
                            "  SELECT DISTINCT ON (staging.id) staging.id, old.varchar_col, staging.char_col, old.bigint_col, staging.int_col, old.int32_col, old.double_col, old.float_col, old.date_col, old.time_col, old.timestamp_col, old.boolean_col, old.uuid_col, old.link_col, 1 AS sys_from, 0 AS sys_op \n" +
                            "  FROM datamart.table_staging staging\n" +
                            "    LEFT JOIN datamart.table_actual old\n" +
                            "      ON old.id = staging.id AND old.sys_to = 0   \n" +
                            "    LEFT JOIN datamart.table_actual actual \n" +
                            "      ON staging.id = actual.id AND actual.sys_from = 1\n" +
                            "  WHERE actual.sys_from IS NULL AND staging.sys_op <> 1;");
                }).completeNow());
    }

    @Test
    void transferShouldFailWhenUpdateFailed(VertxTestContext testContext) {
        //arrange
        when(executor.executeUpdate(sqlCaptor.capture()))
                .thenReturn(Future.failedFuture("Failed"));

        //act
        transferService.transfer(getRequest())
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    verify(executor, times(1)).executeUpdate(anyString());
                    val sqls = sqlCaptor.getAllValues();
                    assertThat(sqls.get(0)).isEqualToIgnoringNewLines("UPDATE datamart.table_actual actual\n" +
                            "SET \n" +
                            "  sys_to = 0,\n" +
                            "  sys_op = staging.sys_op\n" +
                            "FROM (\n" +
                            "  SELECT id, MAX(sys_op) as sys_op\n" +
                            "  FROM datamart.table_staging\n" +
                            "  GROUP BY id\n" +
                            "    ) staging\n" +
                            "WHERE staging.id = actual.id \n" +
                            "  AND actual.sys_from < 1\n" +
                            "  AND actual.sys_to IS NULL;");
                }).completeNow());
    }

    private UpsertTransferRequest getRequest() {
        return UpsertTransferRequest.builder()
                .entity(TestUtils.createAllTypesTable())
                .sysCn(1L)
                .targetColumnList(Arrays.asList("id", "int_col", "char_col"))
                .build();
    }

}
