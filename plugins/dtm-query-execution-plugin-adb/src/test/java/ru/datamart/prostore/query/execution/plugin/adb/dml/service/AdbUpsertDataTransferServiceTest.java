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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service;

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
import ru.datamart.prostore.query.execution.plugin.adb.dml.dto.UpsertTransferRequest;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import utils.CreateEntityUtils;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdbUpsertDataTransferServiceTest {

    @Mock
    private DatabaseExecutor executor;

    @InjectMocks
    private AdbUpsertDataTransferService transferService;

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
                    assertThat(sqls.get(0)).isEqualToIgnoringNewLines("UPDATE test_schema.test_table_actual actual" +
                            "SET " +
                            "  sys_to = 0," +
                            "  sys_op = staging.sys_op" +
                            "FROM (" +
                            "  SELECT id, pk2, MAX(sys_op) as sys_op" +
                            "  FROM test_schema.test_table_staging" +
                            "  GROUP BY id, pk2" +
                            "    ) staging" +
                            "WHERE staging.id = actual.id AND staging.pk2 = actual.pk2 " +
                            "  AND actual.sys_from < 1" +
                            "  AND actual.sys_to IS NULL;");
                    assertThat(sqls.get(1)).isEqualToIgnoringNewLines("INSERT INTO test_schema.test_table_actual (id, sk_key2, pk2, sk_key3, VARCHAR_type, CHAR_type, BIGINT_type, INT_type, INT32_type, DOUBLE_type, FLOAT_type, DATE_type, TIME_type, TIMESTAMP_type, BOOLEAN_type, UUID_type, LINK_type, sys_from, sys_op)\n" +
                            "  SELECT DISTINCT ON (staging.id, staging.pk2) staging.id, old.sk_key2, staging.pk2, old.sk_key3, old.VARCHAR_type, staging.CHAR_type, old.BIGINT_type, old.INT_type, old.INT32_type, old.DOUBLE_type, old.FLOAT_type, old.DATE_type, old.TIME_type, old.TIMESTAMP_type, old.BOOLEAN_type, old.UUID_type, old.LINK_type, 1 AS sys_from, 0 AS sys_op \n" +
                            "  FROM test_schema.test_table_staging staging\n" +
                            "    LEFT JOIN test_schema.test_table_actual old\n" +
                            "      ON old.id = staging.id AND old.pk2 = staging.pk2 AND old.sys_to = 0   \n" +
                            "    LEFT JOIN test_schema.test_table_actual actual \n" +
                            "      ON staging.id = actual.id AND staging.pk2 = actual.pk2 AND actual.sys_from = 1\n" +
                            "  WHERE actual.sys_from IS NULL AND staging.sys_op <> 1;");
                    assertThat(sqls.get(2)).isEqualToIgnoringNewLines("TRUNCATE test_schema.test_table_staging;");
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
                    assertThat(sqls.get(0)).isEqualToIgnoringNewLines("UPDATE test_schema.test_table_actual actual" +
                            "SET " +
                            "  sys_to = 0," +
                            "  sys_op = staging.sys_op" +
                            "FROM (" +
                            "  SELECT id, pk2, MAX(sys_op) as sys_op" +
                            "  FROM test_schema.test_table_staging" +
                            "  GROUP BY id, pk2" +
                            "    ) staging" +
                            "WHERE staging.id = actual.id AND staging.pk2 = actual.pk2 " +
                            "  AND actual.sys_from < 1" +
                            "  AND actual.sys_to IS NULL;");
                    assertThat(sqls.get(1)).isEqualToIgnoringNewLines("INSERT INTO test_schema.test_table_actual (id, sk_key2, pk2, sk_key3, VARCHAR_type, CHAR_type, BIGINT_type, INT_type, INT32_type, DOUBLE_type, FLOAT_type, DATE_type, TIME_type, TIMESTAMP_type, BOOLEAN_type, UUID_type, LINK_type, sys_from, sys_op)\n" +
                            "  SELECT DISTINCT ON (staging.id, staging.pk2) staging.id, old.sk_key2, staging.pk2, old.sk_key3, old.VARCHAR_type, staging.CHAR_type, old.BIGINT_type, old.INT_type, old.INT32_type, old.DOUBLE_type, old.FLOAT_type, old.DATE_type, old.TIME_type, old.TIMESTAMP_type, old.BOOLEAN_type, old.UUID_type, old.LINK_type, 1 AS sys_from, 0 AS sys_op \n" +
                            "  FROM test_schema.test_table_staging staging\n" +
                            "    LEFT JOIN test_schema.test_table_actual old\n" +
                            "      ON old.id = staging.id AND old.pk2 = staging.pk2 AND old.sys_to = 0   \n" +
                            "    LEFT JOIN test_schema.test_table_actual actual \n" +
                            "      ON staging.id = actual.id AND staging.pk2 = actual.pk2 AND actual.sys_from = 1\n" +
                            "  WHERE actual.sys_from IS NULL AND staging.sys_op <> 1;");
                    assertThat(sqls.get(2)).isEqualToIgnoringNewLines("TRUNCATE test_schema.test_table_staging;");
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
                    assertThat(sqls.get(0)).isEqualToIgnoringNewLines("UPDATE test_schema.test_table_actual actual" +
                            "SET " +
                            "  sys_to = 0," +
                            "  sys_op = staging.sys_op" +
                            "FROM (" +
                            "  SELECT id, pk2, MAX(sys_op) as sys_op" +
                            "  FROM test_schema.test_table_staging" +
                            "  GROUP BY id, pk2" +
                            "    ) staging" +
                            "WHERE staging.id = actual.id AND staging.pk2 = actual.pk2 " +
                            "  AND actual.sys_from < 1" +
                            "  AND actual.sys_to IS NULL;");
                    assertThat(sqls.get(1)).isEqualToIgnoringNewLines("INSERT INTO test_schema.test_table_actual (id, sk_key2, pk2, sk_key3, VARCHAR_type, CHAR_type, BIGINT_type, INT_type, INT32_type, DOUBLE_type, FLOAT_type, DATE_type, TIME_type, TIMESTAMP_type, BOOLEAN_type, UUID_type, LINK_type, sys_from, sys_op)\n" +
                            "  SELECT DISTINCT ON (staging.id, staging.pk2) staging.id, old.sk_key2, staging.pk2, old.sk_key3, old.VARCHAR_type, staging.CHAR_type, old.BIGINT_type, old.INT_type, old.INT32_type, old.DOUBLE_type, old.FLOAT_type, old.DATE_type, old.TIME_type, old.TIMESTAMP_type, old.BOOLEAN_type, old.UUID_type, old.LINK_type, 1 AS sys_from, 0 AS sys_op \n" +
                            "  FROM test_schema.test_table_staging staging\n" +
                            "    LEFT JOIN test_schema.test_table_actual old\n" +
                            "      ON old.id = staging.id AND old.pk2 = staging.pk2 AND old.sys_to = 0   \n" +
                            "    LEFT JOIN test_schema.test_table_actual actual \n" +
                            "      ON staging.id = actual.id AND staging.pk2 = actual.pk2 AND actual.sys_from = 1\n" +
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
                    assertThat(sqls.get(0)).isEqualToIgnoringNewLines("UPDATE test_schema.test_table_actual actual" +
                            "SET " +
                            "  sys_to = 0," +
                            "  sys_op = staging.sys_op" +
                            "FROM (" +
                            "  SELECT id, pk2, MAX(sys_op) as sys_op" +
                            "  FROM test_schema.test_table_staging" +
                            "  GROUP BY id, pk2" +
                            "    ) staging" +
                            "WHERE staging.id = actual.id AND staging.pk2 = actual.pk2 " +
                            "  AND actual.sys_from < 1" +
                            "  AND actual.sys_to IS NULL;");
                }).completeNow());
    }

    private UpsertTransferRequest getRequest() {
        return UpsertTransferRequest.builder()
                .entity(CreateEntityUtils.getEntity())
                .sysCn(1L)
                .targetColumnList(Arrays.asList("id", "pk2", "CHAR_type"))
                .build();
    }

}
