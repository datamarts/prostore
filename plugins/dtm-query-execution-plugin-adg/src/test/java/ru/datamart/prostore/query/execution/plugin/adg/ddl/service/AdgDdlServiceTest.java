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
package ru.datamart.prostore.query.execution.plugin.adg.ddl.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.plugin.api.exception.DdlDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlExecutor;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdgDdlServiceTest {

    @Mock
    private DdlExecutor<Void> createTableExecutor;

    private final AdgDdlService adgDdlService = new AdgDdlService();

    @Test
    void shouldSuccessWhenExecuteCreateTableSuccess(VertxTestContext testContext) {
        //arrange
        when(createTableExecutor.getSqlKind()).thenReturn(SqlKind.CREATE_TABLE);
        adgDdlService.addExecutor(createTableExecutor);
        when(createTableExecutor.execute(any())).thenReturn(Future.succeededFuture());

        val request = DdlRequest.builder()
                .sqlKind(SqlKind.CREATE_TABLE)
                .build();

        //act
        adgDdlService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenExecuteCreateTableFailed(VertxTestContext testContext) {
        //arrange
        val error = "create table error";
        when(createTableExecutor.getSqlKind()).thenReturn(SqlKind.CREATE_TABLE);
        adgDdlService.addExecutor(createTableExecutor);
        when(createTableExecutor.execute(any())).thenReturn(Future.failedFuture(new DtmException(error)));

        val request = DdlRequest.builder()
                .sqlKind(SqlKind.CREATE_TABLE)
                .build();

        //act
        adgDdlService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals(error, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenExecuteUnknownedDdl(VertxTestContext testContext) {
        //arrange
        when(createTableExecutor.getSqlKind()).thenReturn(SqlKind.CREATE_TABLE);
        adgDdlService.addExecutor(createTableExecutor);

        val sqlKind = SqlKind.CREATE_SCHEMA;
        val request = DdlRequest.builder()
                .sqlKind(sqlKind)
                .build();

        //act
        adgDdlService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DdlDatasourceException);
                    assertEquals(String.format("Unknown DDL: %s", sqlKind), ar.cause().getMessage());
                }).completeNow());
    }
}
