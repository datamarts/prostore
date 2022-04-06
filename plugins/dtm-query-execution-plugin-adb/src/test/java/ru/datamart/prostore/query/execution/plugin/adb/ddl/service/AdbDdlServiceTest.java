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
package ru.datamart.prostore.query.execution.plugin.adb.ddl.service;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
class AdbDdlServiceTest {

    private final DdlExecutor<Void> dropTableExecutor = mock(DdlExecutor.class);
    private final DdlExecutor<Void> dropSchemaExecutor = mock(DdlExecutor.class);
    private final AdbDdlService ddlService = new AdbDdlService();

    @BeforeEach
    void beforeEach() {
        lenient().when(dropTableExecutor.getSqlKind()).thenReturn(SqlKind.DROP_TABLE);
        lenient().when(dropSchemaExecutor.getSqlKind()).thenReturn(SqlKind.DROP_SCHEMA);
        ddlService.addExecutor(dropTableExecutor);
        ddlService.addExecutor(dropSchemaExecutor);
    }

    @Test
    void shouldSuccessWithCorrectExecutor(VertxTestContext testContext) {
        //arrange
        val request = DdlRequest.builder()
                .sqlKind(SqlKind.DROP_TABLE)
                .build();
        when(dropTableExecutor.execute(any())).thenReturn(Future.succeededFuture());

        //act
        ddlService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    verify(dropTableExecutor).execute(any());
                    verify(dropSchemaExecutor, never()).execute(any());
                }).completeNow());
    }

    @Test
    void shouldFailWithCorrectExecutor(VertxTestContext testContext) {
        //arrange
        val request = DdlRequest.builder()
                .sqlKind(SqlKind.DROP_TABLE)
                .build();
        val error = "error";
        when(dropTableExecutor.execute(any())).thenReturn(Future.failedFuture(error));

        //act
        ddlService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(error, ar.cause().getMessage());
                    verify(dropTableExecutor).execute(any());
                    verify(dropSchemaExecutor, never()).execute(any());
                }).completeNow());
    }

    @Test
    void shouldFailWithIncorrectExecutor(VertxTestContext testContext) {
        //arrange
        val request = DdlRequest.builder()
                .sqlKind(SqlKind.CREATE_TABLE)
                .build();

        //act
        ddlService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals("Unknown DDL: CREATE_TABLE", ar.cause().getMessage());
                    verify(dropTableExecutor, never()).execute(any());
                    verify(dropSchemaExecutor, never()).execute(any());
                }).completeNow());
    }
}
