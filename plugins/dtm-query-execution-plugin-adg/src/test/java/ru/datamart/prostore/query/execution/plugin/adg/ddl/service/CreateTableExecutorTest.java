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
package ru.datamart.prostore.query.execution.plugin.adg.ddl.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.OperationYaml;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeSchemaGenerator;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class CreateTableExecutorTest {

    @Mock
    private DdlService<Void> ddlService;

    @Mock
    private DropTableExecutor dropTableExecutor;

    @Mock
    private AdgCartridgeSchemaGenerator generator;

    @Mock
    private AdgCartridgeClient adgCartridgeClient;

    @InjectMocks
    private CreateTableExecutor createTableExecutor;

    private final DdlRequest request = DdlRequest.builder().build();

    @Test
    void shouldSuccessWhenExecute(VertxTestContext testContext) {
        //arrange
        when(dropTableExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(generator.generate(any(), any())).thenReturn(Future.succeededFuture(new OperationYaml()));
        when(adgCartridgeClient.executeCreateSpacesQueued(any())).thenReturn(Future.succeededFuture());

        //act
        createTableExecutor.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(SqlKind.CREATE_TABLE, createTableExecutor.getSqlKind());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenDropTableExecuteFailed(VertxTestContext testContext) {
        //arrange
        val dropTableError = "drop table error";
        when(dropTableExecutor.execute(any())).thenReturn(Future.failedFuture(new DtmException(dropTableError)));

        //act
        createTableExecutor.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals(dropTableError, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenGenerateCatridgeSchemaFailed(VertxTestContext testContext) {
        //arrange
        val generateError = "generate error";
        when(dropTableExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(generator.generate(any(), any())).thenReturn(Future.failedFuture(new DtmException(generateError)));

        //act
        createTableExecutor.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals(generateError, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenCatridgeExecuteFailed(VertxTestContext testContext) {
        //arrange
        val catridgeExecuteError = "catridge execute error";
        when(dropTableExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(generator.generate(any(), any())).thenReturn(Future.succeededFuture(new OperationYaml()));
        when(adgCartridgeClient.executeCreateSpacesQueued(any())).thenReturn(Future.failedFuture(new DtmException(catridgeExecuteError)));

        //act
        createTableExecutor.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals(catridgeExecuteError, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenRegister() {
        //act
        createTableExecutor.register(ddlService);

        //assert
        verify(ddlService).addExecutor(createTableExecutor);
    }

}
