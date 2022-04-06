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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.delete.AdbLogicalDeleteService;
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.delete.AdbStandaloneDeleteService;
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.upsert.values.AdbLogicalUpsertValuesService;
import ru.datamart.prostore.query.execution.plugin.api.request.DeleteRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdbDeleteServiceTest {

    @Mock
    private AdbLogicalDeleteService logicalDeleteService;

    @Mock
    private AdbStandaloneDeleteService standaloneDeleteService;

    @InjectMocks
    private AdbDeleteService deleteService;

    @Test
    void shouldSuccessLogicalDelete(VertxTestContext testContext) {
        //arrange
        val request  = createRequest(EntityType.TABLE);
        when(logicalDeleteService.execute(any())).thenReturn(Future.succeededFuture());

        //act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    verify(logicalDeleteService).execute(any());
                    verifyNoInteractions(standaloneDeleteService);
                }).completeNow());
    }

    @Test
    void shouldFailLogicalDelete(VertxTestContext testContext) {
        //arrange
        val request  = createRequest(EntityType.TABLE);
        val error = "error";
        when(logicalDeleteService.execute(any())).thenReturn(Future.failedFuture(error));

        //act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(error, ar.cause().getMessage());
                    verify(logicalDeleteService).execute(any());
                    verifyNoInteractions(standaloneDeleteService);
                }).completeNow());
    }

    @Test
    void shouldSuccessStandaloneDelete(VertxTestContext testContext) {
        //arrange
        val request  = createRequest(EntityType.WRITEABLE_EXTERNAL_TABLE);
        when(standaloneDeleteService.execute(any())).thenReturn(Future.succeededFuture());

        //act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    verify(standaloneDeleteService).execute(any());
                    verifyNoInteractions(logicalDeleteService);
                }).completeNow());
    }

    @Test
    void shouldFailStandaloneDelete(VertxTestContext testContext) {
        //arrange
        val request  = createRequest(EntityType.WRITEABLE_EXTERNAL_TABLE);
        val error = "error";
        when(standaloneDeleteService.execute(any())).thenReturn(Future.failedFuture(error));

        //act
        deleteService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(error, ar.cause().getMessage());
                    verify(standaloneDeleteService).execute(any());
                    verifyNoInteractions(logicalDeleteService);
                }).completeNow());
    }

    private DeleteRequest createRequest(EntityType type) {
        return new DeleteRequest(null,
                null,
                null,
                Entity.builder()
                        .entityType(type)
                        .build(),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
