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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.plugin.adg.dml.service.insert.select.DestinationInsertSelectHandler;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class AdgInsertSelectServiceTest {

    private final static DestinationInsertSelectHandler INSERT_SELECT_TO_ADG_HANDLER = mock(DestinationInsertSelectHandler.class);
    private final static DestinationInsertSelectHandler INSERT_SELECT_TO_ADQM_HANDLER = mock(DestinationInsertSelectHandler.class);

    private static AdgInsertSelectService service;

    @BeforeAll
    static void setUp() {
        when(INSERT_SELECT_TO_ADG_HANDLER.getDestinations()).thenReturn(SourceType.ADG);
        when(INSERT_SELECT_TO_ADQM_HANDLER.getDestinations()).thenReturn(SourceType.ADQM);
        service = new AdgInsertSelectService(Arrays.asList(INSERT_SELECT_TO_ADG_HANDLER, INSERT_SELECT_TO_ADQM_HANDLER));
    }

    @Test
    void shouldSuccessWhenHandlerSuccess(VertxTestContext testContext) {
        //arrange
        when(INSERT_SELECT_TO_ADG_HANDLER.handle(any())).thenReturn(Future.succeededFuture());
        val request = getRequest(SourceType.ADG);

        //act
        service.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenHandlersSuccess(VertxTestContext testContext) {
        //arrange
        when(INSERT_SELECT_TO_ADG_HANDLER.handle(any())).thenReturn(Future.succeededFuture());
        when(INSERT_SELECT_TO_ADQM_HANDLER.handle(any())).thenReturn(Future.succeededFuture());
        val request = getRequest(SourceType.ADG, SourceType.ADQM);

        //act
        service.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenHandlerFailed(VertxTestContext testContext) {
        //arrange
        val error = "handler error";
        when(INSERT_SELECT_TO_ADG_HANDLER.handle(any())).thenReturn(Future.failedFuture(new DtmException(error)));
        val request = getRequest(SourceType.ADG);

        //act
        service.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals(error, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenAdgHandlerFailed(VertxTestContext testContext) {
        //arrange
        val error = "adg handler error";
        when(INSERT_SELECT_TO_ADG_HANDLER.handle(any())).thenReturn(Future.failedFuture(new DtmException(error)));
        when(INSERT_SELECT_TO_ADQM_HANDLER.handle(any())).thenReturn(Future.succeededFuture());
        val request = getRequest(SourceType.ADG, SourceType.ADQM);

        //act
        service.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals(error, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenUnknownDestination(VertxTestContext testContext) {
        //arrange
        val request = getRequest(SourceType.ADB);

        //act
        service.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals(String.format("Feature is not implemented [insert select from [ADG] to [%s]]", SourceType.ADB), ar.cause().getMessage());
                }).completeNow());
    }

    private InsertSelectRequest getRequest(SourceType... sourceTypes) {
        return new InsertSelectRequest(
                null,
                null,
                null,
                null,
                Entity.builder()
                        .destination(Arrays.stream(sourceTypes)
                                .collect(Collectors.toSet()))
                        .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                        .build(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

}
