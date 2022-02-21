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
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
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
class DropSchemaExecutorTest {

    private static final String ENV = "test";
    private static final String DATAMART = "dtm";
    private static final String PREFIX = "prefix";

    @Mock
    private DdlService<Void> ddlService;

    @Mock
    private AdgCartridgeClient cartridgeClient;

    @Mock
    private AdgHelperTableNamesFactory adgHelperTableNamesFactory;

    @InjectMocks
    private DropSchemaExecutor dropSchemaExecutor;

    private final DdlRequest request = DdlRequest.builder()
            .envName(ENV)
            .datamartMnemonic(DATAMART)
            .build();

    @Test
    void shouldSuccessWhenExecute(VertxTestContext testContext) {
        //arrange
        when(adgHelperTableNamesFactory.getTablePrefix(ENV, DATAMART)).thenReturn(PREFIX);
        when(cartridgeClient.executeDeleteSpacesWithPrefixQueued(any())).thenReturn(Future.succeededFuture());

        //act
        dropSchemaExecutor.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(SqlKind.DROP_SCHEMA, dropSchemaExecutor.getSqlKind());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenGetPrefixFailed(VertxTestContext testContext) {
        //arrange
        val getPrefixError = "get prefix error";
        when(adgHelperTableNamesFactory.getTablePrefix(ENV, DATAMART)).thenThrow(new DtmException(getPrefixError));

        //act
        dropSchemaExecutor.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals(getPrefixError, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenDeleteSpaceFailed(VertxTestContext testContext) {
        //arrange
        val deleteSpaceError = "delete space error";
        when(adgHelperTableNamesFactory.getTablePrefix(ENV, DATAMART)).thenReturn(PREFIX);
        when(cartridgeClient.executeDeleteSpacesWithPrefixQueued(any())).thenReturn(Future.failedFuture(new DtmException(deleteSpaceError)));

        //act
        dropSchemaExecutor.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals(deleteSpaceError, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenRegister() {
        //act
        dropSchemaExecutor.register(ddlService);

        //assert
        verify(ddlService).addExecutor(dropSchemaExecutor);
    }
}
