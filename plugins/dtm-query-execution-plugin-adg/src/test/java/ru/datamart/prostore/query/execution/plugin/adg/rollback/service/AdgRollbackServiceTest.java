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
package ru.datamart.prostore.query.execution.plugin.adg.rollback.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.rollback.dto.ReverseHistoryTransferRequest;
import ru.datamart.prostore.query.execution.plugin.adg.rollback.factory.ReverseHistoryTransferRequestFactory;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdgRollbackServiceTest {

    @Mock
    private ReverseHistoryTransferRequestFactory requestFactory;

    @Mock
    private AdgCartridgeClient cartridgeClient;

    @InjectMocks
    private AdgRollbackService adgRollbackService;

    private final RollbackRequest request = RollbackRequest.builder().build();
    private final ReverseHistoryTransferRequest reverseHistoryTransferRequest = new ReverseHistoryTransferRequest();

    @Test
    void shouldSuccessWhenExecute(VertxTestContext testContext) {
        //assert
        when(requestFactory.create(any())).thenReturn(reverseHistoryTransferRequest);
        when(cartridgeClient.reverseHistoryTransfer(any())).thenReturn(Future.succeededFuture());

        //act
        adgRollbackService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                })
                .completeNow());
    }

    @Test
    void shouldFailedWhenCatridgeReverseHistoryFail(VertxTestContext testContext) {
        //assert
        val error = "catridge reverse history error";
        when(requestFactory.create(any())).thenReturn(reverseHistoryTransferRequest);
        when(cartridgeClient.reverseHistoryTransfer(any())).thenReturn(Future.failedFuture(new DtmException(error)));

        //act
        adgRollbackService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals(error, ar.cause().getMessage());
                }).completeNow());
    }
}
