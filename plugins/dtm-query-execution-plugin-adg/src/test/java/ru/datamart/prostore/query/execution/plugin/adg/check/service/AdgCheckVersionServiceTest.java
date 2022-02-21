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
package ru.datamart.prostore.query.execution.plugin.adg.check.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckVersionRequest;
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
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdgCheckVersionServiceTest {

    @Mock
    private AdgCartridgeClient adgCartridgeClient;

    @InjectMocks
    private AdgCheckVersionService adgCheckVersionService;

    private final CheckVersionRequest request = new CheckVersionRequest(null, null, null);

    @Test
    void shouldSuccessWhenCheckVersion(VertxTestContext testContext) {
        //arrange
        when(adgCartridgeClient.getCheckVersions()).thenReturn(Future.succeededFuture());

        //act
        adgCheckVersionService.checkVersion(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void shouldFailedWhenCheckVersionFail(VertxTestContext testContext) {
        //arrange
        val error = "check version error";
        when(adgCartridgeClient.getCheckVersions()).thenReturn(Future.failedFuture(new DtmException(error)));

        //act
        adgCheckVersionService.checkVersion(request)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals(error, ar.cause().getMessage());
                }).completeNow());
    }
}
