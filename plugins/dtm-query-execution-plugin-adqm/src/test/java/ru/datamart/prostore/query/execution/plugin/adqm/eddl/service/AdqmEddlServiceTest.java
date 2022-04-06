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
package ru.datamart.prostore.query.execution.plugin.adqm.eddl.service;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;
import static ru.datamart.prostore.query.execution.plugin.adqm.utils.StandaloneTestUtils.createEddlRequest;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdqmEddlServiceTest {
    private static EddlRequest createEddlRequest;
    private static EddlRequest dropEddlRequest;

    @Mock
    private CreateStandaloneTableExecutor createExecutor;

    @Mock
    private DropStandaloneTableExecutor dropExecutor;

    @InjectMocks
    private AdqmEddlService service;

    @BeforeAll
    static void setUp() {
        createEddlRequest = createEddlRequest(true);
        dropEddlRequest = createEddlRequest(false);
    }

    @Test
    void shouldSuccessWhenCreateEddl(VertxTestContext testContext) {
        //arrange
        when(createExecutor.execute(createEddlRequest)).thenReturn(Future.succeededFuture());

        //act
        service.execute(createEddlRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    verify(createExecutor).execute(createEddlRequest);
                    verifyNoInteractions(dropExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWhenCreateEddl(VertxTestContext testContext) {
        //arrange
        when(createExecutor.execute(createEddlRequest)).thenReturn(Future.failedFuture("error"));

        //act
        service.execute(createEddlRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    verify(createExecutor).execute(createEddlRequest);
                    verifyNoInteractions(dropExecutor);
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenDropEddl(VertxTestContext testContext) {
        //arrange
        when(dropExecutor.execute(dropEddlRequest)).thenReturn(Future.succeededFuture());

        //act
        service.execute(dropEddlRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    verify(dropExecutor).execute(dropEddlRequest);
                    verifyNoInteractions(createExecutor);
                }).completeNow());
    }

    @Test
    void shouldFailWhenDropEddl(VertxTestContext testContext) {
        //arrange
        when(dropExecutor.execute(dropEddlRequest)).thenReturn(Future.failedFuture("error"));

        //act
        service.execute(dropEddlRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    verify(dropExecutor).execute(dropEddlRequest);
                    verifyNoInteractions(createExecutor);
                }).completeNow());
    }
}