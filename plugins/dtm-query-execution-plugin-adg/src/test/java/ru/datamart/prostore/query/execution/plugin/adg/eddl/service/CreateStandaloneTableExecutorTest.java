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
package ru.datamart.prostore.query.execution.plugin.adg.eddl.service;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.eddl.factory.AdgStandaloneQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.datamart.prostore.query.execution.plugin.adg.utils.TestUtils.createEddlRequest;
import static ru.datamart.prostore.query.execution.plugin.adg.utils.TestUtils.createEddlRequestWithBucket;

@ExtendWith(VertxExtension.class)
class CreateStandaloneTableExecutorTest {
    private static EddlRequest eddlRequestWithoutBucketField;
    private static EddlRequest eddlRequestWithBucketField;
    private final AdgCartridgeClient client = mock(AdgCartridgeClient.class);
    private final AdgStandaloneQueriesFactory createStandaloneFactory = new AdgStandaloneQueriesFactory(new TarantoolDatabaseProperties());

    private final CreateStandaloneTableExecutor executor = new CreateStandaloneTableExecutor(client, createStandaloneFactory);

    @BeforeAll
    static void setUp() {
        eddlRequestWithoutBucketField = createEddlRequest();
        eddlRequestWithBucketField = createEddlRequestWithBucket();
    }

    @Test
    void shouldSuccess(VertxTestContext testContext) {
        when(client.executeCreateSpacesQueued(any())).thenReturn(Future.succeededFuture());
        executor.execute(eddlRequestWithBucketField)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void shouldFailWhenBucketFieldDoesNotExists(VertxTestContext testContext) {
        executor.execute(eddlRequestWithoutBucketField)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertEquals("bucket_id not found", ar.cause().getMessage());
                }).completeNow());
    }
}