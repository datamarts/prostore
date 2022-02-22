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
package ru.datamart.prostore.query.execution.plugin.adb.init;

import ru.datamart.prostore.query.execution.plugin.adb.base.factory.hash.AdbHashFunctionFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.impl.AdbQueryExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.init.service.AdbInitializationService;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdbInitializationServiceTest {

    private final AdbQueryExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
    private final AdbHashFunctionFactory hashFunctionFactory = new AdbHashFunctionFactory();
    private final AdbInitializationService initializationService = new AdbInitializationService(adbQueryExecutor, hashFunctionFactory);

    @Test
    void executeSuccess() {
        when(adbQueryExecutor.executeUpdate(any()))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(adbQueryExecutor, times(1)).executeUpdate(any());
                });

    }

    @Test
    void executeQueryError() {
        when(adbQueryExecutor.executeUpdate(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("")));

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(adbQueryExecutor, times(1)).executeUpdate(any());
                });
    }
}