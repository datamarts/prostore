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
package ru.datamart.prostore.query.execution.plugin.adp.init;

import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.init.service.AdpInitializationService;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdpInitializationServiceTest {

    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final AdpInitializationService initializationService = new AdpInitializationService(databaseExecutor);

    @Test
    void executeSuccess() {
        when(databaseExecutor.executeUpdate(any()))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(databaseExecutor, times(1)).executeUpdate(any());
                });

    }

    @Test
    void executeQueryError() {
        when(databaseExecutor.executeUpdate(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("")));

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(databaseExecutor, times(1)).executeUpdate(any());
                });
    }
}