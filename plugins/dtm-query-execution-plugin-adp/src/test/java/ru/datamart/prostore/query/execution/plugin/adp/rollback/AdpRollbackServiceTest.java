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
package ru.datamart.prostore.query.execution.plugin.adp.rollback;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.rollback.service.AdpRollbackService;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AdpRollbackServiceTest {

    private static final String TABLE = "table";
    private static final String DATAMART = "dtm";

    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final AdpRollbackService adpRollbackService = new AdpRollbackService(databaseExecutor);

    private final RollbackRequest request = RollbackRequest.builder()
            .datamartMnemonic(DATAMART)
            .entity(Entity.builder()
                    .name(TABLE)
                    .build())
            .sysCn(1L)
            .build();

    @Test
    void rollbackSuccess() {
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());

        adpRollbackService.execute(request)
                .onComplete(ar -> assertTrue(ar.succeeded()));
    }

    @Test
    void rollbackFail() {
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.failedFuture("error"));

        adpRollbackService.execute(request)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertEquals("error", ar.cause().getMessage());
                });
    }
}
