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
package io.arenadata.dtm.query.execution.core.config;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigType;
import io.arenadata.dtm.query.execution.core.config.service.ConfigService;
import io.arenadata.dtm.query.execution.core.config.dto.ConfigRequestContext;
import io.arenadata.dtm.query.execution.core.config.service.impl.ConfigStorageAddDdlExecutor;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConfigServiceTest {
    private final ConfigStorageAddDdlExecutor configStorageAddDdlExecutor = mock(ConfigStorageAddDdlExecutor.class);
    private final ConfigService configService = new ConfigService();

    @BeforeEach
    void init() {
        when(configStorageAddDdlExecutor.getConfigType()).thenCallRealMethod();
        when(configStorageAddDdlExecutor.execute(any())).thenReturn(Future.succeededFuture(QueryResult.emptyResult()));
        configService.addExecutor(configStorageAddDdlExecutor);
    }

    @Test
    void testConfigStorageAdd() {
        SqlConfigCall sqlConfigCall = mock(SqlConfigCall.class);
        when(sqlConfigCall.getSqlConfigType()).thenReturn(SqlConfigType.CONFIG_STORAGE_ADD);
        ConfigRequestContext context = ConfigRequestContext.builder()
                .sqlConfigCall(sqlConfigCall)
                .build();
        configService.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(QueryResult.emptyResult(), ar.result());
                });
    }

    @Test
    void testNotExistExecutor() {
        SqlConfigCall sqlConfigCall = mock(SqlConfigCall.class);
        when(sqlConfigCall.getSqlConfigType()).thenReturn(null);
        ConfigRequestContext context = ConfigRequestContext.builder()
                .sqlConfigCall(sqlConfigCall)
                .build();
        configService.execute(context).onComplete(ar -> {
            assertTrue(ar.failed());
            assertTrue(ar.cause() instanceof DtmException);
        });
    }
}
