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
package ru.datamart.prostore.query.execution.core.check;

import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckCall;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.service.CheckExecutor;
import ru.datamart.prostore.query.execution.core.check.service.CheckService;
import ru.datamart.prostore.query.execution.core.check.service.impl.*;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CheckServiceTest {
    private final List<CheckExecutor> executors = Arrays.asList(mock(CheckDatabaseExecutor.class),
            mock(CheckTableExecutor.class), mock(CheckDataExecutor.class), mock(CheckMaterializedViewExecutor.class),
            mock(CheckVersionsExecutor.class), mock(CheckSumExecutor.class), mock(GetChangesExecutor.class),
            mock(GetEntityDdlExecutor.class));
    private final CheckService checkService = new CheckService();
    private final SqlCheckCall sqlCheckCall = mock(SqlCheckCall.class);

    @BeforeEach
    void setUp() {
        executors.forEach(checkExecutor -> {
            when(checkExecutor.getType()).thenCallRealMethod();
            when(checkExecutor.execute(any())).thenReturn(Future.succeededFuture(QueryResult.emptyResult()));
            checkService.addExecutor(checkExecutor);
        });


    }

    @Test
    void testCheckDatabase() {
        checkExecutor(CheckType.DATABASE);
    }

    @Test
    void testCheckTable() {
        checkExecutor(CheckType.TABLE);
    }

    @Test
    void testCheckData() {
        checkExecutor(CheckType.DATA);
    }

    @Test
    void testEmptyDatamartErrorPresenceOrAbser() {
        // arrange 1
        val expectedToWorkWithoutDatamart = EnumSet.of(CheckType.MATERIALIZED_VIEW,
                CheckType.VERSIONS,
                CheckType.CHANGES,
                CheckType.ENTITY_DDL);
        val datamartRequest = new DatamartRequest(new QueryRequest());

        for (val checkType : CheckType.values()) {
            // arrange 2
            val checkContext = new CheckContext(new RequestMetrics(), "env", datamartRequest,
                    checkType, sqlCheckCall);

            // act assert
            checkService.execute(checkContext).onComplete(ar -> {
                if (expectedToWorkWithoutDatamart.contains(checkType)) {
                    assertTrue(ar.succeeded());
                } else {
                    assertTrue(ar.failed());
                }
            });
        }
    }

    private void checkExecutor(CheckType type) {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("schema");
        DatamartRequest datamartRequest = new DatamartRequest(queryRequest);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env", datamartRequest,
                type, sqlCheckCall);

        checkService.execute(checkContext).onComplete(ar -> {
            assertTrue(ar.succeeded());
            //FIXME
            // assertEquals(RESULT, ar.result().getResult().get(0).get(CheckService.CHECK_RESULT_COLUMN_NAME));
        });
        executors.stream()
                .filter(checkExecutor -> checkExecutor.getType().equals(type))
                .findFirst()
                .ifPresent(checkExecutor -> verify(checkExecutor, times(1)).execute(checkContext));
        executors.stream()
                .filter(checkExecutor -> !checkExecutor.getType().equals(type))
                .forEach(checkExecutor -> verify(checkExecutor, never()).execute(any()));

    }
}
