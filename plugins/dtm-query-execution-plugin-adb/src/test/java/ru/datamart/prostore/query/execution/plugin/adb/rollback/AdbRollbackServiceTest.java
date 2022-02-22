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
package ru.datamart.prostore.query.execution.plugin.adb.rollback;

import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.query.execution.plugin.adb.rollback.dto.AdbRollbackRequest;
import ru.datamart.prostore.query.execution.plugin.adb.rollback.factory.AdbRollbackRequestFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.impl.AdbQueryExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.rollback.service.AdbRollbackService;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.factory.RollbackRequestFactory;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdbRollbackServiceTest {

    private final RollbackRequestFactory<AdbRollbackRequest> rollbackRequestFactory = mock(AdbRollbackRequestFactory.class);
    private final AdbQueryExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
    private AdbRollbackService adbRollbackService = new AdbRollbackService(rollbackRequestFactory, adbQueryExecutor);

    @Test
    void executeSuccess() {
        Promise promise = Promise.promise();
        RollbackRequest rollbackRequest = RollbackRequest.builder().build();
        AdbRollbackRequest sqlList = new AdbRollbackRequest(
                PreparedStatementRequest.onlySql("truncateSql"),
                PreparedStatementRequest.onlySql("deleteFromActualSql"),
                Arrays.asList(PreparedStatementRequest.onlySql("eraseSql"))
        );
        when(rollbackRequestFactory.create(any())).thenReturn(sqlList);
        Map<String, Integer> execCount = new HashMap<>();
        sqlList.getStatements().forEach(request -> execCount.put(request.getSql(), 0));

        doAnswer(invocation -> {
            final String sql = invocation.getArgument(0);
            execCount.put(sql, 1);
            return Future.succeededFuture();
        }).when(adbQueryExecutor).executeUpdate(any());

        doAnswer(invocation -> {
            final List<PreparedStatementRequest> requests = invocation.getArgument(0);
            requests.forEach(r -> execCount.put(r.getSql(), 1));
            return Future.succeededFuture();
        }).when(adbQueryExecutor).executeInTransaction(any());

        adbRollbackService.execute(rollbackRequest).onComplete(promise);
        assertTrue(promise.future().succeeded());
        sqlList.getStatements().forEach(statement -> assertEquals(execCount.get(statement.getSql()), 1));
    }

    @Test
    void executeError() {
        Promise promise = Promise.promise();
        RollbackRequest rollbackRequest = RollbackRequest.builder().build();

        when(rollbackRequestFactory.create(any())).thenReturn(new AdbRollbackRequest(
                PreparedStatementRequest.onlySql("truncateSql"),
                PreparedStatementRequest.onlySql("deleteFromActualSql"),
                Arrays.asList(PreparedStatementRequest.onlySql("eraseSql"))
        ));

        when(adbQueryExecutor.executeUpdate(any())).thenReturn(Future.failedFuture(new DataSourceException("")));

        when(adbQueryExecutor.executeInTransaction(any())).thenReturn(Future.failedFuture(new DataSourceException("")));

        adbRollbackService.execute(rollbackRequest).onComplete(promise);
        assertTrue(promise.future().failed());
    }
}
