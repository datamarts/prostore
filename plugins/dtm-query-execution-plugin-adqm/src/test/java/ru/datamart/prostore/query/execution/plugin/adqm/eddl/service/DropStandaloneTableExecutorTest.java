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
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adqm.eddl.factory.AdqmStandaloneQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class DropStandaloneTableExecutorTest {

    private static final String DTM_TBL = "dtm.tbl";
    public static final EddlRequest REQUEST = EddlRequest.builder()
            .entity(Entity.builder()
                    .externalTableLocationPath(DTM_TBL)
                    .build())
            .build();
    private static final String DROP_DISTRIBUTED_SQL = "DROP DISTRIBUTED TABLE";
    private static final String DROP_SHARD_SQL = "DROP SHARD TABLE";

    @Mock
    private DatabaseExecutor databaseExecutor;

    @Mock
    private AdqmStandaloneQueriesFactory queriesFactory;

    @InjectMocks
    private DropStandaloneTableExecutor dropTableExecutor;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    @Test
    void shouldSucceed(VertxTestContext testContext) {
        //arrange
        when(queriesFactory.dropDistributedTable(anyString())).thenReturn(DROP_DISTRIBUTED_SQL);
        when(queriesFactory.dropShardTable(anyString())).thenReturn(DROP_SHARD_SQL);
        when(databaseExecutor.executeUpdate(sqlCaptor.capture()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.succeededFuture());

        //act
        dropTableExecutor.execute(REQUEST)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    val sqls = sqlCaptor.getAllValues();
                    assertTrue(sqls.containsAll(Arrays.asList(DROP_DISTRIBUTED_SQL, DROP_SHARD_SQL)));
                }).completeNow());
    }

    @Test
    void shouldFailedWhenDatabaseExecuteFailed(VertxTestContext testContext) {
        //arrange
        when(queriesFactory.dropDistributedTable(anyString())).thenReturn(DROP_DISTRIBUTED_SQL);
        when(queriesFactory.dropShardTable(anyString())).thenReturn(DROP_SHARD_SQL);
        when(databaseExecutor.executeUpdate(sqlCaptor.capture())).thenReturn(Future.failedFuture("error"));

        //act
        dropTableExecutor.execute(REQUEST)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(DROP_DISTRIBUTED_SQL, sqlCaptor.getValue());
                }).completeNow());
    }

}
