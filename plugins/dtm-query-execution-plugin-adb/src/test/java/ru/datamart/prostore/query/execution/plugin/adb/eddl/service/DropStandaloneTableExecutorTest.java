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
package ru.datamart.prostore.query.execution.plugin.adb.eddl.service;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class DropStandaloneTableExecutorTest {

    private static final String DTM_TBL = "dtm.tbl";
    public static final EddlRequest REQUEST = EddlRequest.builder()
            .entity(Entity.builder()
                    .externalTableLocationPath(DTM_TBL)
                    .build())
            .build();
    private static final String DROP_SQL = "DROP TABLE " + DTM_TBL;

    @Mock
    private DatabaseExecutor databaseExecutor;

    @InjectMocks
    private DropStandaloneTableExecutor dropTableExecutor;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    @Test
    void shouldSucceed(VertxTestContext testContext) {
        //arrange
        when(databaseExecutor.executeUpdate(sqlCaptor.capture())).thenReturn(Future.succeededFuture());

        //act
        dropTableExecutor.execute(REQUEST)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(DROP_SQL, sqlCaptor.getValue());
                })
                .completeNow());
    }

    @Test
    void shouldFailed(VertxTestContext testContext) {
        //arrange
        when(databaseExecutor.executeUpdate(sqlCaptor.capture())).thenReturn(Future.failedFuture("error"));

        //act
        dropTableExecutor.execute(REQUEST)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertEquals(DROP_SQL, sqlCaptor.getValue());
                }).completeNow());
    }

}