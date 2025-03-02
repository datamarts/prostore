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
package ru.datamart.prostore.query.execution.plugin.adp.eddl.service;

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
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.createEddlRequest;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class DropStandaloneTableExecutorTest {

    private static final EddlRequest REQUEST = createEddlRequest(false);
    private static final String EXPECTED_DROP_STANDALONE_QUERY = "DROP TABLE table_path";

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
                    assertEquals(EXPECTED_DROP_STANDALONE_QUERY, sqlCaptor.getValue());
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
                    assertEquals(EXPECTED_DROP_STANDALONE_QUERY, sqlCaptor.getValue());
                }).completeNow());
    }

}
