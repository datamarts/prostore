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
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;
import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.createEddlRequest;

@ExtendWith(VertxExtension.class)
class CreateStandaloneTableExecutorTest {
    private static EddlRequest eddlRequest;
    private final DatabaseExecutor adpQueryExecutor = mock(DatabaseExecutor.class);

    private final CreateStandaloneTableExecutor executor = new CreateStandaloneTableExecutor(adpQueryExecutor);

    @BeforeAll
    static void setUp() {
        eddlRequest = createEddlRequest(true);
    }

    @Test
    void shouldSuccess(VertxTestContext testContext) {
        //arrange
        when(adpQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
        val expectedCreateStandaloneQuery = "CREATE TABLE table_path " +
                "(id int8 NOT NULL, varchar_col varchar(10) , char_col varchar(10) , " +
                "bigint_col int8 , int_col int8 , int32_col int4 , double_col float8 , " +
                "float_col float4 , date_col date , time_col time(6) , timestamp_col timestamp(6) , " +
                "boolean_col bool , uuid_col varchar(36) , link_col varchar , " +
                "PRIMARY KEY (id))";

        //act
        executor.execute(eddlRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    verify(adpQueryExecutor).executeUpdate(argThat(actualSql -> actualSql.equalsIgnoreCase(expectedCreateStandaloneQuery)));
                }).completeNow());
    }

    @Test
    void shouldFail(VertxTestContext testContext) {
        //arrange
        when(adpQueryExecutor.executeUpdate(anyString())).thenReturn(Future.failedFuture(new DtmException("error")));

        //act
        executor.execute(eddlRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                }).completeNow());
    }
}