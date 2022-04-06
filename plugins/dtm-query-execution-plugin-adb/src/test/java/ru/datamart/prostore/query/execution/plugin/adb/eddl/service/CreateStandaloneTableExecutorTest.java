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
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static utils.CreateEntityUtils.createEddlRequest;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class CreateStandaloneTableExecutorTest {
    private static EddlRequest eddlRequest;

    @Mock
    private DatabaseExecutor adpQueryExecutor;

    @InjectMocks
    private CreateStandaloneTableExecutor executor;

    @BeforeAll
    static void setUp() {
        eddlRequest = createEddlRequest(true);
    }

    @Test
    void shouldSuccess(VertxTestContext testContext) {
        //arrange
        when(adpQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
        val expectedQuery = "CREATE TABLE table_path \n" +
                "(id int8 NOT NULL, sk_key2 int8 NOT NULL, pk2 int8 NOT NULL, " +
                "sk_key3 int8 NOT NULL, VARCHAR_type varchar(20) , CHAR_type varchar(20) ," +
                " BIGINT_type int8 , INT_type int8 , INT32_type int4 , " +
                "DOUBLE_type float8 , FLOAT_type float4 , DATE_type date , TIME_type time(6) , " +
                "TIMESTAMP_type timestamp(6) , BOOLEAN_type bool , UUID_type varchar(36) , " +
                "LINK_type varchar ,\n" +
                " PRIMARY KEY (id, pk2)) \n" +
                " DISTRIBUTED BY (id, sk_key2, sk_key3)";

        //act
        executor.execute(eddlRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    verify(adpQueryExecutor).executeUpdate(argThat(actualSql -> actualSql.equalsIgnoreCase(expectedQuery)));
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