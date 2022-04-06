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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.factory.ShardingExpr;
import ru.datamart.prostore.query.execution.plugin.adqm.eddl.factory.AdqmStandaloneQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;
import static ru.datamart.prostore.query.execution.plugin.adqm.utils.StandaloneTestUtils.CLUSTER;
import static ru.datamart.prostore.query.execution.plugin.adqm.utils.StandaloneTestUtils.createEddlRequest;

@ExtendWith(VertxExtension.class)
class CreateStandaloneTableExecutorTest {
    private static EddlRequest eddlRequest;
    private final DatabaseExecutor adqmQueryExecutor = mock(DatabaseExecutor.class);
    private static final DdlProperties ddlProperties = mock(DdlProperties.class);
    private final AdqmStandaloneQueriesFactory factory = new AdqmStandaloneQueriesFactory(ddlProperties);

    private final CreateStandaloneTableExecutor executor = new CreateStandaloneTableExecutor(adqmQueryExecutor, factory);

    @BeforeAll
    static void setUp() {
        when(ddlProperties.getCluster()).thenReturn(CLUSTER);
        when(ddlProperties.getShardingKeyExpr()).thenReturn(ShardingExpr.CITY_HASH_64);
        eddlRequest = createEddlRequest(true);
    }

    @Test
    void shouldSuccess(VertxTestContext testContext) {
        //arrange
        val expectedShardQuery = "CREATE TABLE dbName.table_path_shard ON CLUSTER cluster\n" +
                "(ID Int64, DESCRIPTION String, FOREIGN_KEY Int64, TIME_COL Nullable(Int64))\n" +
                "ENGINE = MergeTree()\n" +
                "ORDER BY (ID)";
        val expectedDistributedQuery = "CREATE TABLE dbName.table_path ON CLUSTER cluster AS dbName.table_path_shard \n" +
                "ENGINE = Distributed(cluster, dbName, table_path_shard, cityHash64(ID))";
        when(adqmQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());

        //act
        executor.execute(eddlRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    verify(adqmQueryExecutor).executeUpdate(argThat(actualSql -> actualSql.equalsIgnoreCase(expectedShardQuery)));
                    verify(adqmQueryExecutor).executeUpdate(argThat(actualSql -> actualSql.equalsIgnoreCase(expectedDistributedQuery)));
                }).completeNow());
    }

    @Test
    void shouldFail(VertxTestContext testContext) {
        //arrange
        when(adqmQueryExecutor.executeUpdate(anyString())).thenReturn(Future.failedFuture(new DtmException("error")));

        //act
        executor.execute(eddlRequest)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                }).completeNow());
    }
}