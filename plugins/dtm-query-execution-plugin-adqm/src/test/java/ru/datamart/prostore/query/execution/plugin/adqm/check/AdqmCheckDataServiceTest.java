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
package ru.datamart.prostore.query.execution.plugin.adqm.check;

import io.vertx.core.Future;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adqm.check.factory.AdqmCheckDataQueryFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.check.service.AdqmCheckDataService;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdqmCheckDataServiceTest {
    private final static Long RESULT = 1L;
    private final DatabaseExecutor adqmQueryExecutor = mock(DatabaseExecutor.class);
    private final AdqmCheckDataQueryFactory queryFactory = new AdqmCheckDataQueryFactory();
    private final AdqmCheckDataService adqmCheckDataService = new AdqmCheckDataService(adqmQueryExecutor, queryFactory);

    @Test
    void testCheckByHash() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("sum", RESULT);
        when(adqmQueryExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));
        Entity entity = Entity.builder()
                .name("entity")
                .schema("schema")
                .fields(Collections.emptyList())
                .build();
        CheckDataByHashInt32Request request = CheckDataByHashInt32Request.builder()
                .envName("env")
                .datamart("schema")
                .columns(Collections.singleton("column"))
                .entity(entity)
                .cnFrom(1L)
                .cnTo(1L)
                .normalization(1L)
                .build();
        adqmCheckDataService.checkDataByHashInt32(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adqmQueryExecutor, times(1)).execute(any());
                });
    }

    @Test
    void testCheckSnapshotByHash() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("sum", RESULT);
        when(adqmQueryExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));
        Entity entity = Entity.builder()
                .name("entity")
                .schema("schema")
                .fields(Collections.emptyList())
                .build();
        CheckDataByHashInt32Request request = CheckDataByHashInt32Request.builder()
                .envName("env")
                .datamart("schema")
                .columns(Collections.singleton("column"))
                .entity(entity)
                .cnFrom(1L)
                .cnTo(1L)
                .normalization(1L)
                .build();
        adqmCheckDataService.checkDataSnapshotByHashInt32(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adqmQueryExecutor, times(1)).execute(any());
                });
    }

    @Test
    void testCheckByCount() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("count", RESULT);
        when(adqmQueryExecutor.execute(any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));
        CheckDataByCountRequest request = CheckDataByCountRequest.builder()
                .envName("env")
                .datamart("schema")
                .entity(Entity.builder().build())
                .cnFrom(1L)
                .cnTo(1L)
                .build();
        adqmCheckDataService.checkDataByCount(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adqmQueryExecutor, times(1)).execute(any());
                });
    }
}
