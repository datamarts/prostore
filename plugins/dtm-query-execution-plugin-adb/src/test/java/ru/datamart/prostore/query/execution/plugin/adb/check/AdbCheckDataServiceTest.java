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
package ru.datamart.prostore.query.execution.plugin.adb.check;

import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adb.check.factory.impl.AdbCheckDataQueryFactory;
import ru.datamart.prostore.query.execution.plugin.adb.check.service.AdbCheckDataService;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.impl.AdbQueryExecutor;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class AdbCheckDataServiceTest {
    private final static Long RESULT = 1L;
    private final AdbQueryExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
    private final AdbCheckDataService adbCheckDataService = new AdbCheckDataService(
            new AdbCheckDataQueryFactory(), adbQueryExecutor);

    @BeforeEach
    void setUp() {
        when(adbQueryExecutor.executeUpdate(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void testCheckByHash() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("hash_sum", RESULT);
        when(adbQueryExecutor.execute(any(), any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));

        CheckDataByHashInt32Request request = CheckDataByHashInt32Request.builder()
                .cnFrom(1L)
                .cnTo(2L)
                .columns(Collections.emptySet())
                .entity(Entity.builder()
                        .fields(Collections.emptyList())
                        .build())
                .build();
        adbCheckDataService.checkDataByHashInt32(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adbQueryExecutor, times(1)).execute(any(), any());
                });
    }

    @Test
    void testCheckSnapshotByHash() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("hash_sum", RESULT);
        when(adbQueryExecutor.execute(any(), any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));

        CheckDataByHashInt32Request request = CheckDataByHashInt32Request.builder()
                .cnFrom(1L)
                .cnTo(2L)
                .columns(Collections.emptySet())
                .entity(Entity.builder()
                        .fields(Collections.emptyList())
                        .build())
                .build();
        adbCheckDataService.checkDataSnapshotByHashInt32(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adbQueryExecutor, times(1)).execute(any(), any());
                });
    }

    @Test
    void testCheckByHashNullResult() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("hash_sum", null);
        when(adbQueryExecutor.execute(any(), any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));

        CheckDataByHashInt32Request request = CheckDataByHashInt32Request.builder()
                .cnFrom(1L)
                .cnTo(2L)
                .columns(Collections.emptySet())
                .entity(Entity.builder()
                        .fields(Collections.emptyList())
                        .build())
                .build();
        adbCheckDataService.checkDataByHashInt32(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(0L, ar.result());
                    verify(adbQueryExecutor, times(1)).execute(any(), any());
                });
    }

    @Test
    void testCheckByCount() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("cnt", RESULT);
        when(adbQueryExecutor.execute(any(), any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));

        CheckDataByCountRequest request = CheckDataByCountRequest.builder()
                .cnFrom(1L)
                .cnTo(2L)
                .entity(Entity.builder().build())
                .build();
        adbCheckDataService.checkDataByCount(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(RESULT, ar.result());
                    verify(adbQueryExecutor, times(1)).execute(any(), any());
                });
    }
}
