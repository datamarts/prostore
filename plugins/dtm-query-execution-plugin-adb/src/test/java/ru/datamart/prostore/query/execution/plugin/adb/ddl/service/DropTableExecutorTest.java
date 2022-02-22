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
package ru.datamart.prostore.query.execution.plugin.adb.ddl.service;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.DdlSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

class DropTableExecutorTest {

    private final DatabaseExecutor adbQueryExecutor = mock(DatabaseExecutor.class);
    private final DdlSqlFactory ddlSqlFactory = new DdlSqlFactory();
    private final DropTableExecutor dropTableExecutor = new DropTableExecutor(adbQueryExecutor, ddlSqlFactory);

    private static final SqlKind EXPECTED_SQL_KIND = SqlKind.DROP_TABLE;
    private static final String EXPECTED_DROP_ACTUAL_SQL = "DROP TABLE IF EXISTS datamart.test_actual;";
    private static final String EXPECTED_DROP_HISTORY_SQL = "DROP TABLE IF EXISTS datamart.test_history;";
    private static final String EXPECTED_DROP_STAGING_SQL = "DROP TABLE IF EXISTS datamart.test_staging;";

    private static final String ENV = "env";
    private static final String SCHEMA = "datamart";

    private DdlRequest request;
    private static Entity entity;

    @BeforeAll
    static void setUpEntity() {
        entity = Entity.builder()
                .schema(SCHEMA)
                .name("test")
                .build();
    }

    @BeforeEach
    void setUp() {
        when(adbQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
        request = new DdlRequest(UUID.randomUUID(), ENV, SCHEMA, entity, null);
    }

    @Test
    void shouldExecuteWithAllDropTableScripts() {
        dropTableExecutor.execute(request)
                .onComplete(ar -> assertTrue(ar.succeeded()));
        verify(adbQueryExecutor).executeUpdate(argThat(this::hasAllDropTableScripts));
    }

    @Test
    void shouldReturnCorrectSqlKind() {
        assertEquals(EXPECTED_SQL_KIND, dropTableExecutor.getSqlKind());
    }

    private boolean hasAllDropTableScripts(String actualSql) {
        return actualSql.contains(EXPECTED_DROP_ACTUAL_SQL)
                && actualSql.contains(EXPECTED_DROP_HISTORY_SQL)
                && actualSql.contains(EXPECTED_DROP_STAGING_SQL);
    }
}