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

import ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.DdlSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

class DropSchemaExecutorTest {

    private final DatabaseExecutor adbQueryExecutor = mock(DatabaseExecutor.class);
    private final DdlSqlFactory ddlSqlFactory = new DdlSqlFactory();
    private final DropSchemaExecutor dropSchemaExecutor = new DropSchemaExecutor(adbQueryExecutor, ddlSqlFactory);

    private static final SqlKind EXPECTED_SQL_KIND = SqlKind.DROP_SCHEMA;
    private static final String EXPECTED_DROP_SCHEMA_SQL = "DROP SCHEMA IF EXISTS datamart CASCADE";

    private static final String ENV = "env";
    private static final String SCHEMA = "datamart";

    private DdlRequest request;

    @BeforeEach
    void setUp() {
        when(adbQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
        request = new DdlRequest(UUID.randomUUID(), ENV, SCHEMA, null, null);
    }

    @Test
    void shouldExecuteWithDropSchemaScript() {
        dropSchemaExecutor.execute(request)
                .onComplete(ar -> assertTrue(ar.succeeded()));
        verify(adbQueryExecutor).executeUpdate(argThat(actualSql -> actualSql.equalsIgnoreCase(EXPECTED_DROP_SCHEMA_SQL)));
    }

    @Test
    void shouldReturnCorrectSqlKind() {
        assertEquals(EXPECTED_SQL_KIND, dropSchemaExecutor.getSqlKind());
    }
}