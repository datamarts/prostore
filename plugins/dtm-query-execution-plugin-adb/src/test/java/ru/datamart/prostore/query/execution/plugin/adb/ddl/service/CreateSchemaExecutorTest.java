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
package ru.datamart.prostore.query.execution.plugin.adb.ddl.service;

import ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.DdlSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class CreateSchemaExecutorTest {

    private final DatabaseExecutor adbQueryExecutor = mock(DatabaseExecutor.class);
    private final DdlSqlFactory ddlSqlFactory = new DdlSqlFactory();
    private final CreateSchemaExecutor createSchemaExecutor = new CreateSchemaExecutor(adbQueryExecutor, ddlSqlFactory);

    private static final SqlKind EXPECTED_SQL_KIND = SqlKind.CREATE_SCHEMA;
    private static final String EXPECTED_CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS datamart";

    private DdlRequest request;

    private static final String ENV = "env";
    private static final String SCHEMA = "datamart";

    @BeforeEach
    void setUp() {
        when(adbQueryExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
        request = new DdlRequest(UUID.randomUUID(), ENV, SCHEMA, null, null);
    }

    @Test
    void shouldExecuteWithCreateSchemaScript() {
        createSchemaExecutor.execute(request)
                .onComplete(ar -> assertTrue(ar.succeeded()));
        verify(adbQueryExecutor).executeUpdate(argThat(actualSql -> actualSql.equalsIgnoreCase(EXPECTED_CREATE_SCHEMA_SQL)));
    }

    @Test
    void shouldReturnCorrectSqlKind() {
        assertEquals(EXPECTED_SQL_KIND, createSchemaExecutor.getSqlKind());
    }

}