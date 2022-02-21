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
package ru.datamart.prostore.query.execution.plugin.adp.ddl;

import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTableEntity;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTables;
import ru.datamart.prostore.query.execution.plugin.adp.base.factory.metadata.AdpTableEntitiesFactory;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.ddl.factory.AdpCreateTableQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.adp.ddl.service.CreateTableExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.ddl.service.DropTableExecutor;
import ru.datamart.prostore.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.api.factory.TableEntitiesFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;
import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.createAllTypesTable;
import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.createDdlRequest;

class CreateTableExecutorTest {
    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final DropTableExecutor dropTableExecutor = mock(DropTableExecutor.class);
    private final TableEntitiesFactory<AdpTables<AdpTableEntity>> tableEntitiesFactory = new AdpTableEntitiesFactory();
    private final CreateTableQueriesFactory<AdpTables<String>> queriesFactory = new AdpCreateTableQueriesFactory(tableEntitiesFactory);
    private final CreateTableExecutor createTableExecutor = new CreateTableExecutor(databaseExecutor, dropTableExecutor, queriesFactory);

    @BeforeEach
    void setUp() {
        when(dropTableExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
    }

    @Test
    void testCreateTable() {
        val expectedSql = "CREATE TABLE datamart.table_actual (id int8 NOT NULL, varchar_col varchar(10), char_col varchar(10), bigint_col int8, int_col int8, int32_col int4, double_col float8, float_col float4, date_col date, time_col time(6), timestamp_col timestamp(6), boolean_col bool, uuid_col varchar(36), link_col varchar, sys_from int8, sys_to int8, sys_op int4, constraint pk_datamart_table_actual primary key (id, sys_from)); " +
                "CREATE TABLE datamart.table_history (id int8 NOT NULL, varchar_col varchar(10), char_col varchar(10), bigint_col int8, int_col int8, int32_col int4, double_col float8, float_col float4, date_col date, time_col time(6), timestamp_col timestamp(6), boolean_col bool, uuid_col varchar(36), link_col varchar, sys_from int8, sys_to int8, sys_op int4, constraint pk_datamart_table_history primary key (id, sys_from)); " +
                "CREATE TABLE datamart.table_staging (id int8, varchar_col varchar(10), char_col varchar(10), bigint_col int8, int_col int8, int32_col int4, double_col float8, float_col float4, date_col date, time_col time(6), timestamp_col timestamp(6), boolean_col bool, uuid_col varchar(36), link_col varchar, sys_from int8, sys_to int8, sys_op int4)";
        val request = createDdlRequest(createAllTypesTable());

        createTableExecutor.execute(request)
                .onComplete(ar -> assertTrue(ar.succeeded()));

        verify(databaseExecutor).executeUpdate(argThat(input -> input.equals(expectedSql)));
        verify(dropTableExecutor).execute(any());
    }
}
