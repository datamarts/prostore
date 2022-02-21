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

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.plugin.adb.base.dto.metadata.AdbTableEntity;
import ru.datamart.prostore.query.execution.plugin.adb.base.dto.metadata.AdbTables;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.metadata.AdbTableEntitiesFactory;
import ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.DdlSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.impl.AdbCreateTableQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.api.factory.TableEntitiesFactory;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class CreateTableExecutorTest {
    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final DropTableExecutor dropTableExecutor = mock(DropTableExecutor.class);
    private final DdlSqlFactory ddlSqlFactory = new DdlSqlFactory();
    private final TableEntitiesFactory<AdbTables<AdbTableEntity>> tableEntitiesFactory = new AdbTableEntitiesFactory();
    private final CreateTableQueriesFactory<AdbTables<String>> createTableQueriesFactory = new AdbCreateTableQueriesFactory(tableEntitiesFactory);

    private final CreateTableExecutor createTableExecutor = new CreateTableExecutor(databaseExecutor, ddlSqlFactory, dropTableExecutor, createTableQueriesFactory);
    private DdlRequest ddlRequest;

    private static final SqlKind EXPECTED_SQL_KIND = SqlKind.CREATE_TABLE;
    private static final String CREATE_ACTUAL_TABLE_SQL = "CREATE TABLE datamart.test_entity_actual (" +
            "id int8 NOT NULL, " +
            "name varchar(50) NOT NULL, " +
            "sys_from int8, " +
            "sys_to int8, " +
            "sys_op int4, " +
            "constraint pk_datamart_test_entity_actual primary key (id, sys_from)) DISTRIBUTED BY (id);";
    private static final String CREATE_HISTORY_TABLE_SQL = "CREATE TABLE datamart.test_entity_history (" +
            "id int8 NOT NULL, " +
            "name varchar(50) NOT NULL, " +
            "sys_from int8, " +
            "sys_to int8, " +
            "sys_op int4, " +
            "constraint pk_datamart_test_entity_history primary key (id, sys_from)) DISTRIBUTED BY (id);";
    private static final String CREATE_STAGING_TABLE_SQL = "CREATE TABLE datamart.test_entity_staging (" +
            "id int8, " +
            "name varchar(50), " +
            "sys_from int8, " +
            "sys_to int8, " +
            "sys_op int4) DISTRIBUTED BY (id)";
    private static final String CREATE_INDEX_FOR_ACTUAL_TABLE_SQL = "CREATE INDEX test_entity_actual_sys_from_idx ON datamart.test_entity_actual (sys_from);";
    private static final String CREATE_INDEX_FOR_HISTORY_SQL = "CREATE INDEX test_entity_history_sys_to_idx ON datamart.test_entity_history (sys_to, sys_op)";

    private static final String ENV = "env";
    private static final String SCHEMA = "datamart";
    private static final String ENTITY_NAME = "test_entity";


    @BeforeEach
    void setUp() {
        when(dropTableExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
        ddlRequest = new DdlRequest(UUID.randomUUID(), ENV, SCHEMA, createEntity(), null);
    }

    @Test
    void shouldExecuteWithAllCreateTableScripts() {
        createTableExecutor.execute(ddlRequest).onComplete(ar -> assertTrue(ar.succeeded()));
        verify(databaseExecutor).executeUpdate(argThat(this::hasAllCreateTableScripts));
    }

    @Test
    void shouldExecuteWithAllCreateIndexScripts() {
        createTableExecutor.execute(ddlRequest).onComplete(ar -> assertTrue(ar.succeeded()));
        verify(databaseExecutor).executeUpdate(argThat(this::hasAllCreateIndexScripts));
    }

    @Test
    void shouldReturnCorrectSqlKind() {
        assertEquals(EXPECTED_SQL_KIND, createTableExecutor.getSqlKind());
    }

    private boolean hasAllCreateTableScripts(String input) {
        return input.contains(CREATE_ACTUAL_TABLE_SQL)
                && input.contains(CREATE_HISTORY_TABLE_SQL)
                && input.contains(CREATE_STAGING_TABLE_SQL);
    }

    private boolean hasAllCreateIndexScripts(String input) {
        return input.contains(CREATE_INDEX_FOR_ACTUAL_TABLE_SQL)
                && input.contains(CREATE_INDEX_FOR_HISTORY_SQL);
    }

    private Entity createEntity() {
        val id = EntityField.builder()
                .ordinalPosition(0)
                .name("id")
                .type(ColumnType.BIGINT)
                .nullable(false)
                .primaryOrder(1)
                .shardingOrder(1)
                .build();
        val name = EntityField.builder()
                .ordinalPosition(1)
                .name("name")
                .type(ColumnType.VARCHAR)
                .size(50)
                .nullable(false)
                .build();
        val fieldsList = Lists.newArrayList(id, name);
        return Entity.builder()
                .schema(SCHEMA)
                .name(ENTITY_NAME)
                .entityType(EntityType.TABLE)
                .fields(fieldsList)
                .build();
    }

}