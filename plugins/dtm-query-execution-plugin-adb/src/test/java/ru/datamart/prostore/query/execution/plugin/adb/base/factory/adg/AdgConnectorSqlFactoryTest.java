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
package ru.datamart.prostore.query.execution.plugin.adb.base.factory.adg;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adg.AdgSharedService;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedProperties;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class AdgConnectorSqlFactoryTest {

    public static final String TARANTOOL_SERVER = "tarantool_server";
    public static final String ENV = "env";
    public static final String DATAMART = "datamart";
    public static final String ENTITY_NAME = "entity_name";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final long CONNECT_TIMEOUT = 1234L;
    public static final long READ_TIMEOUT = 2345L;
    public static final long REQUEST_TIMEOUT = 3456L;
    public static final int BUFFER_SIZE = 4567;
    public static final String QUERY = "query";
    @Mock
    private AdgSharedService adgSharedService;

    @InjectMocks
    private AdgConnectorSqlFactory adgConnectorSqlFactory;

    @BeforeEach
    void setUp() {
        lenient().when(adgSharedService.getSharedProperties()).thenReturn(new AdgSharedProperties(TARANTOOL_SERVER, USER, PASSWORD, CONNECT_TIMEOUT, READ_TIMEOUT, REQUEST_TIMEOUT, BUFFER_SIZE));
    }

    @Test
    void shouldPrepareCreateExternalTable() {
        // arrange
        Entity build = getEntity();

        // act
        String sql = adgConnectorSqlFactory.createExternalTable(ENV, DATAMART, build);

        // assert
        Assertions.assertThat(sql).isEqualToNormalizingNewlines("CREATE WRITABLE EXTERNAL TABLE datamart.TARANTOOL_EXT_entity_name\n" +
                "(col_varchar varchar,col_char varchar,col_bigint int8,col_int int8,col_int32 int4,col_double float8,col_float float4,col_date int8,col_time int8,col_timestamp int8,col_boolean bool,col_uuid varchar,col_link varchar,sys_op int8,bucket_id int8) LOCATION ('pxf://env__datamart__entity_name_staging?PROFILE=tarantool-upsert&TARANTOOL_SERVER=tarantool_server&USER=user&PASSWORD=password&TIMEOUT_CONNECT=1234&TIMEOUT_READ=2345&TIMEOUT_REQUEST=3456&BUFFER_SIZE=4567')\n" +
                "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')");
    }

    @Test
    void shouldPrepareDropExternalTable() {
        // arrange
        Entity entity = getEntity();

        // act
        String sql = adgConnectorSqlFactory.dropExternalTable(DATAMART, entity);

        // assert
        assertEquals("DROP EXTERNAL TABLE IF EXISTS " + DATAMART + ".TARANTOOL_EXT_" + ENTITY_NAME, sql);
    }

    @Test
    void shouldPrepareInsertIntoExternalTable() {
        // arrange
        Entity entity = getEntity();

        // act
        String sql = adgConnectorSqlFactory.insertIntoExternalTable(DATAMART, entity, QUERY, false);

        // assert
        assertEquals("INSERT INTO " + DATAMART + ".TARANTOOL_EXT_" + ENTITY_NAME + " SELECT *, 0 FROM (" + QUERY + ") as __temp_tbl", sql);
    }

    @Test
    void shouldPrepareInsertIntoExternalTableWhenOnlyPk() {
        // arrange
        Entity entity = getEntity();

        // act
        String sql = adgConnectorSqlFactory.insertIntoExternalTable(DATAMART, entity, QUERY, true);

        // assert
        assertEquals("INSERT INTO " + DATAMART + ".TARANTOOL_EXT_" + ENTITY_NAME + " (col_varchar, col_char, col_bigint, col_int, col_int32, col_double, col_float, col_date, col_time, col_timestamp, col_boolean, col_uuid, col_link, sys_op) SELECT *, 1 FROM (" + QUERY + ") as __temp_tbl", sql);
    }

    private Entity getEntity() {
        List<EntityField> fields = new ArrayList<>();
        int pos = 0;
        for (ColumnType columnType : ColumnType.values()) {
            if (columnType == ColumnType.ANY || columnType == ColumnType.BLOB) continue;

            EntityField field = EntityField.builder()
                    .ordinalPosition(pos++)
                    .name("col_" + columnType.name().toLowerCase())
                    .type(columnType)
                    .primaryOrder(pos)
                    .nullable(true)
                    .build();
            switch (columnType) {
                case TIME:
                case TIMESTAMP:
                    field.setAccuracy(5);
                    break;
                case CHAR:
                case VARCHAR:
                    field.setSize(100);
                    break;
                case UUID:
                    field.setSize(36);
                    break;
            }

            fields.add(field);
        }

        return Entity.builder()
                .name(ENTITY_NAME)
                .fields(fields)
                .build();
    }
}