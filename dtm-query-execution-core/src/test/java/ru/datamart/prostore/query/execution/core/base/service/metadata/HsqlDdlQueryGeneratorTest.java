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
package ru.datamart.prostore.query.execution.core.base.service.metadata;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.core.base.service.metadata.query.HsqlDdlQueryGenerator;
import ru.datamart.prostore.query.execution.core.base.service.metadata.query.DdlQueryGenerator;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HsqlDdlQueryGeneratorTest {

    private DdlQueryGenerator ddlQueryGenerator = new HsqlDdlQueryGenerator();

    @Test
    void createTableTest() {
        Entity entity = Entity.builder()
                .name("test_table")
                .entityType(EntityType.TABLE)
                .schema("test_schema")
                .fields(createFields())
                .build();
        String expectedDdlQuery = "CREATE TABLE test_schema.test_table (" +
                "account_id BIGINT NOT NULL, " +
                "account_type VARCHAR(2) NOT NULL, " +
                "key INTEGER, " +
                "double_col DOUBLE, " +
                "boolean_col BOOLEAN, " +
                "int_col INTEGER, " +
                "date_col DATE, " +
                "timestamp_col TIMESTAMP(6), " +
                "timestamp_col_3 TIMESTAMP(3), " +
                "time_col TIME(6), " +
                "time_col_5 TIME(5), " +
                "uuid_col LONGVARCHAR, " +
                "char_col CHAR(1), " +
                "link_col LONGVARCHAR, " +
                "constraint pk_test_schema_test_table primary key (account_id, key))";
        assertEquals(expectedDdlQuery, ddlQueryGenerator.generateCreateTableQuery(entity));
    }

    private List<EntityField> createFields() {
        return Arrays.asList(EntityField.builder()
                        .name("account_id")
                        .nullable(false)
                        .ordinalPosition(1)
                        .primaryOrder(1)
                        .shardingOrder(1)
                        .type(ColumnType.BIGINT)
                        .build(),
                EntityField.builder()
                        .name("account_type")
                        .nullable(false)
                        .ordinalPosition(2)
                        .size(2)
                        .type(ColumnType.VARCHAR)
                        .build(),
                EntityField.builder()
                        .name("key")
                        .nullable(true)
                        .ordinalPosition(3)
                        .primaryOrder(2)
                        .type(ColumnType.INT)
                        .build(),
                EntityField.builder()
                        .name("double_col")
                        .nullable(true)
                        .ordinalPosition(4)
                        .type(ColumnType.DOUBLE)
                        .build(),
                EntityField.builder()
                        .name("boolean_col")
                        .nullable(true)
                        .ordinalPosition(5)
                        .type(ColumnType.BOOLEAN)
                        .build(),
                EntityField.builder()
                        .name("int_col")
                        .nullable(true)
                        .ordinalPosition(6)
                        .type(ColumnType.INT)
                        .build(),
                EntityField.builder()
                        .name("date_col")
                        .nullable(true)
                        .ordinalPosition(7)
                        .type(ColumnType.DATE)
                        .build(),
                EntityField.builder()
                        .name("timestamp_col")
                        .nullable(true)
                        .ordinalPosition(8)
                        .accuracy(6)
                        .type(ColumnType.TIMESTAMP)
                        .build(),
                EntityField.builder()
                        .name("timestamp_col_3")
                        .nullable(true)
                        .ordinalPosition(9)
                        .accuracy(3)
                        .type(ColumnType.TIMESTAMP)
                        .build(),
                EntityField.builder()
                        .name("time_col")
                        .nullable(true)
                        .ordinalPosition(10)
                        .accuracy(6)
                        .type(ColumnType.TIME)
                        .build(),
                EntityField.builder()
                        .name("time_col_5")
                        .nullable(true)
                        .ordinalPosition(11)
                        .accuracy(5)
                        .type(ColumnType.TIME)
                        .build(),
                EntityField.builder()
                        .name("uuid_col")
                        .nullable(true)
                        .ordinalPosition(12)
                        .type(ColumnType.UUID)
                        .build(),
                EntityField.builder()
                        .name("char_col")
                        .nullable(true)
                        .ordinalPosition(13)
                        .size(1)
                        .type(ColumnType.CHAR)
                        .build(),
                EntityField.builder()
                        .name("link_col")
                        .nullable(true)
                        .ordinalPosition(14)
                        .type(ColumnType.LINK)
                        .build()
        );
    }


}