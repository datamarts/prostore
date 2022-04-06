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
package ru.datamart.prostore.query.execution.plugin.adqm.utils;

import com.google.common.collect.Lists;
import lombok.val;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

import java.util.List;
import java.util.UUID;

public class StandaloneTestUtils {
    public static final String ENV = "env";
    public static final String TABLE_PATH = "dbName.table_path";
    public static final String CLUSTER = "cluster";
    public final static String TABLE = "table";

    public static Entity createTestEntity() {
        return Entity.builder()
                .name(TABLE)
                .externalTableLocationPath(TABLE_PATH)
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .fields(createTestEntityFields())
                .build();
    }

    private static List<EntityField> createTestEntityFields() {
        val idCol = EntityField.builder()
                .ordinalPosition(0)
                .name("ID")
                .type(ColumnType.INT)
                .nullable(false)
                .primaryOrder(1)
                .shardingOrder(1)
                .build();
        val descriptionCol = EntityField.builder()
                .ordinalPosition(1)
                .name("DESCRIPTION")
                .type(ColumnType.VARCHAR)
                .size(200)
                .nullable(false)
                .build();
        val foreignKeyCol = EntityField.builder()
                .ordinalPosition(2)
                .name("FOREIGN_KEY")
                .type(ColumnType.INT)
                .nullable(false)
                .build();
        val timeCol = EntityField.builder()
                .ordinalPosition(3)
                .name("TIME_COL")
                .type(ColumnType.TIME)
                .nullable(true)
                .accuracy(5)
                .build();
        return Lists.newArrayList(idCol, descriptionCol, foreignKeyCol, timeCol);
    }

    public static EddlRequest createEddlRequest(boolean isCreate) {
        return EddlRequest.builder()
                .createRequest(isCreate)
                .requestId(UUID.randomUUID())
                .envName(ENV)
                .entity(createTestEntity())
                .build();
    }
}
