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
package utils;

import lombok.val;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class CreateEntityUtils {
    public static final String SCHEMA = "test_schema";
    public static final String TABLE = "test_table";
    public static final String LOCATION_PATH = "table_path";
    public static final String ENV = "env";

    public static Entity getEntity() {
        List<EntityField> keyFields = Arrays.asList(
                new EntityField(0, "id", ColumnType.INT.name(), false, 1, 1, null),
                new EntityField(1, "sk_key2", ColumnType.INT.name(), false, null, 2, null),
                new EntityField(2, "pk2", ColumnType.INT.name(), false, 2, null, null),
                new EntityField(3, "sk_key3", ColumnType.INT.name(), false, null, 3, null)
        );
        ColumnType[] types = ColumnType.values();
        List<EntityField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            ColumnType type = types[i];
            if (Arrays.asList(ColumnType.BLOB, ColumnType.ANY).contains(type)) {
                continue;
            }

            EntityField.EntityFieldBuilder builder = EntityField.builder()
                    .ordinalPosition(i + keyFields.size())
                    .type(type)
                    .nullable(true)
                    .name(type.name() + "_type");
            if (Arrays.asList(ColumnType.CHAR, ColumnType.VARCHAR).contains(type)) {
                builder.size(20);
            } else if (Arrays.asList(ColumnType.TIME, ColumnType.TIMESTAMP).contains(type)) {
                builder.accuracy(5);
            }
            fields.add(builder.build());
        }
        fields.addAll(keyFields);
        val nameWithSchema = SCHEMA + "." + TABLE;
        val entity = new Entity(nameWithSchema, fields);
        entity.setExternalTableLocationPath(LOCATION_PATH);
        return entity;
    }

    public static EddlRequest createEddlRequest(boolean isCreate) {
        return EddlRequest.builder()
                .createRequest(isCreate)
                .requestId(UUID.randomUUID())
                .envName(ENV)
                .entity(getEntity())
                .datamartMnemonic(SCHEMA)
                .build();
    }
}
