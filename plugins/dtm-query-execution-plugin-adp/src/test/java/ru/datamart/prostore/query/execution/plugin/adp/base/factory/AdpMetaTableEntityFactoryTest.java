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
package ru.datamart.prostore.query.execution.plugin.adp.base.factory;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTableEntity;
import ru.datamart.prostore.query.execution.plugin.adp.base.factory.metadata.AdpMetaTableEntityFactory;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.*;

import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.adpTableColumnsFromEntityFields;
import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.createAllTypesTable;
import static ru.datamart.prostore.query.execution.plugin.api.factory.MetaTableEntityFactory.COLUMN_NAME;
import static ru.datamart.prostore.query.execution.plugin.api.factory.MetaTableEntityFactory.DATA_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AdpMetaTableEntityFactoryTest {
    private static final String ENV = "env";
    private static final String SCHEMA = "dtm";
    private static final String TABLE = "table";
    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final AdpMetaTableEntityFactory metaTableEntityFactory = new AdpMetaTableEntityFactory(databaseExecutor);

    @Test
    void createEmptyResultSuccess() {
        when(databaseExecutor.execute(anyString())).thenReturn(Future.succeededFuture(new ArrayList<>()));

        metaTableEntityFactory.create(ENV, SCHEMA, TABLE)
                .onComplete(asyncResult -> {
                    assertTrue(asyncResult.succeeded());
                    assertFalse(asyncResult.result().isPresent());
                });
    }

    @Test
    void createResultSuccess() {
        val allTypesTable = createAllTypesTable();
        val expectedAdpTable = new AdpTableEntity(null,
                null,
                adpTableColumnsFromEntityFields(allTypesTable.getFields()),
                Collections.singletonList("id"));

        when(databaseExecutor.execute(anyString())).thenReturn(Future.succeededFuture(getAllTypesResult(allTypesTable.getFields())));

        metaTableEntityFactory.create(ENV, SCHEMA, TABLE)
                .onComplete(asyncResult -> {
                    assertTrue(asyncResult.succeeded());
                    assertTrue(asyncResult.result().isPresent());
                    assertThat(asyncResult.result().get())
                            .usingRecursiveComparison()
                            .ignoringFields("columns.nullable")
                            .isEqualTo(expectedAdpTable);
                });
    }

    @Test
    void createFail() {
        when(databaseExecutor.execute(anyString())).thenReturn(Future.failedFuture("database error"));

        metaTableEntityFactory.create(ENV, SCHEMA, TABLE)
                .onComplete(asyncResult -> {
                    assertTrue(asyncResult.failed());
                    assertEquals("database error", asyncResult.cause().getMessage());
                });
    }

    private List<Map<String, Object>> getAllTypesResult(List<EntityField> fields) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (EntityField field: fields) {
            Map<String, Object> row = new HashMap<>();
            row.put(COLUMN_NAME, field.getName());
            row.put(DATA_TYPE, getAdpType(field.getType()));
            if (field.getPrimaryOrder() != null) {
                row.put(AdpMetaTableEntityFactory.ORDINAL_POSITION, field.getPrimaryOrder());
                row.put(AdpMetaTableEntityFactory.CONSTRAINT_TYPE, AdpMetaTableEntityFactory.PRIMARY_KEY_CS_TYPE);
            }
            if (field.getSize() != null) {
                row.put(AdpMetaTableEntityFactory.CHARACTER_MAXIMUM_LENGTH, field.getSize());
                row.put(AdpMetaTableEntityFactory.DATETIME_PRECISION, field.getSize());
            }
            result.add(row);
        }
        return result;
    }

    private String getAdpType(ColumnType type) {
        switch (type) {
            case LINK:
            case CHAR:
            case UUID:
                return "varchar";
            case FLOAT:
                return "float4";
            case BOOLEAN:
                return "bool";
            case DOUBLE:
                return "float8";
            case INT32:
                return "int4";
            case INT:
            case BIGINT:
                return "int8";
            default:
                return type.name().toLowerCase();
        }
    }
}
