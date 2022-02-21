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
package ru.datamart.prostore.query.execution.plugin.adp.check;

import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.adp.base.factory.metadata.AdpMetaTableEntityFactory;
import ru.datamart.prostore.query.execution.plugin.adp.base.factory.metadata.AdpTableEntitiesFactory;
import ru.datamart.prostore.query.execution.plugin.adp.base.utils.AdpTypeUtil;
import ru.datamart.prostore.query.execution.plugin.adp.check.service.AdpCheckTableService;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.AdpQueryExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckTableRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckTableService;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.datamart.prostore.query.execution.plugin.adp.base.Constants.*;

class AdpCheckTableServiceTest {

    private static final String ACTUAL_TABLE_POSTFIX = "actual";
    private static final String HISTORY_TABLE_POSTFIX = "history";
    private static final String STAGING_TABLE_POSTFIX = "staging";

    private static Map<String, List<Map<String, Object>>> sysColumns;
    private CheckTableService adpCheckTableService;
    private final List<String> tablePostFixes = Arrays.asList(
            ACTUAL_TABLE_POSTFIX,
            HISTORY_TABLE_POSTFIX,
            STAGING_TABLE_POSTFIX);
    private Entity entity;
    private CheckTableRequest checkTableRequest;
    private static final String TEST_COLUMN_NAME = "test_column";

    @BeforeAll
    static void init() {
        Map<String, Object> sysFromAttr = new HashMap<>();
        sysFromAttr.put(AdpMetaTableEntityFactory.CONSTRAINT_TYPE, null);
        sysFromAttr.put(AdpMetaTableEntityFactory.DATETIME_PRECISION, null);
        sysFromAttr.put(AdpMetaTableEntityFactory.ORDINAL_POSITION, null);
        sysFromAttr.put(AdpMetaTableEntityFactory.COLUMN_NAME, SYS_FROM_ATTR);
        sysFromAttr.put(AdpMetaTableEntityFactory.DATA_TYPE, "int8");
        sysFromAttr.put(AdpMetaTableEntityFactory.CHARACTER_MAXIMUM_LENGTH, null);

        Map<String, Object> sysToAttr = new HashMap<>();
        sysToAttr.put(AdpMetaTableEntityFactory.CONSTRAINT_TYPE, null);
        sysToAttr.put(AdpMetaTableEntityFactory.DATETIME_PRECISION, null);
        sysToAttr.put(AdpMetaTableEntityFactory.ORDINAL_POSITION, null);
        sysToAttr.put(AdpMetaTableEntityFactory.COLUMN_NAME, SYS_TO_ATTR);
        sysToAttr.put(AdpMetaTableEntityFactory.DATA_TYPE, "int8");
        sysToAttr.put(AdpMetaTableEntityFactory.CHARACTER_MAXIMUM_LENGTH, null);

        Map<String, Object> sysOpAttr = new HashMap<>();
        sysOpAttr.put(AdpMetaTableEntityFactory.CONSTRAINT_TYPE, null);
        sysOpAttr.put(AdpMetaTableEntityFactory.DATETIME_PRECISION, null);
        sysOpAttr.put(AdpMetaTableEntityFactory.ORDINAL_POSITION, null);
        sysOpAttr.put(AdpMetaTableEntityFactory.COLUMN_NAME, SYS_OP_ATTR);
        sysOpAttr.put(AdpMetaTableEntityFactory.DATA_TYPE, "int4");
        sysOpAttr.put(AdpMetaTableEntityFactory.CHARACTER_MAXIMUM_LENGTH, null);

        Map<String, Object> PKSysFromAttr = new HashMap<>();
        PKSysFromAttr.put(AdpMetaTableEntityFactory.CONSTRAINT_TYPE, null);
        PKSysFromAttr.put(AdpMetaTableEntityFactory.DATETIME_PRECISION, null);
        PKSysFromAttr.put(AdpMetaTableEntityFactory.ORDINAL_POSITION, 3);
        PKSysFromAttr.put(AdpMetaTableEntityFactory.COLUMN_NAME, SYS_FROM_ATTR);
        PKSysFromAttr.put(AdpMetaTableEntityFactory.DATA_TYPE, "int8");
        PKSysFromAttr.put(AdpMetaTableEntityFactory.CHARACTER_MAXIMUM_LENGTH, null);

        sysColumns = new HashMap<>();
        sysColumns.put(ACTUAL_TABLE_POSTFIX, Arrays.asList(PKSysFromAttr, sysToAttr, sysOpAttr));
        sysColumns.put(HISTORY_TABLE_POSTFIX, Arrays.asList(PKSysFromAttr, sysToAttr, sysOpAttr));
        sysColumns.put(STAGING_TABLE_POSTFIX, Arrays.asList(sysFromAttr, sysToAttr, sysOpAttr));

    }

    @BeforeEach
    void setUp() {
        DatabaseExecutor adpQueryExecutor = mock(AdpQueryExecutor.class);
        entity = getEntity();

        int fieldsCount = entity.getFields().size();
        entity.getFields().add(EntityField.builder()
                .name(TEST_COLUMN_NAME)
                .ordinalPosition(fieldsCount + 1)
                .type(ColumnType.BIGINT)
                .nullable(true)
                .build());

        checkTableRequest = new CheckTableRequest(UUID.randomUUID(), "env", entity.getSchema(), entity);

        tablePostFixes.forEach(postFix -> when(adpQueryExecutor.execute(argThat(getPredicate(postFix)::test)))
                .thenReturn(Future.succeededFuture(getResultSet(postFix))));

        List<String> queries = tablePostFixes.stream()
                .map(postFix -> String.format(AdpMetaTableEntityFactory.QUERY_PATTERN,
                        entity.getSchema(), String.format("%s_%s", entity.getName(), postFix)))
                .collect(Collectors.toList());
        when(adpQueryExecutor.execute(argThat(arg -> queries.stream().noneMatch(arg::equals))))
                .thenReturn(Future.succeededFuture(Collections.emptyList()));
        adpCheckTableService = new AdpCheckTableService(new AdpTableEntitiesFactory(),
                new AdpMetaTableEntityFactory(adpQueryExecutor));
    }

    @Test
    void testSuccess() {
        assertTrue(adpCheckTableService.check(checkTableRequest).succeeded());
    }

    @Test
    void testTableNotExist() {
        entity.setName("not_exist_table");
        Future<Void> result = adpCheckTableService.check(checkTableRequest);
        tablePostFixes.forEach(postFix ->
                assertThat(result.cause().getMessage(),
                        containsString(String.format(AdpCheckTableService.TABLE_NOT_EXIST_ERROR_TEMPLATE,
                                String.format("not_exist_table_%s", postFix)))));
    }

    @Test
    void testColumnNotExist() {
        entity.getFields().add(EntityField.builder()
                .name("not_exist_column")
                .size(1)
                .type(ColumnType.VARCHAR)
                .nullable(true)
                .build());
        String expectedError = String.format(AdpCheckTableService.COLUMN_NOT_EXIST_ERROR_TEMPLATE,
                "not_exist_column");
        assertThat(adpCheckTableService.check(checkTableRequest).cause().getMessage(),
                containsString(expectedError));
    }

    @Test
    void testDataType() {
        String expectedError = String.format(AdpCheckTableService.FIELD_ERROR_TEMPLATE,
                AdpMetaTableEntityFactory.DATA_TYPE, "time(6)", "int8");
        testColumns(field -> field.setType(ColumnType.TIME), expectedError);

    }

    @Test
    void testPrimaryKey() {
        String expectedError = String.format(AdpCheckTableService.PRIMARY_KEY_ERROR_TEMPLATE,
                "id, pk2, test_column, sys_from", "id, pk2, sys_from");
        testColumns(field -> field.setPrimaryOrder(4), expectedError);

    }

    private void testColumns(Consumer<EntityField> consumer,
                             String expectedError) {
        EntityField testColumn = entity.getFields().stream()
                .filter(field -> TEST_COLUMN_NAME.equals(field.getName()))
                .findAny()
                .orElseThrow(RuntimeException::new);
        consumer.accept(testColumn);
        assertThat(adpCheckTableService.check(checkTableRequest).cause().getMessage(),
                containsString(expectedError));
    }

    private Predicate<String> getPredicate(String postFix) {
        String query = String.format(AdpMetaTableEntityFactory.QUERY_PATTERN,
                entity.getSchema(), String.format("%s_%s", entity.getName(), postFix));
        return query::equals;
    }

    private List<Map<String, Object>> getResultSet(String postFix) {
        List<Map<String, Object>> resultSet = entity.getFields().stream()
                .map(field -> fieldToMapTransform(field, postFix))
                .collect(Collectors.toList());
        resultSet.addAll(sysColumns.get(postFix));
        return resultSet;
    }

    private Map<String, Object> fieldToMapTransform(EntityField field, String postFix) {
        HashMap<String, Object> result = new HashMap<>();
        if ("staging".equals(postFix)) {
            result.put(AdpMetaTableEntityFactory.CONSTRAINT_TYPE, null);
            result.put(AdpMetaTableEntityFactory.ORDINAL_POSITION, null);

        } else {
            result.put(AdpMetaTableEntityFactory.CONSTRAINT_TYPE, Optional.ofNullable(field.getPrimaryOrder())
                    .map(o -> AdpMetaTableEntityFactory.PRIMARY_KEY_CS_TYPE).orElse(null));
            result.put(AdpMetaTableEntityFactory.ORDINAL_POSITION, field.getPrimaryOrder());
        }
        result.put(AdpMetaTableEntityFactory.DATETIME_PRECISION, field.getAccuracy());
        result.put(AdpMetaTableEntityFactory.COLUMN_NAME, field.getName());
        result.put(AdpMetaTableEntityFactory.DATA_TYPE, AdpTypeUtil.adpTypeFromDtmType(field));
        result.put(AdpMetaTableEntityFactory.CHARACTER_MAXIMUM_LENGTH, field.getSize());
        return result;
    }

    private Entity getEntity() {
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
        return new Entity("test_schema.test_table", fields);
    }
}
