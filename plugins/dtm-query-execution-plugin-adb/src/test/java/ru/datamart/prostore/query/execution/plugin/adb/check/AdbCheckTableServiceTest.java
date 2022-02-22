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
package ru.datamart.prostore.query.execution.plugin.adb.check;

import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.adb.base.dto.metadata.AdbTables;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.metadata.AdbMetaTableEntityFactory;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.metadata.AdbTableEntitiesFactory;
import ru.datamart.prostore.query.execution.plugin.adb.base.utils.AdbTypeUtil;
import ru.datamart.prostore.query.execution.plugin.adb.check.service.AdbCheckTableService;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.impl.AdbQueryExecutor;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckTableRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckTableService;
import utils.CreateEntityUtils;

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
import static ru.datamart.prostore.query.execution.plugin.adb.base.factory.Constants.*;

public class AdbCheckTableServiceTest {

    private static Map<String, List<Map<String, Object>>> sysColumns;
    private CheckTableService adbCheckTableService;
    private final List<String> tablePostFixes = Arrays.asList(
            AdbTables.ACTUAL_TABLE_POSTFIX,
            AdbTables.HISTORY_TABLE_POSTFIX,
            AdbTables.STAGING_TABLE_POSTFIX);
    private Entity entity;
    private CheckTableRequest checkTableRequest;
    private static final String TEST_COLUMN_NAME = "test_column";

    @BeforeAll
    static void init() {
        Map<String, Object> sysFromAttr = new HashMap<>();
        sysFromAttr.put(AdbMetaTableEntityFactory.CONSTRAINT_TYPE, null);
        sysFromAttr.put(AdbMetaTableEntityFactory.DATETIME_PRECISION, null);
        sysFromAttr.put(AdbMetaTableEntityFactory.ORDINAL_POSITION, null);
        sysFromAttr.put(AdbMetaTableEntityFactory.COLUMN_NAME, SYS_FROM_ATTR);
        sysFromAttr.put(AdbMetaTableEntityFactory.DATA_TYPE, "int8");
        sysFromAttr.put(AdbMetaTableEntityFactory.CHARACTER_MAXIMUM_LENGTH, null);

        Map<String, Object> sysToAttr = new HashMap<>();
        sysToAttr.put(AdbMetaTableEntityFactory.CONSTRAINT_TYPE, null);
        sysToAttr.put(AdbMetaTableEntityFactory.DATETIME_PRECISION, null);
        sysToAttr.put(AdbMetaTableEntityFactory.ORDINAL_POSITION, null);
        sysToAttr.put(AdbMetaTableEntityFactory.COLUMN_NAME, SYS_TO_ATTR);
        sysToAttr.put(AdbMetaTableEntityFactory.DATA_TYPE, "int8");
        sysToAttr.put(AdbMetaTableEntityFactory.CHARACTER_MAXIMUM_LENGTH, null);

        Map<String, Object> sysOpAttr = new HashMap<>();
        sysOpAttr.put(AdbMetaTableEntityFactory.CONSTRAINT_TYPE, null);
        sysOpAttr.put(AdbMetaTableEntityFactory.DATETIME_PRECISION, null);
        sysOpAttr.put(AdbMetaTableEntityFactory.ORDINAL_POSITION, null);
        sysOpAttr.put(AdbMetaTableEntityFactory.COLUMN_NAME, SYS_OP_ATTR);
        sysOpAttr.put(AdbMetaTableEntityFactory.DATA_TYPE, "int4");
        sysOpAttr.put(AdbMetaTableEntityFactory.CHARACTER_MAXIMUM_LENGTH, null);

        Map<String, Object> PKSysFromAttr = new HashMap<>();
        PKSysFromAttr.put(AdbMetaTableEntityFactory.CONSTRAINT_TYPE, null);
        PKSysFromAttr.put(AdbMetaTableEntityFactory.DATETIME_PRECISION, null);
        PKSysFromAttr.put(AdbMetaTableEntityFactory.ORDINAL_POSITION, 3);
        PKSysFromAttr.put(AdbMetaTableEntityFactory.COLUMN_NAME, SYS_FROM_ATTR);
        PKSysFromAttr.put(AdbMetaTableEntityFactory.DATA_TYPE, "int8");
        PKSysFromAttr.put(AdbMetaTableEntityFactory.CHARACTER_MAXIMUM_LENGTH, null);

        sysColumns = new HashMap<>();
        sysColumns.put(AdbTables.ACTUAL_TABLE_POSTFIX, Arrays.asList(PKSysFromAttr, sysToAttr, sysOpAttr));
        sysColumns.put(AdbTables.HISTORY_TABLE_POSTFIX, Arrays.asList(PKSysFromAttr, sysToAttr, sysOpAttr));
        sysColumns.put(AdbTables.STAGING_TABLE_POSTFIX, Arrays.asList(sysFromAttr, sysToAttr, sysOpAttr));

    }

    @BeforeEach
    void setUp() {
        DatabaseExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
        entity = CreateEntityUtils.getEntity();

        int fieldsCount = entity.getFields().size();
        entity.getFields().add(EntityField.builder()
                .name(TEST_COLUMN_NAME)
                .ordinalPosition(fieldsCount + 1)
                .type(ColumnType.BIGINT)
                .nullable(true)
                .build());

        checkTableRequest = new CheckTableRequest(UUID.randomUUID(), "env", entity.getSchema(), entity);

        tablePostFixes.forEach(postFix -> when(adbQueryExecutor.execute(argThat(getPredicate(postFix)::test)))
                .thenReturn(Future.succeededFuture(getResultSet(postFix))));

        List<String> queries = tablePostFixes.stream()
                .map(postFix -> String.format(AdbMetaTableEntityFactory.QUERY_PATTERN,
                        entity.getSchema(), String.format("%s_%s", entity.getName(), postFix)))
                .collect(Collectors.toList());
        when(adbQueryExecutor.execute(argThat(arg -> queries.stream().noneMatch(arg::equals))))
                .thenReturn(Future.succeededFuture(Collections.emptyList()));
        adbCheckTableService = new AdbCheckTableService(new AdbTableEntitiesFactory(),
                new AdbMetaTableEntityFactory(adbQueryExecutor));
    }

    @Test
    void testSuccess() {
        assertTrue(adbCheckTableService.check(checkTableRequest).succeeded());
    }

    @Test
    void testTableNotExist() {
        entity.setName("not_exist_table");
        Future<Void> result = adbCheckTableService.check(checkTableRequest);
        tablePostFixes.forEach(postFix ->
                assertThat(result.cause().getMessage(),
                        containsString(String.format(AdbCheckTableService.TABLE_NOT_EXIST_ERROR_TEMPLATE,
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
        String expectedError = String.format(AdbCheckTableService.COLUMN_NOT_EXIST_ERROR_TEMPLATE,
                "not_exist_column");
        assertThat(adbCheckTableService.check(checkTableRequest).cause().getMessage(),
                containsString(expectedError));
    }

    @Test
    void testDataType() {
        String expectedError = String.format(AdbCheckTableService.FIELD_ERROR_TEMPLATE,
                AdbMetaTableEntityFactory.DATA_TYPE, "time(6)", "int8");
        testColumns(field -> field.setType(ColumnType.TIME), expectedError);

    }

    @Test
    void testPrimaryKey() {
        String expectedError = String.format(AdbCheckTableService.PRIMARY_KEY_ERROR_TEMPLATE,
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
        assertThat(adbCheckTableService.check(checkTableRequest).cause().getMessage(),
                containsString(expectedError));
    }

    private Predicate<String> getPredicate(String postFix) {
        String query = String.format(AdbMetaTableEntityFactory.QUERY_PATTERN,
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
            result.put(AdbMetaTableEntityFactory.CONSTRAINT_TYPE, null);
            result.put(AdbMetaTableEntityFactory.ORDINAL_POSITION, null);

        } else {
            result.put(AdbMetaTableEntityFactory.CONSTRAINT_TYPE, Optional.ofNullable(field.getPrimaryOrder())
                    .map(o -> AdbMetaTableEntityFactory.PRIMARY_KEY_CS_TYPE).orElse(null));
            result.put(AdbMetaTableEntityFactory.ORDINAL_POSITION, field.getPrimaryOrder());
        }
        result.put(AdbMetaTableEntityFactory.DATETIME_PRECISION, field.getAccuracy());
        result.put(AdbMetaTableEntityFactory.COLUMN_NAME, field.getName());
        result.put(AdbMetaTableEntityFactory.DATA_TYPE, AdbTypeUtil.adbTypeFromDtmType(field));
        result.put(AdbMetaTableEntityFactory.CHARACTER_MAXIMUM_LENGTH, field.getSize());
        return result;
    }
}
