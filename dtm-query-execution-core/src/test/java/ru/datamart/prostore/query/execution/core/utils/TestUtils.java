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
package ru.datamart.prostore.query.execution.core.utils;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.commons.io.IOUtils;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.dialect.LimitSqlDialect;
import ru.datamart.prostore.query.calcite.core.provider.CalciteContextProvider;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.core.base.configuration.AppConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDefinitionService;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestUtils {
    public static final CalciteConfiguration CALCITE_CONFIGURATION = new CalciteConfiguration();
    public static final CalciteCoreConfiguration CALCITE_CORE_CONFIGURATION = new CalciteCoreConfiguration();
    public static final DefinitionService<SqlNode> DEFINITION_SERVICE =
            new CoreCalciteDefinitionService(CALCITE_CONFIGURATION.configEddlParser(CALCITE_CORE_CONFIGURATION.eddlParserImplFactory()));
    public static final SqlDialect SQL_DIALECT = new LimitSqlDialect(SqlDialect.EMPTY_CONTEXT);

    private static final LimitSqlDialect SQL_DIALECT_DEFAULT_CONTEXT = new LimitSqlDialect(CalciteSqlDialect.DEFAULT_CONTEXT);

    private TestUtils() {
    }

    public static AppConfiguration getCoreConfiguration(String envName) {
        return getCoreAppConfiguration(envName);
    }

    public static AppConfiguration getCoreAppConfiguration(String envName) {
        return new AppConfiguration(null) {
            @Override
            public String getEnvName() {
                return envName;
            }
        };
    }

    public static void assertException(Class<? extends Throwable> expected, String partOfMessage, Throwable actual) {
        assertNotNull(actual);
        assertSame(expected, actual.getClass());
        assertTrue(actual.getMessage().contains(partOfMessage), String.format("Message: %s\nNot contains expected part of message: %s", actual.getMessage(), partOfMessage));
    }

    public static void initEntityList(List<Entity> entityList, String schema) {
        List<EntityField> fields = Collections.singletonList(
                EntityField.builder()
                        .ordinalPosition(0)
                        .name("id")
                        .type(ColumnType.BIGINT)
                        .nullable(false)
                        .build());
        Entity entity1 = Entity.builder()
                .schema(schema)
                .name("test_view")
                .viewQuery(String.format("SELECT * FROM %s.%s", schema, "test_table"))
                .fields(fields)
                .entityType(EntityType.VIEW)
                .build();
        Entity entity2 = Entity.builder()
                .schema(schema)
                .name("test_table")
                .fields(fields)
                .entityType(EntityType.TABLE)
                .build();
        Entity entity3 = Entity.builder()
                .schema(schema)
                .name("accounts")
                .fields(fields)
                .entityType(EntityType.TABLE)
                .build();
        Entity entity4 = Entity.builder()
                .schema(schema)
                .name("transactions")
                .fields(fields)
                .entityType(EntityType.TABLE)
                .build();

        List<EntityField> accountFields = new ArrayList<>();
        accountFields.add(EntityField.builder()
                .ordinalPosition(0)
                .name("id")
                .type(ColumnType.BIGINT)
                .nullable(false)
                .build());
        accountFields.add(EntityField.builder()
                .ordinalPosition(0)
                .name("account_type")
                .type(ColumnType.BIGINT)
                .nullable(false)
                .build());
        Entity entity5 = Entity.builder()
                .schema(schema)
                .name("accounts1")
                .fields(accountFields)
                .entityType(EntityType.TABLE)
                .build();
        Entity entity6 = Entity.builder()
                .schema(schema)
                .name("accounts2")
                .fields(accountFields)
                .entityType(EntityType.TABLE)
                .build();

        Entity entity7 = Entity.builder()
                .schema(schema)
                .name("entity")
                .entityType(EntityType.TABLE)
                .fields(Arrays.asList(EntityField.builder()
                                .ordinalPosition(0)
                                .name("id")
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .ordinalPosition(1)
                                .name("timestamp_col")
                                .type(ColumnType.TIMESTAMP)
                                .accuracy(6)
                                .nullable(true)
                                .build()))
                .build();

        entityList.add(entity1);
        entityList.add(entity2);
        entityList.add(entity3);
        entityList.add(entity4);
        entityList.add(entity5);
        entityList.add(entity6);
        entityList.add(entity7);
    }


    @SneakyThrows
    public static QueryParserResponse parse(CalciteContextProvider contextProvider, QueryParserRequest request) {
        val context = contextProvider.context(request.getSchema());
        val sql = request.getQuery().toSqlString(SQL_DIALECT_DEFAULT_CONTEXT).getSql();
        val parse = context.getPlanner().parse(sql);
        val validatedQuery = context.getPlanner().validate(parse);
        val relQuery = context.getPlanner().rel(validatedQuery);
        return new QueryParserResponse(
                context,
                request.getSchema(),
                relQuery,
                validatedQuery);
    }

    @SneakyThrows
    public static String loadTextFromFile(String path) {
        try (InputStream inputStream = TestUtils.class.getClassLoader().getResourceAsStream(path)) {
            assert inputStream != null;
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }

    @SneakyThrows
    public static <T> T getFieldValue(Object obj, String fieldName) {
        Field field = obj.getClass().getDeclaredField(fieldName);
        boolean origAccess = field.isAccessible();
        field.setAccessible(true);
        T fieldValue = (T) field.get(obj);
        field.setAccessible(origAccess);
        return fieldValue;
    }
}
